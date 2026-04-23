from __future__ import annotations

import ast
from datetime import UTC, datetime
import importlib
from pathlib import Path

from common.suricata_monthly_triage import (
    build_signature_summary_for_llm,
    build_suppression_candidates,
    extract_ipv4_identity,
    extract_ipv6_identity,
    group_alerts_by_signature,
    normalize_suricata_loki_response,
    previous_calendar_month_window,
    render_plaintext_report,
    run_signature_agent,
)
from dags.cyber_analyst import (
    _collection_limit,
    _rate_limit_warning,
)


def test_analyze_signatures_imports_loki_client_inside_virtualenv_task() -> None:
    source = Path("/opt/docker/airflow/dags/cyber_analyst.py").read_text()
    module = ast.parse(source)

    analyze_signatures = next(
        node
        for node in ast.walk(module)
        if isinstance(node, ast.FunctionDef) and node.name == "analyze_signatures"
    )
    query_suricata_context = next(
        node
        for node in ast.walk(analyze_signatures)
        if isinstance(node, ast.FunctionDef) and node.name == "query_suricata_context"
    )

    assert any(
        isinstance(stmt, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "LOKI_CONN_ID"
            for target in stmt.targets
        )
        for stmt in analyze_signatures.body
    )
    assert any(
        isinstance(stmt, ast.FunctionDef) and stmt.name == "rate_limit_warning"
        for stmt in analyze_signatures.body
    )
    assert any(
        isinstance(stmt, ast.ImportFrom)
        and stmt.module == "common.loki"
        and any(alias.name == "query_loki_range" for alias in stmt.names)
        for stmt in analyze_signatures.body
    )
    assert "alert_signature_id" in source
    assert "alert.signature_id" not in source


def test_previous_calendar_month_window() -> None:
    window_start, window_end = previous_calendar_month_window(
        datetime(2026, 4, 21, 10, 30, tzinfo=UTC)
    )

    assert window_start == datetime(2026, 3, 1, 0, 0, tzinfo=UTC)
    assert window_end == datetime(2026, 4, 1, 0, 0, tzinfo=UTC)


def test_normalize_suricata_row_and_grouping() -> None:
    payload = {
        "data": {
            "result": [
                {
                    "stream": {"job": "suricata"},
                    "values": [
                        [
                            "1740787200000000000",
                            (
                                '{"timestamp":"2026-03-01T00:00:00+00:00","src_ip":"10.0.0.5",'
                                '"dest_ip":"1.1.1.1","src_port":12345,"dest_port":443,"proto":"TCP",'
                                '"alert":{"category":"Attempted Admin","signature":"ET TEST",'
                                '"signature_id":2100001}}'
                            ),
                        ]
                    ],
                }
            ]
        }
    }

    rows = normalize_suricata_loki_response(payload)
    groups = group_alerts_by_signature(rows)

    assert rows[0]["signature_id"] == 2100001
    assert groups == [
        {
            "signature": "ET TEST",
            "alert_count": 1,
            "categories": ["Attempted Admin"],
            "signature_ids": [2100001],
            "representative_categories": ["Attempted Admin"],
            "representative_signatures": ["ET TEST"],
            "example_alerts": rows,
            "alerts": rows,
            "first_seen": "2026-03-01T00:00:00+00:00",
            "last_seen": "2026-03-01T00:00:00+00:00",
        }
    ]


def test_build_signature_summary_for_llm_omits_alert_payloads() -> None:
    summary = build_signature_summary_for_llm(
        {
            "signature": "ET TEST",
            "alert_count": 2,
            "categories": ["Attempted Admin"],
            "signature_ids": [2100001],
            "representative_categories": ["Attempted Admin"],
            "first_seen": "2026-03-01T00:00:00+00:00",
            "last_seen": "2026-03-02T00:00:00+00:00",
            "alerts": [{"raw_alert": {"secret": "payload"}}],
            "example_alerts": [{"raw_alert": {"secret": "payload"}}],
        }
    )

    assert summary == {
        "signature": "ET TEST",
        "alert_count": 2,
        "categories": ["Attempted Admin"],
        "signature_ids": [2100001],
        "representative_categories": ["Attempted Admin"],
        "first_seen": "2026-03-01T00:00:00+00:00",
        "last_seen": "2026-03-02T00:00:00+00:00",
    }


def test_extract_ipv4_identity() -> None:
    identity = extract_ipv4_identity(
        "192.0.2.10",
        [
            {
                "ip_address": "192.0.2.10",
                "mac_address": "00:11:22:33:44:55",
                "hostname": "workstation-01",
            }
        ],
        vendor_lookup=lambda _mac: "Example Vendor",
    )

    assert identity["hostname"] == "workstation-01"
    assert identity["mac_vendor"] == "Example Vendor"


def test_extract_ipv6_identity() -> None:
    identity = extract_ipv6_identity(
        "2001:db8::25",
        [{"ipv6_address": "2001:db8::25", "mac_address": "aa:bb:cc:dd:ee:ff"}],
        [{"mac_address": "aa:bb:cc:dd:ee:ff", "hostname": "printer-01"}],
        vendor_lookup=lambda _mac: "Printer Vendor",
    )

    assert identity["mac_address"] == "aa:bb:cc:dd:ee:ff"
    assert identity["hostname"] == "printer-01"
    assert identity["mac_vendor"] == "Printer Vendor"


def test_signature_agent_ignore_path_has_no_per_alert_rows() -> None:
    signature_group = {
        "signature": "Noise Signature",
        "alert_count": 25,
        "signature_ids": [1001],
        "categories": ["Chatty Background"],
        "alerts": [{"signature": "Noise"}],
    }

    result = run_signature_agent(
        signature_group,
        decide_signature=lambda group: {
            "signature": group["signature"],
            "alert_count": group["alert_count"],
            "decision": "ignore_background_noise",
            "reasoning_summary": "Routine scanners.",
            "confidence": "high",
            "representative_categories": ["Chatty Background"],
            "suppression_candidates": [],
        },
        analyze_alert=lambda _alert, _result: {"unexpected": True},
    )

    assert result["per_alert_results"] == []
    assert result["signature_result"]["suppression_candidates"] == [
        {
            "signature_id": 1001,
            "rule_line": "suppress gen_id 1, sig_id 1001",
            "scope_reason": (
                "Signature Noise Signature was classified as background noise "
                "for the full reporting window."
            ),
        }
    ]


def test_signature_agent_drilldown_path_produces_per_alert_rows() -> None:
    signature_group = {
        "signature": "Needs review",
        "alert_count": 1,
        "signature_ids": [2002],
        "categories": ["Suspicious"],
        "alerts": [{"signature": "Needs review"}],
    }

    result = run_signature_agent(
        signature_group,
        decide_signature=lambda group: {
            "signature": group["signature"],
            "alert_count": group["alert_count"],
            "decision": "analyze_individually",
            "reasoning_summary": "Escalate for drilldown.",
            "confidence": "medium",
            "representative_categories": ["Suspicious"],
            "suppression_candidates": [],
        },
        analyze_alert=lambda _alert, _result: {
            "signature": "Needs review",
            "signature_id": 2002,
            "category": "Suspicious",
            "src_ip": "10.0.0.5",
            "dest_ip": "198.51.100.20",
            "src_port": 44444,
            "dest_port": 443,
            "protocol": "TCP",
            "observed_at": "2026-03-05T10:00:00+00:00",
            "asset_identity": "host-01",
            "verdict": "suspicious_monitor",
            "evidence": "Rare outbound connection.",
            "recommended_follow_up": "Check the host.",
            "lookup_warnings": ["No hostname found"],
        },
    )

    assert len(result["per_alert_results"]) == 1
    assert result["unresolved_lookups"] == ["No hostname found"]


def test_build_suppression_candidates() -> None:
    assert build_suppression_candidates(
        {
            "signature": "Noise Signature",
            "signature_ids": [1001, 1002],
        }
    ) == [
        {
            "signature_id": 1001,
            "rule_line": "suppress gen_id 1, sig_id 1001",
            "scope_reason": (
                "Signature Noise Signature was classified as background noise for the full reporting window."
            ),
        },
        {
            "signature_id": 1002,
            "rule_line": "suppress gen_id 1, sig_id 1002",
            "scope_reason": (
                "Signature Noise Signature was classified as background noise for the full reporting window."
            ),
        },
    ]


def test_render_report_ignore_only() -> None:
    report = render_plaintext_report(
        window_start=datetime(2026, 3, 1, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, tzinfo=UTC),
        analyses=[
            {
                "signature_result": {
                    "signature": "Noise Signature",
                    "alert_count": 10,
                    "decision": "ignore_background_noise",
                    "reasoning_summary": "Routine.",
                    "confidence": "high",
                    "suppression_candidates": [
                        {
                            "signature_id": 1001,
                            "rule_line": "suppress gen_id 1, sig_id 1001",
                            "scope_reason": "Routine.",
                        }
                    ],
                },
                "per_alert_results": [],
                "unresolved_lookups": [],
            }
        ],
    )

    assert "Ignored Signatures" in report
    assert "threshold.config: suppress gen_id 1, sig_id 1001" in report
    assert "Signatures Analyzed Individually\n- None" in report


def test_render_report_mixed_with_failed_lookups() -> None:
    report = render_plaintext_report(
        window_start=datetime(2026, 3, 1, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, tzinfo=UTC),
        analyses=[
            {
                "signature_result": {
                    "signature": "Investigate Signature",
                    "alert_count": 1,
                    "decision": "analyze_individually",
                    "reasoning_summary": "Rare behavior.",
                    "confidence": "medium",
                    "suppression_candidates": [],
                },
                "per_alert_results": [
                    {
                        "signature": "Possible exploit",
                        "signature_id": 9001,
                        "category": "Investigate",
                        "src_ip": "10.0.0.2",
                        "dest_ip": "203.0.113.10",
                        "src_port": 55555,
                        "dest_port": 443,
                        "protocol": "TCP",
                        "observed_at": "2026-03-08T11:00:00+00:00",
                        "asset_identity": "server-01",
                        "verdict": "proof_of_malware",
                        "evidence": "Matched malware family and TI.",
                        "recommended_follow_up": "Isolate immediately.",
                    }
                ],
                "unresolved_lookups": [
                    "No hostname found",
                    "GreyNoise lookup failed",
                    "Missing CVE context",
                ],
            }
        ],
    )

    assert "Proof of Malware" in report
    assert "Isolate immediately." in report
    assert "GreyNoise lookup failed" in report


def test_rate_limit_warning_mentions_temporary_unavailability() -> None:
    assert _rate_limit_warning("GreyNoise", "60") == (
        "GreyNoise lookup temporarily unavailable because the API rate limit "
        "was reached; retry after 60."
    )


def test_collection_limit_supports_test_mode() -> None:
    assert _collection_limit(True) == 1
    assert _collection_limit(False) == 5000


def test_dag_import_and_virtualenv_requirements() -> None:
    module = importlib.import_module("dags.cyber_analyst")

    assert module.dag.dag_id == "cyber_analyst"
    assert "test_mode" in module.dag.params
    assert module.VIRTUALENV_REQUIREMENTS == [
        "apache-airflow",
        "arize-phoenix-otel",
        "langgraph",
        "langchain-openai",
        "mac-vendor-lookup",
        "pydantic",
        "openinference-instrumentation-langchain",
        "requests",
    ]
