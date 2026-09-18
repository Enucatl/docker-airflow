from __future__ import annotations

from datetime import UTC, datetime
import json

from common.suricata_monthly_triage import (
    compact_signature_groups,
    extract_ipv4_identity,
    extract_ipv6_identity,
    group_alerts_by_signature,
    normalize_suricata_loki_response,
    previous_calendar_month_window,
    render_plaintext_report,
)
from cyber_analyst import (
    AlertTriageResult,
    AssetIdentitySummary,
    ExternalAlertContext,
    LocalAlertContext,
    SuricataContextSummary,
    _collection_limit,
    _rate_limit_warning,
    analyze_enriched_alert,
    enrich_alert_externally,
    enrich_alert_locally,
    summarize_suricata_context,
    triage_alert,
    triage_signature,
)
import cyber_analyst


def _alert(
    signature: str = "ET TEST",
    signature_id: int = 2100001,
    observed_at: str = "2026-03-01T00:00:00+00:00",
    src_ip: str = "10.0.0.5",
    dest_ip: str = "1.1.1.1",
    dest_port: int = 443,
) -> dict[str, object]:
    return {
        "category": "Attempted Admin",
        "signature": signature,
        "signature_id": signature_id,
        "src_ip": src_ip,
        "dest_ip": dest_ip,
        "src_port": 12345,
        "dest_port": dest_port,
        "protocol": "TCP",
        "observed_at": observed_at,
    }


def _loki_payload(alerts: list[dict[str, object]]) -> dict[str, object]:
    return {
        "data": {
            "result": [
                {
                    "stream": {"job": "suricata"},
                    "values": [
                        [str(index), json.dumps(alert)]
                        for index, alert in enumerate(alerts, start=1)
                    ],
                }
            ]
        }
    }


def _signature_group(
    signature: str, signature_id: int, count: int
) -> dict[str, object]:
    return {
        "signature": signature,
        "alert_count": count,
        "categories": ["Suspicious"],
        "signature_ids": [signature_id],
        "representative_categories": ["Suspicious"],
        "representative_signatures": [signature],
        "first_seen": "2026-03-01T00:00:00+00:00",
        "last_seen": "2026-03-01T00:01:00+00:00",
    }


def _local_context() -> LocalAlertContext:
    identity = AssetIdentitySummary(
        ip="10.0.0.5",
        hostname="host-01",
        mac_address="00:11:22:33:44:55",
        mac_vendor="Example",
        lookup_path="ipv4_dhcp",
        resolved=True,
    )
    return LocalAlertContext(
        src_identity=identity,
        dest_identity=identity.model_copy(update={"ip": "1.1.1.1"}),
        suricata_context=SuricataContextSummary(
            event_count=1,
            same_signature_count=1,
            same_source_count=1,
            same_destination_count=1,
            distinct_src_ips=1,
            distinct_dest_ips=1,
            distinct_dest_ports=[443],
            distinct_protocols=["TCP"],
            related_signatures=[],
            first_event_at="2026-03-01T00:00:00+00:00",
            last_event_at="2026-03-01T00:00:00+00:00",
            fan_out=False,
            rapid_repeat=False,
        ),
    )


class FakeJev:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.calls: list[dict[str, object]] = []

    def decide(self, *, state, questions):
        self.calls.append({"state": state, "questions": questions})
        return self.response


def test_previous_calendar_month_window() -> None:
    window_start, window_end = previous_calendar_month_window(
        datetime(2026, 4, 21, 10, 30, tzinfo=UTC)
    )
    assert window_start == datetime(2026, 3, 1, tzinfo=UTC)
    assert window_end == datetime(2026, 4, 1, tzinfo=UTC)


def test_normalize_suricata_row_and_grouping() -> None:
    rows = normalize_suricata_loki_response(
        _loki_payload([_alert() | {"timestamp": "2026-03-01T00:00:00+00:00"}])
    )
    groups = group_alerts_by_signature(rows)
    assert rows[0]["signature_id"] == 2100001
    assert groups[0]["alert_count"] == 1
    assert groups[0]["alerts"] == rows


def test_compact_signature_groups_omit_alert_payloads() -> None:
    rows = normalize_suricata_loki_response(
        _loki_payload([_alert() | {"timestamp": "2026-03-01T00:00:00+00:00"}])
    )
    compact = compact_signature_groups(group_alerts_by_signature(rows))
    assert compact[0]["signature"] == "ET TEST"
    assert "alerts" not in compact[0]
    assert "raw_alert" not in str(compact)


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


def test_signature_triage_is_conservative() -> None:
    group = _signature_group("Noise", 1001, 25)
    assert (
        triage_signature(
            group,
            triage_client=FakeJev(
                {
                    "security_relevant": 0.05,
                    "background_noise": 0.95,
                    "needs_inspection": 0.05,
                }
            ),
        ).decision
        == "ignore_background_noise"
    )
    assert (
        triage_signature(
            group,
            triage_client=FakeJev(
                {
                    "security_relevant": 0.85,
                    "background_noise": 0.10,
                    "needs_inspection": 0.90,
                }
            ),
        ).decision
        == "investigate"
    )
    assert (
        triage_signature(
            group,
            triage_client=FakeJev(
                {
                    "security_relevant": {"noul": 0.10, "confidence": 0.20},
                    "background_noise": {"noul": 0.95, "confidence": 0.90},
                    "needs_inspection": {"noul": 0.05, "confidence": 0.90},
                }
            ),
        ).decision
        == "investigate"
    )
    assert (
        triage_signature(
            group,
            triage_client=FakeJev(
                {
                    "security_relevant": 0.05,
                    "background_noise": 0.95,
                    "needs_inspection": 0.95,
                }
            ),
        ).decision
        == "investigate"
    )


def test_alert_triage_uses_local_context_and_low_confidence_investigates() -> None:
    context = _local_context()
    fake = FakeJev(
        {
            "unexpected_source": 0.05,
            "suricata_relevant": 0.05,
            "could_conceal": 0.05,
            "enough_for_investigation": 0.05,
        }
    )
    result = triage_alert(_alert(), context, triage_client=fake)
    assert result == AlertTriageResult(
        decision="skip",
        security_relevance_probability=0.05,
        confidence=0.9,
        reasoning_summary="Jev score: security relevance=0.05.",
    )
    assert "src_identity" in fake.calls[0]["state"]
    assert "suricata_context" in fake.calls[0]["state"]

    low_confidence = FakeJev(
        {
            "unexpected_source": {"noul": 0.10, "confidence": 0.20},
            "suricata_relevant": {"noul": 0.05, "confidence": 0.90},
            "could_conceal": {"noul": 0.05, "confidence": 0.90},
            "enough_for_investigation": {"noul": 0.05, "confidence": 0.90},
        }
    )
    assert (
        triage_alert(_alert(), context, triage_client=low_confidence).decision
        == "investigate"
    )


def test_summarize_suricata_context_is_deterministic() -> None:
    alert = _alert()
    context_rows = [
        alert | {"timestamp": "2026-03-01T00:00:00+00:00"},
        alert
        | {
            "timestamp": "2026-03-01T00:01:00+00:00",
            "dest_ip": "2.2.2.2",
            "dest_port": 8443,
        },
        _alert("ET RELATED", 2100002, "2026-03-01T00:02:00+00:00", dest_ip="3.3.3.3"),
    ]
    summary = summarize_suricata_context(alert, _loki_payload(context_rows))
    assert summary.event_count == 3
    assert summary.same_signature_count == 2
    assert summary.same_source_count == 3
    assert summary.distinct_dest_ports == [443, 8443]
    assert summary.related_signatures == ["ET RELATED"]
    assert summary.fan_out is True
    assert summary.rapid_repeat is False
    assert summarize_suricata_context(alert, {"data": {"result": []}}).event_count == 0


def test_local_enrichment_reuses_compact_context() -> None:
    calls: list[str | None] = []

    def resolve(ip, observed_at):
        calls.append(ip)
        return {
            "ip": ip,
            "hostname": f"host-{ip}",
            "mac_address": None,
            "mac_vendor": None,
            "lookup_path": "test",
        }

    context = enrich_alert_locally(
        _alert(),
        identity_resolver=resolve,
        context_query=lambda _row: _loki_payload([_alert()]),
    )
    assert calls == ["10.0.0.5", "1.1.1.1"]
    assert context.src_identity.hostname == "host-10.0.0.5"
    assert context.suricata_context.event_count == 1
    assert "raw_alert" not in str(context.model_dump())


def test_skipped_alert_has_no_external_enrichment() -> None:
    calls: list[str] = []
    alert = _alert()
    triage = triage_alert(
        alert,
        _local_context(),
        triage_client=FakeJev(
            {
                "unexpected_source": 0.05,
                "suricata_relevant": 0.05,
                "could_conceal": 0.05,
                "enough_for_investigation": 0.05,
            }
        ),
    )
    if triage.decision == "investigate":
        enrich_alert_externally(
            alert,
            threat_intel_checker=lambda _ip: calls.append("threat") or {},
            cve_searcher=lambda _signature: calls.append("cve") or {},
        )
    assert calls == []


def test_final_reasoning_receives_local_and_external_context() -> None:
    class Structured:
        def invoke(self, prompt):
            assert "Summarized Suricata context" in prompt
            assert "CVE context" in prompt
            return {
                **_alert(),
                "asset_identity": "ignored",
                "verdict": "suspicious_monitor",
                "evidence": "Repeated connection.",
                "recommended_follow_up": "Review host.",
            }

    class Reasoning:
        def with_structured_output(self, _schema):
            return Structured()

    finding = analyze_enriched_alert(
        _alert(),
        _local_context(),
        ExternalAlertContext(
            threat_intel={"greynoise": {"classification": "benign"}},
            cve_context={"results": []},
        ),
        reasoning_llm=Reasoning(),
    )
    assert finding.verdict == "suspicious_monitor"
    assert finding.asset_identity == "host-01 / 00:11:22:33:44:55 / Example"


def test_final_reasoning_falls_back_to_destination_identity() -> None:
    unresolved_source = AssetIdentitySummary(
        ip="10.0.0.5",
        hostname=None,
        mac_address=None,
        mac_vendor=None,
        lookup_path="ipv4_dhcp",
        resolved=False,
    )
    destination = _local_context().dest_identity.model_copy(
        update={"hostname": "destination-01"}
    )

    class Structured:
        def invoke(self, _prompt):
            return {
                **_alert(),
                "verdict": "suspicious_monitor",
                "evidence": "Repeated connection.",
                "recommended_follow_up": "Review host.",
            }

    class Reasoning:
        def with_structured_output(self, _schema):
            return Structured()

    context = _local_context().model_copy(
        update={"src_identity": unresolved_source, "dest_identity": destination}
    )
    finding = analyze_enriched_alert(
        _alert(),
        context,
        ExternalAlertContext(threat_intel={}, cve_context={}),
        reasoning_llm=Reasoning(),
    )
    assert finding.asset_identity == "destination-01 / 00:11:22:33:44:55 / Example"


def test_end_to_end_rehydrates_only_investigated_signatures(monkeypatch) -> None:
    groups = [
        _signature_group("A", 1001, 1),
        _signature_group("B", 1002, 3),
        _signature_group("C", 1003, 1),
    ]
    rehydrated = {
        "A": [_alert("A", 1001)],
        "B": [
            _alert("B", 1002, dest_port=80),
            _alert("B", 1002, dest_port=81),
            _alert("B", 1002, dest_port=82),
        ],
        "C": [_alert("C", 1003)],
    }
    calls = {"rehydrate": [], "local": [], "external": [], "reasoning": []}

    class SequenceJev:
        def decide(self, *, state, questions):
            def answers(security, confidence, background=None):
                values = {
                    question_id: {"noul": security, "confidence": confidence}
                    for question_id in questions
                }
                if background is not None:
                    values["background_noise"] = {
                        "noul": background,
                        "confidence": confidence,
                    }
                return values

            if "signature" in state:
                if state["signature"] == "A":
                    return answers(0.05, 0.9, background=0.95)
                return answers(0.8, 0.9, background=0.1)
            signature_id = state["alert"]["signature_id"]
            if signature_id == 1002 and state["alert"]["dest_port"] in {80, 81}:
                return answers(0.05, 0.9)
            if signature_id == 1002:
                return answers(0.8, 0.9)
            return answers(0.1, 0.2)

    def fake_rehydrate(group, **_kwargs):
        calls["rehydrate"].append(group["signature"])
        result = dict(group)
        result["alerts"] = rehydrated[group["signature"]]
        return result

    def fake_local(alert, **_kwargs):
        calls["local"].append(alert["signature_id"])
        return _local_context()

    def fake_external(alert, **_kwargs):
        calls["external"].append(alert["signature_id"])
        return ExternalAlertContext(threat_intel={}, cve_context={})

    def fake_reasoning(alert, local, external, **_kwargs):
        calls["reasoning"].append(alert["signature_id"])
        return {
            **alert,
            "asset_identity": "host-01",
            "verdict": "suspicious_monitor",
            "evidence": "Needs review.",
            "recommended_follow_up": "Review host.",
            "lookup_warnings": [],
        }

    monkeypatch.setattr(
        cyber_analyst, "create_triage_client", lambda _vault: SequenceJev()
    )
    monkeypatch.setattr(cyber_analyst, "create_reasoning_llm", lambda _vault: object())
    monkeypatch.setattr(cyber_analyst, "rehydrate_signature_group", fake_rehydrate)
    monkeypatch.setattr(cyber_analyst, "enrich_alert_locally", fake_local)
    monkeypatch.setattr(cyber_analyst, "enrich_alert_externally", fake_external)
    monkeypatch.setattr(cyber_analyst, "analyze_enriched_alert", fake_reasoning)

    class Vault:
        def get(self, _name):
            return type("Connection", (), {"extra": {}, "password": "", "host": ""})()

    result = cyber_analyst.analyze_signatures(
        Vault(),
        {
            "window_start": "2026-03-01T00:00:00+00:00",
            "window_end": "2026-04-01T00:00:00+00:00",
            "signatures": groups,
            "test_mode": True,
        },
    )
    assert calls["rehydrate"] == ["B", "C"]
    assert calls["local"] == [1002, 1002, 1002, 1003]
    assert calls["external"] == [1002, 1003]
    assert calls["reasoning"] == [1002, 1003]
    assert result["routing_counters"]["signatures_skipped_by_jev"] == 1
    assert result["routing_counters"]["alerts_sent_to_reasoning_model"] == 2


def test_render_report_exposes_routing_without_suppression_rules() -> None:
    report = render_plaintext_report(
        run_id="manual",
        window_start=datetime(2026, 3, 1, tzinfo=UTC),
        window_end=datetime(2026, 4, 1, tzinfo=UTC),
        analyses=[
            {
                "signature_triage": {
                    "signature": "Noise",
                    "alert_count": 10,
                    "decision": "ignore_background_noise",
                    "reasoning_summary": "Routine.",
                    "confidence": 0.9,
                },
                "per_alert_results": [],
            }
        ],
    )
    assert "Ignored Signatures" in report
    assert "Routine." in report
    assert "suppression" not in report.lower()


def test_rate_limit_warning_mentions_temporary_unavailability() -> None:
    assert _rate_limit_warning("GreyNoise", "60") == (
        "GreyNoise lookup temporarily unavailable because the API rate limit "
        "was reached; retry after 60."
    )


def test_collection_limit_supports_test_mode() -> None:
    assert _collection_limit(True) == 1
    assert _collection_limit(False) == 5000


def test_watermark_advances_only_after_report_delivery(monkeypatch) -> None:
    updates: list[tuple[object, ...] | None] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def execute(self, sql, parameters=None) -> None:
            if sql.startswith("UPDATE automation_run_state"):
                updates.append(parameters)

        def fetchone(self):
            return (datetime(2026, 7, 1, tzinfo=UTC),)

    class Database:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def cursor(self):
            return Cursor()

        def commit(self) -> None:
            return None

    databases = iter([Database(), Database()])
    monkeypatch.setattr(
        cyber_analyst, "postgres_connect", lambda _connection: next(databases)
    )
    monkeypatch.setattr(cyber_analyst, "collect_signatures", lambda *args, **kwargs: {})
    monkeypatch.setattr(cyber_analyst, "analyze_signatures", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        cyber_analyst,
        "render_email_report",
        lambda *args, **kwargs: {"subject": "report", "body": "body"},
    )
    monkeypatch.setattr(
        cyber_analyst,
        "send_email",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("SMTP failed")),
    )

    class Vault:
        def get(self, _name):
            return object()

    import pytest

    with pytest.raises(RuntimeError, match="SMTP failed"):
        cyber_analyst.run(Vault())
    assert updates == []
