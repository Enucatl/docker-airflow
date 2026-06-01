from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
import json
from ipaddress import ip_address
from typing import Any, Callable


Decision = str
Verdict = str


def previous_calendar_month_window(reference: datetime) -> tuple[datetime, datetime]:
    current_month_start = reference.astimezone(UTC).replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    previous_month_end = current_month_start
    previous_month_start = (
        current_month_start.replace(day=1) - current_month_start.resolution
    ).replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return previous_month_start, previous_month_end


def _ns_to_iso8601(timestamp_ns: str) -> str:
    return datetime.fromtimestamp(int(timestamp_ns) / 1_000_000_000, tz=UTC).isoformat()


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def normalize_suricata_loki_response(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in payload.get("data", {}).get("result", []):
        stream = result.get("stream", {})
        for timestamp_ns, raw_line in result.get("values", []):
            parsed = json.loads(raw_line)
            alert = parsed.get("alert", {})
            category = alert.get("category") or parsed.get("category")
            signature = alert.get("signature") or parsed.get("signature")
            signature_id = alert.get("signature_id") or parsed.get("signature_id")
            if not category or not signature or signature_id in (None, ""):
                continue
            rows.append(
                {
                    "category": category,
                    "signature": signature,
                    "signature_id": int(signature_id),
                    "src_ip": parsed.get("src_ip"),
                    "dest_ip": parsed.get("dest_ip"),
                    "src_port": _int_or_none(parsed.get("src_port")),
                    "dest_port": _int_or_none(parsed.get("dest_port")),
                    "protocol": parsed.get("proto") or parsed.get("protocol"),
                    "observed_at": parsed.get("timestamp")
                    or parsed.get("@timestamp")
                    or _ns_to_iso8601(timestamp_ns),
                    "loki_stream": stream,
                    "raw_alert": parsed,
                }
            )
    rows.sort(key=lambda row: row["observed_at"])
    return rows


def group_alerts_by_signature(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["signature"]].append(row)

    groups: list[dict[str, Any]] = []
    for signature, signature_rows in sorted(grouped.items()):
        categories: list[str] = []
        signature_ids: list[int] = []
        for row in signature_rows:
            if row["category"] not in categories:
                categories.append(row["category"])
            if row["signature_id"] not in signature_ids:
                signature_ids.append(row["signature_id"])
        groups.append(
            {
                "signature": signature,
                "alert_count": len(signature_rows),
                "categories": categories,
                "signature_ids": signature_ids,
                "representative_categories": categories[:5],
                "representative_signatures": [signature],
                "example_alerts": signature_rows[:5],
                "alerts": signature_rows,
                "first_seen": min(row["observed_at"] for row in signature_rows),
                "last_seen": max(row["observed_at"] for row in signature_rows),
            }
        )
    return groups


def compact_signature_groups(
    signature_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    summary_fields = [
        "signature",
        "alert_count",
        "categories",
        "signature_ids",
        "representative_categories",
        "representative_signatures",
        "first_seen",
        "last_seen",
    ]
    return [
        {field: signature_group[field] for field in summary_fields}
        for signature_group in signature_groups
    ]


def build_signature_summary_for_llm(signature_group: dict[str, Any]) -> dict[str, Any]:
    return {
        "signature": signature_group["signature"],
        "alert_count": signature_group["alert_count"],
        "categories": signature_group["categories"],
        "signature_ids": signature_group["signature_ids"],
        "representative_categories": signature_group["representative_categories"],
        "first_seen": signature_group["first_seen"],
        "last_seen": signature_group["last_seen"],
    }


def extract_ipv4_identity(
    ip: str,
    dhcp_rows: list[dict[str, Any]],
    vendor_lookup: Callable[[str], str | None],
) -> dict[str, Any]:
    for row in dhcp_rows:
        if row.get("ip_address") != ip:
            continue
        mac_address = row.get("mac_address")
        return {
            "ip": ip,
            "mac_address": mac_address,
            "hostname": row.get("hostname"),
            "mac_vendor": vendor_lookup(mac_address) if mac_address else None,
            "lookup_path": "ipv4_dhcp",
        }
    return {
        "ip": ip,
        "mac_address": None,
        "hostname": None,
        "mac_vendor": None,
        "lookup_path": "ipv4_dhcp",
        "warning": f"No DHCP identity found for {ip}",
    }


def extract_ipv6_identity(
    ip: str,
    ndp_rows: list[dict[str, Any]],
    dhcp_rows: list[dict[str, Any]],
    vendor_lookup: Callable[[str], str | None],
) -> dict[str, Any]:
    mac_address: str | None = None
    for row in ndp_rows:
        if row.get("ipv6_address") == ip:
            mac_address = row.get("mac_address")
            break

    hostname: str | None = None
    if mac_address:
        for row in dhcp_rows:
            if row.get("mac_address") == mac_address:
                hostname = row.get("hostname")
                break

    warning_parts: list[str] = []
    if not mac_address:
        warning_parts.append(f"No NDP MAC match for {ip}")
    if mac_address and not hostname:
        warning_parts.append(f"No DHCP hostname match for {mac_address}")

    identity = {
        "ip": ip,
        "mac_address": mac_address,
        "hostname": hostname,
        "mac_vendor": vendor_lookup(mac_address) if mac_address else None,
        "lookup_path": "ipv6_ndp_dhcp",
    }
    if warning_parts:
        identity["warning"] = "; ".join(warning_parts)
    return identity


def build_suppression_candidates(
    signature_group: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for signature_id in signature_group["signature_ids"]:
        candidates.append(
            {
                "signature_id": signature_id,
                "rule_line": f"suppress gen_id 1, sig_id {signature_id}",
                "scope_reason": (
                    f"Signature {signature_group['signature']} was classified as background noise "
                    "for the full reporting window."
                ),
            }
        )
    return candidates


def run_signature_agent(
    signature_group: dict[str, Any],
    decide_signature: Callable[[dict[str, Any]], dict[str, Any]],
    analyze_alert: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    signature_result = decide_signature(signature_group)
    if signature_result["decision"] == "ignore_background_noise":
        signature_result["suppression_candidates"] = build_suppression_candidates(
            signature_group
        )
        return {
            "signature_result": signature_result,
            "per_alert_results": [],
            "unresolved_lookups": [],
        }

    per_alert_results: list[dict[str, Any]] = []
    unresolved_lookups: list[str] = []
    for alert in signature_group["alerts"]:
        finding = analyze_alert(alert, signature_result)
        per_alert_results.append(finding)
        for warning in finding.get("lookup_warnings", []):
            if warning not in unresolved_lookups:
                unresolved_lookups.append(warning)
    signature_result["suppression_candidates"] = []
    return {
        "signature_result": signature_result,
        "per_alert_results": per_alert_results,
        "unresolved_lookups": unresolved_lookups,
    }


def render_plaintext_report(
    *,
    run_id: str | None = None,
    window_start: datetime,
    window_end: datetime,
    analyses: list[dict[str, Any]],
) -> str:
    run_id_line = f"run_id={run_id}" if run_id else "run_id=unknown"

    lines = [
        run_id_line,
        "",
        "Monthly Suricata AI Triage",
        "",
        (
            "Window: "
            f"{window_start.date().isoformat()} through "
            f"{(window_end.date()).isoformat()} (end exclusive)"
        ),
        "",
        "Signature Summary",
    ]

    for analysis in analyses:
        signature_result = analysis["signature_result"]
        lines.append(
            f"- {signature_result['signature']}: {signature_result['alert_count']} alerts, "
            f"decision={signature_result['decision']}, confidence={signature_result['confidence']}"
        )

    ignored = [
        analysis
        for analysis in analyses
        if analysis["signature_result"]["decision"] == "ignore_background_noise"
    ]
    lines.extend(["", "Ignored Signatures"])
    if not ignored:
        lines.append("- None")
    for analysis in ignored:
        signature_result = analysis["signature_result"]
        lines.append(
            f"- {signature_result['signature']}: {signature_result['reasoning_summary']}"
        )
        for candidate in signature_result["suppression_candidates"]:
            lines.append(f"  # {candidate['scope_reason']}")
            lines.append(f"  threshold.config: {candidate['rule_line']}")

    analyzed = [
        analysis
        for analysis in analyses
        if analysis["signature_result"]["decision"] == "analyze_individually"
    ]
    lines.extend(["", "Signatures Analyzed Individually"])
    if not analyzed:
        lines.append("- None")
    else:
        for analysis in analyzed:
            lines.append(f"- {analysis['signature_result']['signature']}")

    verdict_sections: list[tuple[Verdict, str]] = [
        ("proof_of_malware", "Proof of Malware"),
        ("suspicious_monitor", "Suspicious / Monitor"),
        ("false_positive_monitor", "False Positive / Continue Monitoring"),
    ]
    for verdict, title in verdict_sections:
        lines.extend(["", title])
        matches = [
            result
            for analysis in analyses
            for result in analysis["per_alert_results"]
            if result["verdict"] == verdict
        ]
        if not matches:
            lines.append("- None")
            continue
        for match in matches:
            lines.append(
                f"- {match['observed_at']} {match['signature']} "
                f"{match['src_ip']} -> {match['dest_ip']}: {match['evidence']}"
            )
            lines.append(f"  follow_up: {match['recommended_follow_up']}")

    unresolved = sorted(
        {
            warning
            for analysis in analyses
            for warning in analysis.get("unresolved_lookups", [])
        }
    )
    lines.extend(["", "Unresolved Lookups"])
    if not unresolved:
        lines.append("- None")
    else:
        for warning in unresolved:
            lines.append(f"- {warning}")

    instruction_lines = [
        "",
        "Coding Agent Instruction",
        "",
        "Open provisioning/templates/app_configs/suricata-disable.conf.j2.",
        "Add the ignore rules below.",
        "Keep the reason comment and the run_id provenance with each rule.",
        "",
        f"Provenance: {run_id_line}",
        "",
    ]
    if not ignored:
        instruction_lines.append("No ignore rules were generated for this report.")
    else:
        for analysis in ignored:
            signature_result = analysis["signature_result"]
            instruction_lines.append(
                f"- {signature_result['signature']}: {signature_result['reasoning_summary']}"
            )
            for candidate in signature_result["suppression_candidates"]:
                instruction_lines.append(
                    f"  # run_id={run_id or 'unknown'} | {candidate['scope_reason']}"
                )
                instruction_lines.append(
                    f"  threshold.config: {candidate['rule_line']}"
                )

    lines.extend(instruction_lines)

    return "\n".join(lines)


def infer_identity_lookup_version(ip: str) -> str:
    return "ipv6" if ip_address(ip).version == 6 else "ipv4"
