from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import json
import logging
import os
from ipaddress import ip_address
from typing import Any, Literal, TypedDict

import niquests
from pydantic import BaseModel, Field

from automation_core.clients import postgres_connect, send_email
from automation_core.connections import VaultConnections
from common.loki import query_loki_range, query_loki_range_adaptive, set_vault
from common.ssl import verify
from common.suricata_monthly_triage import (
    compact_signature_groups,
    extract_ipv4_identity,
    extract_ipv6_identity,
    group_alerts_by_signature,
    infer_identity_lookup_version,
    normalize_suricata_loki_response,
    render_plaintext_report,
)


ALERT_FROM = "gmatteo.abis+airflow.docker.home.arpa@gmail.com"
ALERT_TO = "m.app.logins@pm.me"

SIGNATURE_INVESTIGATE_THRESHOLD = 0.50
SIGNATURE_BACKGROUND_THRESHOLD = 0.80
SIGNATURE_MIN_CONFIDENCE = 0.70
ALERT_INVESTIGATE_THRESHOLD = 0.50
ALERT_MIN_CONFIDENCE = 0.70

LOKI_CONN_ID = "loki"
JEV_API_PATH = "/v1/systemone"


class SignatureTriageResult(BaseModel):
    """Represents Jev's routing result for one alert signature."""

    decision: Literal["ignore_background_noise", "investigate"]
    security_relevance_probability: float = Field(ge=0, le=1)
    background_noise_probability: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    reasoning_summary: str | None = None
    signature: str = ""
    alert_count: int = 0


class AlertTriageResult(BaseModel):
    """Represents Jev's routing result for one normalized alert."""

    decision: Literal["skip", "investigate"]
    security_relevance_probability: float = Field(ge=0, le=1)
    confidence: float = Field(ge=0, le=1)
    reasoning_summary: str | None = None


class AssetIdentitySummary(BaseModel):
    """Compact identity information for a source or destination asset."""

    ip: str | None
    hostname: str | None
    mac_address: str | None
    mac_vendor: str | None
    lookup_path: str | None
    resolved: bool
    warning: str | None = None


class SuricataContextSummary(BaseModel):
    """Deterministic aggregate features from nearby Suricata events."""

    event_count: int
    same_signature_count: int
    same_source_count: int
    same_destination_count: int
    distinct_src_ips: int
    distinct_dest_ips: int
    distinct_dest_ports: list[int]
    distinct_protocols: list[str]
    related_signatures: list[str]
    first_event_at: str | None
    last_event_at: str | None
    fan_out: bool
    rapid_repeat: bool


class LocalAlertContext(BaseModel):
    """All cheap, local evidence collected before alert-level routing."""

    src_identity: AssetIdentitySummary
    dest_identity: AssetIdentitySummary
    suricata_context: SuricataContextSummary
    lookup_warnings: list[str] = Field(default_factory=list)


class ExternalAlertContext(BaseModel):
    """External threat-intelligence and CVE context for an investigated alert."""

    threat_intel: dict[str, object]
    cve_context: dict[str, object]
    lookup_warnings: list[str] = Field(default_factory=list)


class AlertFinding(BaseModel):
    """Final strong-model finding for a fully enriched alert."""

    signature: str
    signature_id: int
    category: str
    src_ip: str | None
    dest_ip: str | None
    src_port: int | None
    dest_port: int | None
    protocol: str | None
    observed_at: str
    asset_identity: str = "unresolved"
    verdict: Literal[
        "proof_of_malware",
        "suspicious_monitor",
        "false_positive_monitor",
    ]
    evidence: str
    recommended_follow_up: str
    lookup_warnings: list[str] = Field(default_factory=list)


class SignatureAgentState(TypedDict, total=False):
    """LangGraph state for the signature-first workflow."""

    signature_group: dict[str, Any]
    signature_triage: dict[str, Any]
    rehydrated_group: dict[str, Any]
    per_alert_results: list[dict[str, Any]]
    unresolved_lookups: list[str]
    alerts_rehydrated: int
    alerts_skipped_by_jev: int
    alerts_sent_to_external: int
    reasoning_calls: int


@dataclass(frozen=True)
class JevJudgment:
    """Normalized answer returned by the Jev provider boundary."""

    probability: float
    confidence: float


class JevClient:
    """Minimal client for TypeSafe's typed Jev evaluation endpoint."""

    def __init__(
        self,
        *,
        endpoint: str,
        api_key: str,
        model: str = "jev-latest",
        timeout: float = 30.0,
    ) -> None:
        """Initialize a Jev client.

        Args:
            endpoint: TypeSafe API base URL or complete System One URL.
            api_key: Server-side TypeSafe API key.
            model: Jev model identifier.
            timeout: Request timeout in seconds.
        """
        endpoint = endpoint.rstrip("/")
        if not endpoint.startswith(("http://", "https://")):
            endpoint = f"https://{endpoint}"
        self.endpoint = (
            endpoint if endpoint.endswith(JEV_API_PATH) else endpoint + JEV_API_PATH
        )
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    def decide(
        self,
        *,
        state: dict[str, object],
        questions: dict[str, dict[str, object]],
    ) -> dict[str, JevJudgment]:
        """Evaluate typed questions against one application state."""
        response = niquests.post(
            self.endpoint,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            json={"model": self.model, "state": state, "questions": questions},
            timeout=self.timeout,
            verify=verify,
        )
        try:
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"Jev request failed: {exc}") from exc
        answers = payload.get("answers", payload)
        if not isinstance(answers, dict):
            raise RuntimeError("Jev response did not contain an answers object")
        return {
            str(question_id): _normalize_jev_judgment(answer)
            for question_id, answer in answers.items()
        }


def _rate_limit_warning(provider: str, retry_after: str | None = None) -> str:
    """Build a user-facing warning for a rate-limited lookup."""
    warning = f"{provider} lookup temporarily unavailable because the API rate limit was reached"
    return f"{warning}; retry after {retry_after}." if retry_after else f"{warning}."


def _collection_limit(test_mode: bool) -> int:
    """Return the Loki collection limit for the selected run mode."""
    return 1 if test_mode else 5000


def _clamp_probability(value: object, default: float = 0.5) -> float:
    """Convert a provider value into a bounded probability."""
    if isinstance(value, bool):
        return float(value)
    try:
        return max(0.0, min(1.0, float(value)))
    except TypeError, ValueError:
        return default


def _normalize_jev_judgment(answer: object) -> JevJudgment:
    """Normalize one TypeSafe answer into a probability and confidence."""
    if isinstance(answer, JevJudgment):
        return answer
    if isinstance(answer, (bool, int, float)):
        probability = _clamp_probability(answer)
        return JevJudgment(probability, 2 * abs(probability - 0.5))
    if not isinstance(answer, dict):
        return JevJudgment(0.5, 0.0)

    probability: float | None = None
    for key in ("noul", "probability", "value"):
        if key in answer and isinstance(answer[key], (bool, int, float)):
            probability = _clamp_probability(answer[key])
            break
    if probability is None and isinstance(answer.get("choice"), str):
        probabilities = answer.get("probabilities")
        choice = answer["choice"]
        if isinstance(probabilities, dict):
            probability = _clamp_probability(probabilities.get(choice))
    if probability is None:
        probabilities = answer.get("probabilities")
        if isinstance(probabilities, dict) and probabilities:
            probability = max(
                _clamp_probability(value) for value in probabilities.values()
            )
    if probability is None:
        probability = 0.5

    raw_confidence = answer.get("confidence")
    if isinstance(raw_confidence, str):
        raw_confidence = {"high": 0.9, "medium": 0.6, "low": 0.3}.get(
            raw_confidence.lower(), 0.0
        )
    confidence = _clamp_probability(raw_confidence, default=2 * abs(probability - 0.5))
    return JevJudgment(probability, confidence)


def _signature_state(signature_group: dict[str, object]) -> dict[str, object]:
    """Select only the compact fields allowed at signature triage."""
    fields = (
        "signature",
        "alert_count",
        "categories",
        "signature_ids",
        "representative_categories",
        "first_seen",
        "last_seen",
    )
    return {field: signature_group.get(field) for field in fields}


def _route_confidence(judgments: list[JevJudgment]) -> float:
    """Use the least certain atomic judgment for recall-oriented routing."""
    return min((judgment.confidence for judgment in judgments), default=0.0)


def _call_triage_client(
    triage_client: object,
    *,
    state: dict[str, object],
    questions: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Call an injected Jev-compatible client for unit-testable routing."""
    decide = getattr(triage_client, "decide", None)
    if decide is None:
        raise TypeError("triage_client must provide decide(state=..., questions=...)")
    result = decide(state=state, questions=questions)
    if isinstance(result, dict):
        return result
    raise TypeError("triage_client returned a non-object decision")


def _answer_for(answers: dict[str, object], key: str) -> JevJudgment:
    """Read and normalize a named answer, conservatively when absent."""
    return _normalize_jev_judgment(answers.get(key))


def triage_signature(
    signature_group: dict[str, object],
    *,
    triage_client: object,
) -> SignatureTriageResult:
    """Route one compact signature group through Jev."""
    state = _signature_state(signature_group)
    questions = {
        "security_relevant": {
            "type": "noul",
            "instructions": "Could ignoring this signature conceal a security-relevant event?",
            "criteria": {
                "true": "Security-relevant activity is plausible.",
                "false": "This is routine noise.",
            },
        },
        "background_noise": {
            "type": "noul",
            "instructions": "Is this signature consistent with routine background network noise?",
            "criteria": {
                "true": "Routine background noise.",
                "false": "Not routine background noise.",
            },
        },
        "needs_inspection": {
            "type": "noul",
            "instructions": "Does this signature warrant inspection of its individual alerts?",
            "criteria": {
                "true": "Inspect individual alerts.",
                "false": "No individual inspection needed.",
            },
        },
    }
    try:
        answers = _call_triage_client(triage_client, state=state, questions=questions)
        security_judgment = _answer_for(answers, "security_relevant")
        background_judgment = _answer_for(answers, "background_noise")
        inspection_judgment = _answer_for(answers, "needs_inspection")
        security = security_judgment.probability
        background = background_judgment.probability
        confidence = _route_confidence(
            [security_judgment, background_judgment, inspection_judgment]
        )
    except Exception:
        security = 0.5
        background = 0.5
        confidence = 0.0
        inspection_judgment = JevJudgment(0.5, 0.0)
        reasoning = "Jev signature triage was unavailable; routed to investigation."
    else:
        reasoning = (
            f"Jev scores: security relevance={security:.2f}, "
            f"background noise={background:.2f}."
        )

    investigate = (
        security >= SIGNATURE_INVESTIGATE_THRESHOLD
        or background < SIGNATURE_BACKGROUND_THRESHOLD
        or inspection_judgment.probability >= SIGNATURE_INVESTIGATE_THRESHOLD
        or confidence < SIGNATURE_MIN_CONFIDENCE
    )
    return SignatureTriageResult(
        decision="investigate" if investigate else "ignore_background_noise",
        security_relevance_probability=security,
        background_noise_probability=background,
        confidence=confidence,
        reasoning_summary=reasoning,
        signature=str(signature_group["signature"]),
        alert_count=int(signature_group["alert_count"]),
    )


def triage_alert(
    alert_row: dict[str, object],
    local_context: LocalAlertContext,
    *,
    triage_client: object,
) -> AlertTriageResult:
    """Route one locally enriched alert through Jev."""
    alert_fields = {
        field: alert_row.get(field)
        for field in (
            "signature",
            "signature_id",
            "category",
            "src_ip",
            "dest_ip",
            "src_port",
            "dest_port",
            "protocol",
            "observed_at",
        )
    }
    state = {
        "alert": alert_fields,
        "src_identity": local_context.src_identity.model_dump(),
        "dest_identity": local_context.dest_identity.model_dump(),
        "suricata_context": local_context.suricata_context.model_dump(),
    }
    questions = {
        "unexpected_source": {
            "type": "noul",
            "instructions": "Is this activity unexpected for the identified source asset?",
        },
        "suricata_relevant": {
            "type": "noul",
            "instructions": "Does the surrounding Suricata activity make this alert more security-relevant?",
        },
        "could_conceal": {
            "type": "noul",
            "instructions": "Could skipping deeper investigation conceal suspicious behavior?",
        },
        "enough_for_investigation": {
            "type": "noul",
            "instructions": "Is there enough evidence to justify deeper investigation?",
        },
    }
    try:
        answers = _call_triage_client(triage_client, state=state, questions=questions)
        judgments = [
            _answer_for(answers, key)
            for key in (
                "unexpected_source",
                "suricata_relevant",
                "could_conceal",
                "enough_for_investigation",
            )
        ]
        security = max(judgment.probability for judgment in judgments)
        confidence = _route_confidence(judgments)
    except Exception:
        security = 0.5
        confidence = 0.0
        reasoning = "Jev alert triage was unavailable; routed to investigation."
    else:
        reasoning = f"Jev score: security relevance={security:.2f}."

    investigate = (
        security >= ALERT_INVESTIGATE_THRESHOLD or confidence < ALERT_MIN_CONFIDENCE
    )
    return AlertTriageResult(
        decision="investigate" if investigate else "skip",
        security_relevance_probability=security,
        confidence=confidence,
        reasoning_summary=reasoning,
    )


def _parse_json_lines(payload: dict[str, object]) -> list[dict[str, object]]:
    """Parse JSON lines returned by a Loki range query."""
    rows: list[dict[str, object]] = []
    data = payload.get("data", {})
    for result in data.get("result", []):
        for _timestamp_ns, raw_line in result.get("values", []):
            parsed = json.loads(raw_line)
            if isinstance(parsed, dict):
                rows.append(parsed)
    return rows


def _check_mac_vendor(mac_address: str | None) -> str | None:
    """Resolve a MAC vendor without making identity lookup fatal."""
    if not mac_address:
        return None
    try:
        from mac_vendor_lookup import MacLookup

        return MacLookup().lookup(mac_address)
    except Exception:
        return None


def resolve_network_identity(
    ip: str | None,
    observed_at: str,
    *,
    query_loki: Callable[..., dict[str, object]] | None = None,
    vendor_lookup: Callable[[str], str | None] | None = None,
) -> dict[str, object]:
    """Resolve one IP through local DHCP/NDP data."""
    if not ip:
        return {
            "ip": None,
            "mac_address": None,
            "hostname": None,
            "mac_vendor": None,
            "lookup_path": None,
            "warning": "No IP available for identity lookup",
        }
    try:
        lookup_version = infer_identity_lookup_version(ip)
    except ValueError:
        return {
            "ip": ip,
            "mac_address": None,
            "hostname": None,
            "mac_vendor": None,
            "lookup_path": None,
            "warning": f"Invalid IP for identity lookup: {ip}",
        }

    observation_time = datetime.fromisoformat(observed_at)
    start = observation_time - timedelta(hours=12)
    end = observation_time + timedelta(hours=1)
    query = query_loki or query_loki_range
    vendor = vendor_lookup or _check_mac_vendor
    dhcp_rows = _parse_json_lines(
        query(
            LOKI_CONN_ID,
            query='{job="kea-dhcp4"} | json',
            start=start,
            end=end,
            limit=500,
        )
    )
    if lookup_version == "ipv4":
        return extract_ipv4_identity(ip, dhcp_rows, vendor)

    ndp_rows = _parse_json_lines(
        query(
            LOKI_CONN_ID,
            query='{job="ndp"} | json',
            start=start,
            end=end,
            limit=500,
        )
    )
    return extract_ipv6_identity(ip, ndp_rows, dhcp_rows, vendor)


def query_suricata_context(
    alert_row: dict[str, object],
    *,
    query_loki: Callable[..., dict[str, object]] | None = None,
) -> dict[str, object]:
    """Fetch nearby Suricata context for one normalized alert."""
    observed_at = datetime.fromisoformat(str(alert_row["observed_at"]))
    start = observed_at - timedelta(minutes=15)
    end = observed_at + timedelta(minutes=15)
    filters = ['{job="suricata"} | json']
    for field in ("src_ip", "dest_ip"):
        if alert_row.get(field):
            filters.append(f'{field}="{alert_row[field]}"')
    if len(filters) == 1:
        filters.append(f'alert_signature_id="{alert_row["signature_id"]}"')
    payload = (query_loki or query_loki_range)(
        LOKI_CONN_ID,
        query=" | ".join(filters),
        start=start,
        end=end,
        limit=200,
    )
    logging.getLogger(__name__).info(
        "Queried local Suricata context signature=%s events=%s",
        alert_row.get("signature"),
        sum(
            len(result.get("values", []))
            for result in payload.get("data", {}).get("result", [])
        ),
    )
    return payload


def _as_utc(value: str) -> datetime:
    """Parse a timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def summarize_suricata_context(
    alert_row: dict[str, object],
    payload: dict[str, object],
) -> SuricataContextSummary:
    """Reduce raw Loki context into deterministic routing features."""
    rows = normalize_suricata_loki_response(payload)
    signature = alert_row.get("signature")
    source = alert_row.get("src_ip")
    destination = alert_row.get("dest_ip")
    same_signature_rows = [row for row in rows if row.get("signature") == signature]
    same_source_rows = [row for row in rows if source and row.get("src_ip") == source]
    same_destination_rows = [
        row for row in rows if destination and row.get("dest_ip") == destination
    ]
    timestamps = [row["observed_at"] for row in rows]
    distinct_dest_ports = sorted(
        {int(row["dest_port"]) for row in rows if row.get("dest_port") is not None}
    )
    distinct_protocols = sorted(
        {str(row["protocol"]) for row in rows if row.get("protocol")}
    )
    related_signatures = sorted(
        {
            str(row["signature"])
            for row in rows
            if row.get("signature") and row.get("signature") != signature
        }
    )
    rapid_repeat = False
    if len(same_signature_rows) >= 3:
        repeat_times = [_as_utc(str(row["observed_at"])) for row in same_signature_rows]
        rapid_repeat = max(repeat_times) - min(repeat_times) <= timedelta(minutes=5)
    return SuricataContextSummary(
        event_count=len(rows),
        same_signature_count=len(same_signature_rows),
        same_source_count=len(same_source_rows),
        same_destination_count=len(same_destination_rows),
        distinct_src_ips=len({row["src_ip"] for row in rows if row.get("src_ip")}),
        distinct_dest_ips=len({row["dest_ip"] for row in rows if row.get("dest_ip")}),
        distinct_dest_ports=distinct_dest_ports,
        distinct_protocols=distinct_protocols,
        related_signatures=related_signatures,
        first_event_at=min(timestamps) if timestamps else None,
        last_event_at=max(timestamps) if timestamps else None,
        fan_out=len({row["dest_ip"] for row in rows if row.get("dest_ip")}) > 1
        or len(distinct_dest_ports) > 1,
        rapid_repeat=rapid_repeat,
    )


def _identity_summary(identity: dict[str, object]) -> AssetIdentitySummary:
    """Convert a local identity lookup into its compact application model."""
    return AssetIdentitySummary(
        ip=identity.get("ip"),
        hostname=identity.get("hostname"),
        mac_address=identity.get("mac_address"),
        mac_vendor=identity.get("mac_vendor"),
        lookup_path=identity.get("lookup_path"),
        resolved=bool(identity.get("hostname") or identity.get("mac_address")),
        warning=identity.get("warning"),
    )


def enrich_alert_locally(
    alert_row: dict[str, object],
    *,
    identity_resolver: Callable[..., dict[str, object]] | None = None,
    context_query: Callable[..., dict[str, object]] | None = None,
    vendor_lookup: Callable[[str], str | None] | None = None,
) -> LocalAlertContext:
    """Collect and summarize all cheap local evidence for an alert."""
    resolver = identity_resolver or resolve_network_identity
    if vendor_lookup is None:
        src_identity = resolver(alert_row.get("src_ip"), str(alert_row["observed_at"]))
        dest_identity = resolver(
            alert_row.get("dest_ip"), str(alert_row["observed_at"])
        )
    else:
        src_identity = resolver(
            alert_row.get("src_ip"),
            str(alert_row["observed_at"]),
            vendor_lookup=vendor_lookup,
        )
        dest_identity = resolver(
            alert_row.get("dest_ip"),
            str(alert_row["observed_at"]),
            vendor_lookup=vendor_lookup,
        )
    payload = (
        context_query(alert_row) if context_query else query_suricata_context(alert_row)
    )
    local_context = LocalAlertContext(
        src_identity=_identity_summary(src_identity),
        dest_identity=_identity_summary(dest_identity),
        suricata_context=summarize_suricata_context(alert_row, payload),
    )
    local_context.lookup_warnings = [
        warning
        for warning in (src_identity.get("warning"), dest_identity.get("warning"))
        if warning
    ]
    return local_context


def check_threat_intel(
    ip: str | None,
    *,
    get_connection: Callable[[str], tuple[object, dict[str, object]]] | None = None,
) -> dict[str, object]:
    """Query GreyNoise and AbuseIPDB for a public source IP."""
    if not ip:
        return {"warning": "No IP available for threat intel lookup"}
    try:
        if ip_address(ip).is_private:
            return {"note": "Private IP skipped for threat intel"}
    except ValueError:
        return {"warning": f"Invalid IP for threat intel lookup: {ip}"}
    if get_connection is None:
        return {"warning": "Threat intel connection is not configured"}

    findings: dict[str, object] = {}
    greynoise_conn, greynoise_extra = get_connection("greynoise")
    greynoise_headers = {"Accept": "application/json"}
    if api_key := greynoise_conn.password or greynoise_extra.get("api_key"):
        greynoise_headers["key"] = api_key
    try:
        response = niquests.get(
            f"{greynoise_conn.host.rstrip('/')}/v3/community/{ip}",
            headers=greynoise_headers,
            timeout=30,
            verify=verify,
        )
        response.raise_for_status()
        findings["greynoise"] = response.json()
    except niquests.HTTPError as exc:
        response = exc.response
        findings["greynoise_warning"] = (
            _rate_limit_warning("GreyNoise", response.headers.get("Retry-After"))
            if response is not None and response.status_code == 429
            else f"GreyNoise lookup failed for {ip}: {exc}"
        )
    except Exception as exc:
        findings["greynoise_warning"] = f"GreyNoise lookup failed for {ip}: {exc}"

    abuse_conn, abuse_extra = get_connection("abuseipdb")
    abuse_headers = {"Accept": "application/json"}
    if api_key := abuse_conn.password or abuse_extra.get("api_key"):
        abuse_headers["Key"] = api_key
    try:
        response = niquests.get(
            f"{abuse_conn.host.rstrip('/')}/api/v2/check",
            params={"ipAddress": ip, "maxAgeInDays": 90},
            headers=abuse_headers,
            timeout=30,
            verify=verify,
        )
        response.raise_for_status()
        findings["abuseipdb"] = response.json()
    except niquests.HTTPError as exc:
        response = exc.response
        findings["abuseipdb_warning"] = (
            _rate_limit_warning("AbuseIPDB", response.headers.get("Retry-After"))
            if response is not None and response.status_code == 429
            else f"AbuseIPDB lookup failed for {ip}: {exc}"
        )
    except Exception as exc:
        findings["abuseipdb_warning"] = f"AbuseIPDB lookup failed for {ip}: {exc}"
    return findings


def search_cve(
    signature: str,
    *,
    get_connection: Callable[[str], tuple[object, dict[str, object]]] | None = None,
) -> dict[str, object]:
    """Search the configured external provider for CVE context."""
    if get_connection is None:
        return {
            "warning": f"Missing CVE context for {signature}: connection unavailable"
        }
    tavily_conn, tavily_extra = get_connection("tavily")
    api_key = tavily_conn.password or tavily_extra.get("api_key")
    try:
        response = niquests.post(
            f"{tavily_conn.host.rstrip('/')}/search",
            json={
                "api_key": api_key,
                "query": f"{signature} CVE",
                "search_depth": "advanced",
                "max_results": 5,
            },
            timeout=30,
            verify=verify,
        )
        response.raise_for_status()
        return response.json()
    except niquests.HTTPError as exc:
        response = exc.response
        if response is not None and response.status_code == 429:
            return {
                "warning": _rate_limit_warning(
                    "Tavily", response.headers.get("Retry-After")
                )
            }
        return {"warning": f"Missing CVE context for {signature}: {exc}"}
    except Exception as exc:
        return {"warning": f"Missing CVE context for {signature}: {exc}"}


def enrich_alert_externally(
    alert_row: dict[str, object],
    *,
    threat_intel_checker: Callable[[str | None], dict[str, object]] | None = None,
    cve_searcher: Callable[[str], dict[str, object]] | None = None,
) -> ExternalAlertContext:
    """Perform expensive external enrichment after alert-level Jev routing."""
    threat_intel = (threat_intel_checker or check_threat_intel)(alert_row.get("src_ip"))
    cve_context = (cve_searcher or search_cve)(str(alert_row["signature"]))
    warnings = [
        str(value)
        for value in (
            threat_intel.get("greynoise_warning"),
            threat_intel.get("abuseipdb_warning"),
            threat_intel.get("warning"),
            cve_context.get("warning"),
        )
        if value
    ]
    return ExternalAlertContext(
        threat_intel=threat_intel,
        cve_context=cve_context,
        lookup_warnings=warnings,
    )


def analyze_enriched_alert(
    alert_row: dict[str, object],
    local_context: LocalAlertContext,
    external_context: ExternalAlertContext,
    *,
    reasoning_llm: object,
) -> AlertFinding:
    """Ask only the strong reasoning model for the final alert finding."""
    prompt_alert = {
        field: alert_row.get(field)
        for field in (
            "signature",
            "signature_id",
            "category",
            "src_ip",
            "dest_ip",
            "src_port",
            "dest_port",
            "protocol",
            "observed_at",
        )
    }
    prompt = (
        "Analyze this fully enriched Suricata alert and produce one concise finding. "
        "Do not make a routing decision outside the requested verdict schema.\n\n"
        f"Original normalized alert:\n{json.dumps(prompt_alert, default=str)}\n\n"
        f"Source identity summary:\n{json.dumps(local_context.src_identity.model_dump(), default=str)}\n\n"
        f"Destination identity summary:\n{json.dumps(local_context.dest_identity.model_dump(), default=str)}\n\n"
        f"Summarized Suricata context:\n{json.dumps(local_context.suricata_context.model_dump(), default=str)}\n\n"
        f"GreyNoise / AbuseIPDB information:\n{json.dumps(external_context.threat_intel, default=str)}\n\n"
        f"CVE context:\n{json.dumps(external_context.cve_context, default=str)}\n\n"
        f"Lookup warnings:\n{json.dumps(local_context.lookup_warnings + external_context.lookup_warnings)}"
    )
    raw_finding = reasoning_llm.with_structured_output(AlertFinding).invoke(prompt)
    finding = (
        raw_finding.model_dump()
        if isinstance(raw_finding, BaseModel)
        else dict(raw_finding)
    )
    finding = AlertFinding.model_validate(finding)
    source_identity_parts = [
        local_context.src_identity.hostname,
        local_context.src_identity.mac_address,
        local_context.src_identity.mac_vendor,
    ]
    destination_identity_parts = [
        local_context.dest_identity.hostname,
        local_context.dest_identity.mac_address,
        local_context.dest_identity.mac_vendor,
    ]
    identity_parts = (
        source_identity_parts
        if any(source_identity_parts)
        else destination_identity_parts
    )
    return finding.model_copy(
        update={
            "asset_identity": " / ".join(part for part in identity_parts if part)
            or "unresolved",
            "lookup_warnings": local_context.lookup_warnings
            + external_context.lookup_warnings,
        }
    )


def _parse_collected_datetime(value: object) -> datetime:
    """Parse a collected signature timestamp in UTC."""
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _signature_alert_query(signature_group: dict[str, object]) -> str:
    """Build a Loki query for all alerts belonging to a signature group."""
    signature_ids = list(signature_group.get("signature_ids", []))
    query = '{job="suricata"} | json | event_type="alert"'
    if len(signature_ids) == 1:
        return f'{query} | alert_signature_id="{signature_ids[0]}"'
    if len(signature_ids) > 1:
        pattern = "|".join(str(signature_id) for signature_id in signature_ids)
        return f'{query} | alert_signature_id=~"^({pattern})$"'
    return query


def rehydrate_signature_group(
    signature_group: dict[str, object],
    *,
    query_loki: Callable[..., list[dict[str, object]]] | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, object]:
    """Re-query individual normalized alerts for an investigated signature."""
    query = _signature_alert_query(signature_group)
    signature_ids = {int(value) for value in signature_group.get("signature_ids", [])}
    start = _parse_collected_datetime(signature_group["first_seen"])
    end = _parse_collected_datetime(signature_group["last_seen"])
    if end <= start:
        end = start + timedelta(microseconds=1)
    active_logger = logger or logging.getLogger(__name__)
    results = (query_loki or query_loki_range_adaptive)(
        LOKI_CONN_ID,
        query=query,
        start=start,
        end=end,
        limit=5000,
        logger=active_logger,
    )
    rows = normalize_suricata_loki_response({"data": {"result": results}})
    alerts = [
        row
        for row in rows
        if row["signature"] == signature_group["signature"]
        and (not signature_ids or row["signature_id"] in signature_ids)
    ]
    rehydrated = dict(signature_group)
    rehydrated["alerts"] = alerts
    rehydrated["example_alerts"] = alerts[:5]
    return rehydrated


def create_triage_client(vault: VaultConnections) -> JevClient:
    """Create the independently configured Jev routing client from Vault."""
    connection = vault.get("cyber_analyst_triage")
    extra = connection.extra
    endpoint = str(extra.get("endpoint") or extra.get("base_url") or connection.host)
    api_key = str(
        connection.password or extra.get("api_key") or extra.get("token") or ""
    )
    if not endpoint or not api_key:
        raise ValueError("cyber_analyst_triage requires endpoint and API key/token")
    return JevClient(
        endpoint=endpoint,
        api_key=api_key,
        model=str(extra.get("model", "jev-latest")),
        timeout=float(extra.get("timeout", 30)),
    )


def create_reasoning_llm(vault: VaultConnections) -> object:
    """Create the strong reasoning model from its separate Vault connection."""
    from langchain_openai import ChatOpenAI

    connection = vault.get("cyber_analyst_openrouter")
    return ChatOpenAI(
        model=connection.extra.get("model", "deepseek/deepseek-v4.1-flash"),
        api_key=connection.password,
        base_url=connection.host,
        temperature=0,
    )


def collect_signatures(
    vault: VaultConnections,
    logical_date=None,
    prev_data_interval_end_success=None,
    params: dict[str, object] | None = None,
) -> dict[str, object]:
    """Collect and compact monthly Suricata alerts by signature."""
    params = params or {}
    test_mode = bool(params["test_mode"])

    def parse_datetime(value: object) -> datetime:
        if isinstance(value, datetime):
            parsed = value
        elif value not in (None, ""):
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        else:
            raise ValueError("Datetime value is required")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    manual_start = params.get("window_start")
    manual_end = params.get("window_end")
    if manual_start is None and manual_end is None:
        if prev_data_interval_end_success is None:
            raise ValueError(
                "prev_data_interval_end_success is required for scheduled continuity runs; "
                "provide window_start and window_end for the first run"
            )
        window_start = parse_datetime(prev_data_interval_end_success)
        window_end = parse_datetime(logical_date)
    elif manual_start is None or manual_end is None:
        raise ValueError("window_start and window_end must be provided together")
    else:
        window_start = parse_datetime(manual_start)
        window_end = parse_datetime(manual_end)

    logger = logging.getLogger(__name__)
    if test_mode:
        payload = query_loki_range(
            LOKI_CONN_ID,
            query='{job="suricata"} | json | event_type="alert"',
            start=window_start,
            end=window_end,
            limit=_collection_limit(test_mode),
        )
        loki_result = payload.get("data", {}).get("result", [])
    else:
        loki_result = query_loki_range_adaptive(
            LOKI_CONN_ID,
            query='{job="suricata"} | json | event_type="alert"',
            start=window_start,
            end=window_end,
            limit=_collection_limit(test_mode),
            logger=logger,
        )
    rows = normalize_suricata_loki_response({"data": {"result": loki_result}})
    groups = compact_signature_groups(group_alerts_by_signature(rows))
    logger.info("Collected %s alerts across %s signatures", len(rows), len(groups))
    return {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "signatures": groups,
        "test_mode": test_mode,
    }


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate a stable percentage for routing counters."""
    return round(100 * numerator / denominator, 1) if denominator else 0.0


def analyze_signatures(
    vault: VaultConnections, collected: dict[str, object]
) -> dict[str, object]:
    """Run signature Jev triage, alert Jev triage, and final analysis."""
    from langgraph.graph import END, START, StateGraph

    set_vault(vault)
    _configure_tracing()
    logger = logging.getLogger(__name__)
    triage_client = create_triage_client(vault)
    reasoning_llm: object | None = None

    def get_reasoning_llm() -> object:
        nonlocal reasoning_llm
        if reasoning_llm is None:
            reasoning_llm = create_reasoning_llm(vault)
        return reasoning_llm

    def get_connection_payload(
        connection_id: str,
    ) -> tuple[object, dict[str, object]]:
        connection = vault.get(connection_id)
        return connection, connection.extra

    def threat_intel_checker(ip: str | None) -> dict[str, object]:
        return check_threat_intel(ip, get_connection=get_connection_payload)

    def cve_searcher(signature: str) -> dict[str, object]:
        return search_cve(signature, get_connection=get_connection_payload)

    try:
        from mac_vendor_lookup import MacLookup

        mac_lookup = MacLookup()
    except Exception:
        mac_lookup = None

    def vendor_lookup(mac_address: str) -> str | None:
        if mac_lookup is None:
            return None
        try:
            return mac_lookup.lookup(mac_address)
        except Exception:
            return None

    def signature_triage_node(state: SignatureAgentState) -> SignatureAgentState:
        result = triage_signature(state["signature_group"], triage_client=triage_client)
        return {
            "signature_triage": result.model_dump(),
            "per_alert_results": [],
            "unresolved_lookups": [],
            "alerts_rehydrated": 0,
            "alerts_skipped_by_jev": 0,
            "alerts_sent_to_external": 0,
            "reasoning_calls": 0,
        }

    def branch_after_signature_triage(state: SignatureAgentState) -> str:
        return (
            "done"
            if state["signature_triage"]["decision"] == "ignore_background_noise"
            else "rehydrate"
        )

    def rehydrate_node(state: SignatureAgentState) -> SignatureAgentState:
        group = rehydrate_signature_group(state["signature_group"], logger=logger)
        return {"rehydrated_group": group, "alerts_rehydrated": len(group["alerts"])}

    def analyze_alerts_node(state: SignatureAgentState) -> SignatureAgentState:
        group = state["rehydrated_group"]
        findings: list[dict[str, Any]] = []
        unresolved: list[str] = []
        skipped = 0
        sent_external = 0
        reasoning_calls = 0
        for alert in group.get("alerts", []):
            local_context = enrich_alert_locally(alert, vendor_lookup=vendor_lookup)
            alert_triage = triage_alert(
                alert,
                local_context,
                triage_client=triage_client,
            )
            logger.info(
                "Alert Jev decision signature=%s signature_id=%s src_ip=%s dest_ip=%s "
                "decision=%s security_relevance_probability=%.3f confidence=%.3f",
                alert.get("signature"),
                alert.get("signature_id"),
                alert.get("src_ip"),
                alert.get("dest_ip"),
                alert_triage.decision,
                alert_triage.security_relevance_probability,
                alert_triage.confidence,
            )
            if alert_triage.decision == "skip":
                skipped += 1
                continue
            external_context = enrich_alert_externally(
                alert,
                threat_intel_checker=threat_intel_checker,
                cve_searcher=cve_searcher,
            )
            sent_external += 1
            finding_model = analyze_enriched_alert(
                alert,
                local_context,
                external_context,
                reasoning_llm=get_reasoning_llm(),
            )
            reasoning_calls += 1
            finding = (
                finding_model.model_dump()
                if isinstance(finding_model, BaseModel)
                else dict(finding_model)
            )
            findings.append(finding)
            for warning in finding.get("lookup_warnings", []):
                if warning not in unresolved:
                    unresolved.append(warning)
        return {
            "per_alert_results": findings,
            "unresolved_lookups": unresolved,
            "alerts_skipped_by_jev": skipped,
            "alerts_sent_to_external": sent_external,
            "reasoning_calls": reasoning_calls,
        }

    graph = StateGraph(SignatureAgentState)
    graph.add_node("signature_triage", signature_triage_node)
    graph.add_node("rehydrate", rehydrate_node)
    graph.add_node("analyze_alerts", analyze_alerts_node)
    graph.add_edge(START, "signature_triage")
    graph.add_conditional_edges(
        "signature_triage",
        branch_after_signature_triage,
        {"done": END, "rehydrate": "rehydrate"},
    )
    graph.add_edge("rehydrate", "analyze_alerts")
    graph.add_edge("analyze_alerts", END)
    signature_graph = graph.compile()

    signature_groups = list(collected.get("signatures", []))
    analyses: list[dict[str, object]] = []
    for signature_group in signature_groups:
        with _tracing_span("cyber_analyst.signature_triage") as span:
            if hasattr(span, "set_attribute"):
                span.set_attribute("suricata.signature", signature_group["signature"])
                span.set_attribute(
                    "suricata.alert_count", signature_group["alert_count"]
                )
            analysis = signature_graph.invoke({"signature_group": signature_group})
        triage = analysis["signature_triage"]
        logger.info(
            "Signature Jev decision signature=%s alert_count=%s decision=%s "
            "security_relevance_probability=%.3f confidence=%.3f",
            triage["signature"],
            triage["alert_count"],
            triage["decision"],
            triage["security_relevance_probability"],
            triage["confidence"],
        )
        analysis["alerts_selected_for_investigation"] = analysis.get(
            "alerts_sent_to_external", 0
        )
        analyses.append(analysis)

    total_signatures = len(signature_groups)
    signatures_skipped = sum(
        analysis["signature_triage"]["decision"] == "ignore_background_noise"
        for analysis in analyses
    )
    signatures_rehydrated = total_signatures - signatures_skipped
    total_alerts = sum(int(group["alert_count"]) for group in signature_groups)
    alerts_skipped = sum(
        int(analysis.get("alerts_skipped_by_jev", 0)) for analysis in analyses
    )
    alerts_external = sum(
        int(analysis.get("alerts_sent_to_external", 0)) for analysis in analyses
    )
    reasoning_calls = sum(
        int(analysis.get("reasoning_calls", 0)) for analysis in analyses
    )
    counters = {
        "total_signatures": total_signatures,
        "signatures_skipped_by_jev": signatures_skipped,
        "signatures_rehydrated": signatures_rehydrated,
        "total_alerts_in_rehydrated_signatures": sum(
            int(analysis.get("alerts_rehydrated", 0)) for analysis in analyses
        ),
        "alerts_skipped_by_jev": alerts_skipped,
        "alerts_sent_to_external": alerts_external,
        "alerts_sent_to_reasoning_model": reasoning_calls,
        "signature_reduction_percent": _percentage(
            signatures_skipped, total_signatures
        ),
        "alert_reduction_percent": _percentage(alerts_skipped, total_alerts),
        "reasoning_call_reduction_percent": _percentage(
            total_alerts - reasoning_calls, total_alerts
        ),
    }
    logger.info("Cyber analyst routing counters: %s", counters)
    return {
        "window_start": collected["window_start"],
        "window_end": collected["window_end"],
        "analyses": analyses,
        "routing_counters": counters,
        "test_mode": collected.get("test_mode", False),
    }


class _NoopSpan:
    """Small tracing fallback used when OpenTelemetry is unavailable."""

    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def _configure_tracing() -> None:
    """Register Phoenix tracing when the runner provides an endpoint."""
    endpoint = os.getenv("PHOENIX_COLLECTOR_ENDPOINT")
    if not endpoint:
        return
    from phoenix.otel import register

    register(
        endpoint=endpoint,
        project_name=os.getenv("PHOENIX_PROJECT_NAME", "cyber_analyst"),
        auto_instrument=True,
    )


def _tracing_span(name: str) -> Any:
    """Return an OpenTelemetry span when tracing is configured."""
    try:
        from opentelemetry import trace

        return trace.get_tracer(__name__).start_as_current_span(name)
    except ImportError:
        return _NoopSpan()


def render_email_report(
    analyzed: dict[str, object],
    run_id: str | None = None,
) -> dict[str, str]:
    """Render the analyzed workflow result as the monthly email report."""
    window_start = datetime.fromisoformat(str(analyzed["window_start"])).astimezone(UTC)
    window_end = datetime.fromisoformat(str(analyzed["window_end"])).astimezone(UTC)
    body = render_plaintext_report(
        run_id=run_id,
        window_start=window_start,
        window_end=window_end,
        analyses=list(analyzed["analyses"]),
    )
    subject = f"Monthly Suricata AI Triage Report: {window_start.strftime('%Y-%m')}"
    if analyzed.get("test_mode"):
        subject = f"[TEST MODE] {subject}"
    return {"subject": subject, "body": body}


STATE_SQL = """
CREATE TABLE IF NOT EXISTS automation_run_state (
    pipeline TEXT PRIMARY KEY,
    last_successful_boundary TIMESTAMPTZ NOT NULL
)
"""


def run(vault: VaultConnections) -> None:
    """Run collection, analysis, email delivery, and watermark advancement."""
    set_vault(vault)
    boundary = datetime.now(UTC)
    with postgres_connect(vault.get("data")) as database:
        with database.cursor() as cursor:
            cursor.execute(STATE_SQL)
            cursor.execute(
                "SELECT last_successful_boundary FROM automation_run_state WHERE pipeline=%s",
                ("cyber_analyst",),
            )
            row = cursor.fetchone()
        database.commit()
    if row is None:
        raise RuntimeError(
            "cyber_analyst watermark is not seeded; seed it from the final successful Airflow run"
        )
    collected = collect_signatures(
        vault,
        logical_date=boundary,
        prev_data_interval_end_success=row[0],
        params={"test_mode": False},
    )
    analyzed = analyze_signatures(vault, collected)
    report = render_email_report(analyzed, run_id=f"systemd__{boundary.isoformat()}")
    send_email(
        vault.get("smtp_default"),
        sender=ALERT_FROM,
        recipient=ALERT_TO,
        subject=report["subject"],
        body=report["body"],
    )
    with postgres_connect(vault.get("data")) as database:
        with database.cursor() as cursor:
            cursor.execute(
                "UPDATE automation_run_state SET last_successful_boundary=%s WHERE pipeline=%s",
                (boundary, "cyber_analyst"),
            )
        database.commit()
