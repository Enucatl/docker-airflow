from __future__ import annotations

from datetime import UTC, datetime
import os

from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.sdk import DAG, Param, task

from common.defaults import ALERT_FROM, ALERT_TO, SMTP_CONN_ID, default_args
from common.loki import query_loki_range
from common.suricata_monthly_triage import (
    group_alerts_by_signature,
    normalize_suricata_loki_response,
    render_plaintext_report,
)


LOKI_CONN_ID = "loki"
OPENAI_CONN_ID = "openai_compatible"
GREYNOISE_CONN_ID = "greynoise"
ABUSEIPDB_CONN_ID = "abuseipdb"
TAVILY_CONN_ID = "tavily"

VIRTUALENV_REQUIREMENTS = [
    "apache-airflow",
    "arize-phoenix-otel",
    "langgraph",
    "langchain-openai",
    "mac-vendor-lookup",
    "pydantic",
    "openinference-instrumentation-langchain",
    "requests",
]


def _rate_limit_warning(provider: str, retry_after: str | None = None) -> str:
    warning = f"{provider} lookup temporarily unavailable because the API rate limit was reached"
    if retry_after:
        return f"{warning}; retry after {retry_after}."
    return f"{warning}."


def _collection_limit(test_mode: bool) -> int:
    return 1 if test_mode else 5000


with DAG(
    "cyber_analyst",
    default_args=default_args,
    description="Monthly Suricata alert triage and suppression reporting",
    schedule="0 3 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    max_active_runs=1,
    params={
        "test_mode": Param(
            default=False,
            type="boolean",
            description=(
                "If true, collect only one alert from Loki for an end-to-end smoke test."
            ),
        ),
        "window_start": Param(
            default=None,
            type=["null", "string"],
            format="date-time",
            description=(
                "Optional manual start boundary for ad hoc runs. Leave empty for scheduled runs."
            ),
        ),
        "window_end": Param(
            default=None,
            type=["null", "string"],
            format="date-time",
            description=(
                "Optional manual end boundary for ad hoc runs. Leave empty for scheduled runs."
            ),
        ),
    },
) as dag:

    @task.virtualenv(
        task_id="collect_signatures",
        requirements=[
            "apache-airflow",
            "requests",
        ],
    )
    def collect_signatures(
        logical_date=None,
        prev_data_interval_end_success=None,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        import logging
        import sys
        from datetime import UTC, datetime

        dags_root = "/opt/airflow/dags"
        if dags_root not in sys.path:
            sys.path.insert(0, dags_root)

        from common.loki import query_loki_range, query_loki_range_adaptive
        from common.suricata_monthly_triage import (
            group_alerts_by_signature,
            normalize_suricata_loki_response,
        )

        def collection_limit(test_mode: bool) -> int:
            return 1 if test_mode else 5000

        logger = logging.getLogger(__name__)
        if params is None:
            params = {}
        test_mode = params["test_mode"]
        logger.info(
            "Incoming params payload: %r; types=%s",
            params,
            {key: type(value).__name__ for key, value in params.items()},
        )

        def parse_datetime(value: object) -> datetime:
            if isinstance(value, datetime):
                return value.astimezone(UTC)
            if value in (None, ""):
                raise ValueError("Datetime value is required")
            text = str(value)
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)

        manual_start = params.get("window_start")
        manual_end = params.get("window_end")
        if manual_start is None and manual_end is None:
            if prev_data_interval_end_success is None:
                raise ValueError(
                    "prev_data_interval_end_success is required for scheduled "
                    "continuity runs; provide window_start and window_end for "
                    "the first run"
                )
            window_start = parse_datetime(prev_data_interval_end_success)
            window_end = parse_datetime(logical_date)
            window_source = "prev_data_interval_end_success_to_logical_date"
        elif manual_start is None or manual_end is None:
            raise ValueError(
                "window_start and window_end must be provided together when overriding the window"
            )
        else:
            window_start = parse_datetime(manual_start)
            window_end = parse_datetime(manual_end)
            window_source = "manual_params"
        logger.info(
            "Collecting Suricata alerts for %s through %s from %s; test_mode=%s",
            window_start.isoformat(),
            window_end.isoformat(),
            window_source,
            test_mode,
        )
        if test_mode:
            logger.info(
                "test_mode=True => one Loki query total; stopping after the first returned entry"
            )
            loki_payload = query_loki_range(
                "loki",
                query='{job="suricata"} | json | event_type="alert"',
                start=window_start,
                end=window_end,
                limit=collection_limit(test_mode),
            )
            loki_result = loki_payload.get("data", {}).get("result", [])
        else:
            loki_result = query_loki_range_adaptive(
                "loki",
                query='{job="suricata"} | json | event_type="alert"',
                start=window_start,
                end=window_end,
                limit=collection_limit(test_mode),
                logger=logger,
            )
        logger.info(
            "Loki returned %s streams and %s alerts for the full window",
            len(loki_result),
            sum(len(result.get("values", [])) for result in loki_result),
        )
        loki_payload = {"data": {"result": loki_result}}
        normalized_rows = normalize_suricata_loki_response(loki_payload)
        signature_groups = group_alerts_by_signature(normalized_rows)
        signature_summary = ", ".join(
            f"{group['signature']} ({group['alert_count']})"
            for group in signature_groups
        )
        logger.info(
            "Collected %s alerts across %s signatures: %s",
            len(normalized_rows),
            len(signature_groups),
            signature_summary or "none",
        )
        return {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "signatures": signature_groups,
            "test_mode": test_mode,
        }

    @task.virtualenv(
        task_id="analyze_signatures",
        requirements=VIRTUALENV_REQUIREMENTS,
    )
    def analyze_signatures(collected: dict[str, object]) -> dict[str, object]:
        from datetime import UTC, datetime, timedelta
        import logging
        import json
        from ipaddress import ip_address
        import sys
        from typing import Any, TypedDict

        from airflow.sdk.bases.hook import BaseHook
        from langchain_openai import ChatOpenAI
        from langgraph.graph import END, START, StateGraph
        from mac_vendor_lookup import MacLookup
        from pydantic import BaseModel, Field
        import requests
        from common.loki import query_loki_range

        LOKI_CONN_ID = "loki"

        def rate_limit_warning(provider: str, retry_after: str | None = None) -> str:
            warning = f"{provider} lookup temporarily unavailable because the API rate limit was reached"
            if retry_after:
                return f"{warning}; retry after {retry_after}."
            return f"{warning}."

        dags_root = "/opt/airflow/dags"
        if dags_root not in sys.path:
            sys.path.insert(0, dags_root)

        phoenix_collector_endpoint = os.getenv("PHOENIX_COLLECTOR_ENDPOINT")
        if phoenix_collector_endpoint:
            from phoenix.otel import register

            register(
                endpoint=phoenix_collector_endpoint,
                project_name=os.getenv("PHOENIX_PROJECT_NAME", "cyber_analyst"),
                auto_instrument=True,
            )
        from opentelemetry import trace

        tracer = trace.get_tracer(__name__)
        logger = logging.getLogger(__name__)
        test_mode = bool(collected["test_mode"])

        from common.suricata_monthly_triage import (
            build_signature_summary_for_llm,
            build_suppression_candidates,
            extract_ipv4_identity,
            extract_ipv6_identity,
            infer_identity_lookup_version,
            run_signature_agent,
        )

        class SuppressionCandidate(BaseModel):
            signature_id: int
            rule_line: str
            scope_reason: str

        class SignatureDecision(BaseModel):
            signature: str
            alert_count: int
            decision: str = Field(
                pattern="^(ignore_background_noise|analyze_individually)$"
            )
            reasoning_summary: str
            confidence: str
            representative_categories: list[str]
            suppression_candidates: list[SuppressionCandidate] = Field(
                default_factory=list
            )

        class AlertFinding(BaseModel):
            signature: str
            signature_id: int
            category: str
            src_ip: str | None
            dest_ip: str | None
            src_port: int | None
            dest_port: int | None
            protocol: str | None
            observed_at: str
            asset_identity: str
            verdict: str = Field(
                pattern="^(proof_of_malware|suspicious_monitor|false_positive_monitor)$"
            )
            evidence: str
            recommended_follow_up: str

        class SignatureAgentState(TypedDict, total=False):
            signature_group: dict[str, Any]
            signature_result: dict[str, Any]
            per_alert_results: list[dict[str, Any]]
            unresolved_lookups: list[str]

        globals()["SignatureAgentState"] = SignatureAgentState
        globals()["Any"] = Any

        def get_connection_payload(conn_id: str) -> tuple[object, dict[str, object]]:
            conn = BaseHook.get_connection(conn_id)
            return conn, conn.extra_dejson

        def parse_json_lines(payload: dict[str, object]) -> list[dict[str, object]]:
            rows: list[dict[str, object]] = []
            for result in payload.get("data", {}).get("result", []):
                for _timestamp_ns, raw_line in result.get("values", []):
                    rows.append(json.loads(raw_line))
            return rows

        def query_suricata_context(alert_row: dict[str, object]) -> dict[str, object]:
            observed_at = datetime.fromisoformat(alert_row["observed_at"])
            start = observed_at - timedelta(minutes=15)
            end = observed_at + timedelta(minutes=15)
            query = (
                '{job="suricata"} | json '
                f'| src_ip="{alert_row["src_ip"]}" '
                f'| dest_ip="{alert_row["dest_ip"]}" '
                f'| alert_signature_id="{alert_row["signature_id"]}"'
            )
            logger.info(
                "Querying Loki suricata context for signature=%s src_ip=%s dest_ip=%s "
                "signature_id=%s window=%s..%s query=%s",
                alert_row.get("signature"),
                alert_row.get("src_ip"),
                alert_row.get("dest_ip"),
                alert_row.get("signature_id"),
                start.isoformat(),
                end.isoformat(),
                query,
            )
            payload = query_loki_range(
                LOKI_CONN_ID, query=query, start=start, end=end, limit=200
            )
            results = payload.get("data", {}).get("result", [])
            logger.info(
                "Loki suricata context returned %s streams for signature=%s",
                len(results),
                alert_row.get("signature"),
            )
            return payload

        mac_lookup = MacLookup()

        def check_mac_vendor(mac_address: str | None) -> str | None:
            if not mac_address:
                return None
            try:
                return mac_lookup.lookup(mac_address)
            except Exception:
                return None

        def resolve_network_identity(
            ip: str | None, observed_at: str
        ) -> dict[str, object]:
            if not ip:
                return {
                    "ip": None,
                    "mac_address": None,
                    "hostname": None,
                    "mac_vendor": None,
                    "warning": "No IP available for identity lookup",
                }
            lookup_version = infer_identity_lookup_version(ip)
            observation_time = datetime.fromisoformat(observed_at)
            start = observation_time - timedelta(hours=12)
            end = observation_time + timedelta(hours=1)
            dhcp_rows = parse_json_lines(
                query_loki_range(
                    LOKI_CONN_ID,
                    query='{job="kea-dhcp4"} | json',
                    start=start,
                    end=end,
                    limit=500,
                )
            )
            if lookup_version == "ipv4":
                return extract_ipv4_identity(ip, dhcp_rows, check_mac_vendor)

            ndp_rows = parse_json_lines(
                query_loki_range(
                    LOKI_CONN_ID,
                    query='{job="ndp"} | json',
                    start=start,
                    end=end,
                    limit=500,
                )
            )
            return extract_ipv6_identity(ip, ndp_rows, dhcp_rows, check_mac_vendor)

        def check_threat_intel(ip: str | None) -> dict[str, object]:
            if not ip:
                return {"warning": "No IP available for threat intel lookup"}
            try:
                if ip_address(ip).is_private:
                    return {"note": "Private IP skipped for threat intel"}
            except ValueError:
                return {"warning": f"Invalid IP for threat intel lookup: {ip}"}

            findings: dict[str, object] = {}

            greynoise_conn, greynoise_extra = get_connection_payload("greynoise")
            greynoise_headers = {"Accept": "application/json"}
            if api_key := greynoise_conn.password or greynoise_extra.get("api_key"):
                greynoise_headers["key"] = api_key
            try:
                response = requests.get(
                    f"{greynoise_conn.host.rstrip('/')}/v3/community/{ip}",
                    headers=greynoise_headers,
                    timeout=30,
                )
                response.raise_for_status()
                findings["greynoise"] = response.json()
            except requests.HTTPError as exc:
                response = exc.response
                if response is not None and response.status_code == 429:
                    findings["greynoise_warning"] = rate_limit_warning(
                        "GreyNoise",
                        response.headers.get("Retry-After"),
                    )
                else:
                    findings["greynoise_warning"] = (
                        f"GreyNoise lookup failed for {ip}: {exc}"
                    )
            except Exception as exc:
                findings["greynoise_warning"] = (
                    f"GreyNoise lookup failed for {ip}: {exc}"
                )

            abuse_conn, abuse_extra = get_connection_payload("abuseipdb")
            abuse_headers = {"Accept": "application/json"}
            if api_key := abuse_conn.password or abuse_extra.get("api_key"):
                abuse_headers["Key"] = api_key
            try:
                response = requests.get(
                    f"{abuse_conn.host.rstrip('/')}/api/v2/check",
                    params={"ipAddress": ip, "maxAgeInDays": 90},
                    headers=abuse_headers,
                    timeout=30,
                )
                response.raise_for_status()
                findings["abuseipdb"] = response.json()
            except requests.HTTPError as exc:
                response = exc.response
                if response is not None and response.status_code == 429:
                    findings["abuseipdb_warning"] = rate_limit_warning(
                        "AbuseIPDB",
                        response.headers.get("Retry-After"),
                    )
                else:
                    findings["abuseipdb_warning"] = (
                        f"AbuseIPDB lookup failed for {ip}: {exc}"
                    )
            except Exception as exc:
                findings["abuseipdb_warning"] = (
                    f"AbuseIPDB lookup failed for {ip}: {exc}"
                )

            return findings

        def search_cve(signature: str) -> dict[str, object]:
            tavily_conn, tavily_extra = get_connection_payload("tavily")
            api_key = tavily_conn.password or tavily_extra.get("api_key")
            try:
                response = requests.post(
                    f"{tavily_conn.host.rstrip('/')}/search",
                    json={
                        "api_key": api_key,
                        "query": f"{signature} CVE",
                        "search_depth": "advanced",
                        "max_results": 5,
                    },
                    timeout=30,
                )
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                response = exc.response
                if response is not None and response.status_code == 429:
                    return {
                        "warning": rate_limit_warning(
                            "Tavily", response.headers.get("Retry-After")
                        )
                    }
                return {"warning": f"Missing CVE context for {signature}: {exc}"}
            except Exception as exc:
                return {"warning": f"Missing CVE context for {signature}: {exc}"}

        openai_conn, openai_extra = get_connection_payload("openai_compatible")
        model_name = openai_extra.get("model", "gpt-4.1")
        llm = ChatOpenAI(
            model=model_name,
            api_key=openai_conn.password,
            base_url=openai_conn.host,
            temperature=0,
        )

        def decide_signature(signature_group: dict[str, object]) -> dict[str, object]:
            signature_summary = build_signature_summary_for_llm(signature_group)
            prompt = (
                "You are triaging Suricata alert signatures for a monthly report. "
                "Choose ignore_background_noise only for routine background noise that should "
                "not produce per-alert verdicts. Otherwise choose analyze_individually.\n\n"
                f"Signature summary:\n{json.dumps(signature_summary, default=str)}"
            )
            result = llm.with_structured_output(SignatureDecision).invoke(prompt)
            return result.model_dump()

        def analyze_one_alert(
            alert_row: dict[str, object],
            _signature_result: dict[str, object],
        ) -> dict[str, object]:
            src_identity = resolve_network_identity(
                alert_row.get("src_ip"), alert_row["observed_at"]
            )
            dest_identity = resolve_network_identity(
                alert_row.get("dest_ip"),
                alert_row["observed_at"],
            )
            threat_intel = check_threat_intel(alert_row.get("src_ip"))
            cve_context = search_cve(alert_row["signature"])
            suricata_context = query_suricata_context(alert_row)

            lookup_warnings = [
                value
                for value in [
                    src_identity.get("warning"),
                    dest_identity.get("warning"),
                    threat_intel.get("greynoise_warning"),
                    threat_intel.get("abuseipdb_warning"),
                    cve_context.get("warning"),
                ]
                if value
            ]

            prompt = (
                "You are analyzing one Suricata alert. Produce a concise finding for a monthly report.\n\n"
                f"Alert:\n{json.dumps(alert_row, default=str)}\n\n"
                f"Source identity:\n{json.dumps(src_identity, default=str)}\n\n"
                f"Destination identity:\n{json.dumps(dest_identity, default=str)}\n\n"
                f"Threat intel:\n{json.dumps(threat_intel, default=str)}\n\n"
                f"CVE context:\n{json.dumps(cve_context, default=str)}\n\n"
                f"Suricata context:\n{json.dumps(suricata_context, default=str)}"
            )
            finding = (
                llm.with_structured_output(AlertFinding).invoke(prompt).model_dump()
            )
            source_identity_parts = [
                part
                for part in [
                    src_identity.get("hostname"),
                    src_identity.get("mac_address"),
                    src_identity.get("mac_vendor"),
                ]
                if part
            ]
            if not source_identity_parts:
                source_identity_parts = [
                    part
                    for part in [
                        dest_identity.get("hostname"),
                        dest_identity.get("mac_address"),
                        dest_identity.get("mac_vendor"),
                    ]
                    if part
                ]
            finding["asset_identity"] = (
                " / ".join(source_identity_parts) or "unresolved"
            )
            finding["lookup_warnings"] = lookup_warnings
            return finding

        def triage_node(state: SignatureAgentState) -> SignatureAgentState:
            signature_group = state["signature_group"]
            signature_result = decide_signature(signature_group)
            if signature_result["decision"] == "ignore_background_noise":
                signature_result["suppression_candidates"] = (
                    build_suppression_candidates(signature_group)
                )
            return {
                "signature_result": signature_result,
                "per_alert_results": [],
                "unresolved_lookups": [],
            }

        def branch_after_triage(state: SignatureAgentState) -> str:
            if state["signature_result"]["decision"] == "ignore_background_noise":
                return "done"
            return "analyze"

        def analyze_node(state: SignatureAgentState) -> SignatureAgentState:
            result = run_signature_agent(
                state["signature_group"],
                decide_signature=lambda _group: state["signature_result"],
                analyze_alert=analyze_one_alert,
            )
            return {
                "signature_result": result["signature_result"],
                "per_alert_results": result["per_alert_results"],
                "unresolved_lookups": result["unresolved_lookups"],
            }

        graph = StateGraph(SignatureAgentState)
        graph.add_node("triage", triage_node)
        graph.add_node("analyze", analyze_node)
        graph.add_edge(START, "triage")
        graph.add_conditional_edges(
            "triage",
            branch_after_triage,
            {
                "done": END,
                "analyze": "analyze",
            },
        )
        graph.add_edge("analyze", END)
        signature_agent = graph.compile()

        analyses: list[dict[str, object]] = []
        signature_groups = list(collected["signatures"])
        logger.info(
            "Analyzing %s signatures: %s; test_mode=%s",
            len(signature_groups),
            ", ".join(
                f"{group['signature']} ({group['alert_count']})"
                for group in signature_groups
            )
            or "none",
            test_mode,
        )
        with tracer.start_as_current_span("cyber_analyst.analyze_signatures"):
            for signature_group in signature_groups:
                with tracer.start_as_current_span(
                    "cyber_analyst.signature_agent"
                ) as span:
                    span.set_attribute(
                        "suricata.signature", signature_group["signature"]
                    )
                    span.set_attribute(
                        "suricata.alert_count", signature_group["alert_count"]
                    )
                    logger.info(
                        "Processing signature %s with %s alerts",
                        signature_group["signature"],
                        signature_group["alert_count"],
                    )
                    analysis = signature_agent.invoke(
                        {"signature_group": signature_group}
                    )
                    logger.info(
                        "Finished signature %s with decision=%s and %s per-alert results",
                        analysis["signature_result"]["signature"],
                        analysis["signature_result"]["decision"],
                        len(analysis["per_alert_results"]),
                    )
                    analyses.append(analysis)

        analyzed_categories = ", ".join(
            f"{analysis['signature_result']['signature']} ({analysis['signature_result']['alert_count']})"
            for analysis in analyses
        )
        logger.info(
            "Completed signature analysis for %s signatures: %s",
            len(analyses),
            analyzed_categories or "none",
        )
        return {
            "window_start": collected["window_start"],
            "window_end": collected["window_end"],
            "analyses": analyses,
            "test_mode": collected["test_mode"],
        }

    @task(task_id="render_email_report")
    def render_email_report(
        analyzed: dict[str, object],
        run_id: str | None = None,
    ) -> dict[str, str]:
        import logging

        logger = logging.getLogger(__name__)
        window_start = datetime.fromisoformat(str(analyzed["window_start"])).astimezone(
            UTC
        )
        window_end = datetime.fromisoformat(str(analyzed["window_end"])).astimezone(UTC)
        analyses = list(analyzed["analyses"])
        test_mode = bool(analyzed.get("test_mode"))
        logger.info(
            "Rendering email report for %s analyses across signatures: %s; test_mode=%s",
            len(analyses),
            ", ".join(
                f"{analysis['signature_result']['signature']} "
                f"({analysis['signature_result']['decision']})"
                for analysis in analyses
            )
            or "none",
            test_mode,
        )
        body = render_plaintext_report(
            run_id=run_id,
            window_start=window_start,
            window_end=window_end,
            analyses=analyses,
        )
        subject = f"Monthly Suricata AI Triage Report: {window_start.strftime('%Y-%m')}"
        if test_mode:
            subject = f"[TEST MODE] {subject}"
        logger.info("Prepared email report subject: %s", subject)
        return {"subject": subject, "body": body}

    @task(task_id="send_report")
    def send_report(report: dict[str, str]) -> None:
        import logging

        logger = logging.getLogger(__name__)
        logger.info(
            "Sending report email; test_mode=%s",
            report["subject"].startswith("[TEST MODE]"),
        )
        logger.info("Sending report email with subject: %s", report["subject"])
        with SmtpHook(smtp_conn_id=SMTP_CONN_ID) as hook:
            hook.send_email_smtp(
                to=ALERT_TO,
                from_email=ALERT_FROM,
                subject=report["subject"],
                html_content=f"<pre>{report['body']}</pre>",
            )

    send_report(render_email_report(analyze_signatures(collect_signatures())))
