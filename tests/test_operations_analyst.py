from datetime import UTC, datetime, timedelta
import json
import subprocess

import httpx
import pytest
from langchain_core.messages import AIMessage
import langchain_openai
import openai

from operations_analyst import (
    CONTROL_PLANE_GUIDANCE,
    Evidence,
    Finding,
    RepositoryCorpus,
    analyze_findings,
    codex_prompt,
    fingerprint,
    redact_web_query,
    render_email_report,
    render_report,
    weekly_slices,
)


def _connection(extra=None):
    class Connection:
        host = "https://openrouter.ai/api/v1"
        password = "test-key"

    Connection.extra = {"model": "test-model", **(extra or {})}

    return Connection()


class _Vault:
    def __init__(self, connection=None):
        self.connection = connection or _connection()

    def get(self, _name):
        return self.connection


def _install_fake_chat(monkeypatch, outcomes, captured=None):
    class Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.outcomes = outcomes
            if captured is not None:
                captured.append(self)

        def with_structured_output(self, schema, **kwargs):
            self.structured_schema = schema
            self.structured_kwargs = kwargs
            return self

        def invoke(self, prompt):
            self.prompt = prompt
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", Client)
    return Client


def test_fingerprint_masks_dynamic_values_without_mutating_evidence():
    line = "\x1b[31m2026-08-28T12:00:00Z failed 10.2.3.4 status=503 id=123e4567-e89b-12d3-a456-426614174000\x1b[0m"
    evidence = Evidence("now", line)
    first, template = fingerprint(line)
    second, _ = fingerprint(line.replace("10.2.3.4", "10.9.8.7"))

    assert first == second
    assert "status=503" in template
    assert "\x1b" not in template
    assert evidence.line == line
    assert "10.2.3.4" not in redact_web_query(line)


def test_weekly_slices_are_bounded():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    result = weekly_slices(start, start + timedelta(days=16))
    assert [end - begin for begin, end in result] == [
        timedelta(days=7),
        timedelta(days=7),
        timedelta(days=2),
    ]


def test_corpus_allows_any_file_but_confines_paths(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    subprocess.run(["git", "init", "-b", "main", source], check=True)
    subprocess.run(
        ["git", "-C", source, "config", "user.email", "test@example.com"], check=True
    )
    subprocess.run(["git", "-C", source, "config", "user.name", "Test"], check=True)
    (source / "uv.lock").write_text("public generated material")
    subprocess.run(["git", "-C", source, "add", "."], check=True)
    subprocess.run(["git", "-C", source, "commit", "-m", "Initial"], check=True)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(f'{{"repo": "file://{source}"}}')
    corpus = RepositoryCorpus(tmp_path / "corpus", manifest)

    assert corpus.sync() == []
    assert corpus.read_file("repo", "uv.lock")["content"] == "public generated material"
    assert corpus.search("repo", "generated")
    with pytest.raises(ValueError, match="escapes"):
        corpus.read_file("repo", "../manifest.json")


def test_only_actionable_failures_receive_prompts():
    actionable = Finding(
        "a",
        "host",
        "svc",
        "docker",
        "error",
        "boom",
        3,
        classification="actionable_failure",
        analysis="bad config",
        evidence=[Evidence("now", "original")],
    )
    noise = Finding(
        "b",
        "host",
        "svc2",
        "docker",
        "error",
        "noise",
        2,
        classification="expected_noise",
    )
    start = datetime(2026, 1, 1, tzinfo=UTC)
    body = render_report([actionable, noise], start, start + timedelta(days=7), [])
    rendered = render_email_report(
        [actionable, noise], start, start + timedelta(days=7), []
    )

    assert "Do not create a commit." not in body
    assert rendered.attachments == {}
    assert "Priority findings:" in rendered.plain_text
    assert "Weekly Operations Report" in rendered.html
    assert (
        "Representative log lines and surrounding context are retained in Loki"
        in codex_prompt(actionable, start, start + timedelta(days=7))
    )
    assert CONTROL_PLANE_GUIDANCE in codex_prompt(
        actionable, start, start + timedelta(days=7)
    )


def test_unknown_actionable_findings_are_not_rendered_as_diagnoses():
    unknown = Finding(
        "unknown",
        "host",
        "svc",
        "docker",
        "error",
        "boom",
        3,
        classification="unclear",
        cause_status="unknown",
    )
    body = render_report(
        [unknown],
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 8, tzinfo=UTC),
        [],
    )

    assert "Priority findings:\n- None" in body
    assert "Unresolved:\n- [new] host / svc: 3" in body
    assert "Do not create a commit." not in body


def test_code_configuration_findings_get_one_remediation_attachment_and_html_is_escaped():
    finding = Finding(
        "fingerprint",
        "host<&",
        "svc",
        "docker",
        "error",
        "boom",
        4,
        classification="actionable_failure",
        severity="high",
        remediation_kind="configuration",
        impact="<important>",
        analysis="config cause",
        repair_plan="update managed config",
        verification=["run <check>"],
    )
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rendered = render_email_report([finding], start, start + timedelta(days=7), [])

    assert list(rendered.attachments) == [
        "weekly-operations-remediation-2026-01-08.txt"
    ]
    attachment = next(iter(rendered.attachments.values()))
    assert "Do not create a commit." in attachment
    assert "<important>" not in rendered.html
    assert "host&lt;&amp;" in rendered.html
    assert "<check>" not in rendered.html


def test_priority_sort_uses_severity_then_trend_then_fixability():
    low_new = Finding(
        "a",
        "host",
        "low",
        "docker",
        "error",
        "x",
        100,
        classification="actionable_failure",
        severity="low",
        remediation_kind="code",
    )
    high_recurring = Finding(
        "b",
        "host",
        "high",
        "docker",
        "error",
        "x",
        1,
        classification="actionable_failure",
        severity="high",
        trend="recurring",
        remediation_kind="operational",
    )
    rendered = render_email_report(
        [low_new, high_recurring],
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 8, tzinfo=UTC),
        [],
    )

    assert rendered.plain_text.index("host / high") < rendered.plain_text.index(
        "host / low"
    )


def test_historical_docker_host_fallback(monkeypatch):
    import operations_analyst as analyst

    monkeypatch.setattr(
        "common.loki.query_loki_range_adaptive",
        lambda *args, **kwargs: [
            {
                "stream": {
                    "job": "docker",
                    "service_name": "project/service",
                    "detected_level": "error",
                },
                "values": [["1000000000", "failed"]],
            }
        ],
    )
    start = datetime(2026, 1, 1, tzinfo=UTC)
    finding = analyst.collect_candidates(start, start + timedelta(hours=1))[0]
    assert finding.host == "docker.home.arpa"


def test_triage_limits_are_conservative():
    from operations_analyst import (
        MAX_MODEL_TEXT,
        MAX_REPOSITORY_TEXT,
        MAX_TRIAGE_FINDINGS,
        TRIAGE_BATCH_SIZE,
    )

    assert MAX_TRIAGE_FINDINGS <= 200
    assert TRIAGE_BATCH_SIZE <= 25
    assert MAX_MODEL_TEXT <= 1200
    assert MAX_REPOSITORY_TEXT <= 12000


def test_malformed_structured_response_keeps_triage_unresolved(monkeypatch):
    captured = []
    namespace = {}
    exec(
        compile(
            "def parse_chat_completion():\n"
            "    raise TypeError(\"'NoneType' object is not iterable\")\n",
            "/opt/test/.venv/lib/python3.14/site-packages/openai/lib/_parsing/_completions.py",
            "exec",
        ),
        namespace,
    )

    class FailingClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            captured.append(self)

        def with_structured_output(self, _schema, **_kwargs):
            return self

        def invoke(self, _prompt):
            namespace["parse_chat_completion"]()

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", FailingClient)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert analyzed[0].classification == "unclear"
    assert warnings == [
        "Triage batch 1: empty or malformed provider response; affected findings remain unresolved"
    ]
    assert captured[0].kwargs["extra_body"]["provider"] == {
        "data_collection": "deny",
        "require_parameters": True,
    }


def test_unrelated_type_error_propagates(monkeypatch):
    class Client:
        def __init__(self, **_kwargs):
            pass

        def with_structured_output(self, _schema, **_kwargs):
            return self

        def invoke(self, _prompt):
            raise TypeError("'NoneType' object is not iterable")

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", Client)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    with pytest.raises(TypeError, match="NoneType"):
        analyze_findings(_Vault(), [finding], None)


def test_native_structured_output_uses_schema_method_and_validates(monkeypatch):
    captured = []
    _install_fake_chat(
        monkeypatch,
        [
            {
                "decisions": [
                    {"fingerprint": "fingerprint", "classification": "expected_noise"}
                ]
            }
        ],
        captured,
    )
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert captured[0].structured_kwargs == {"method": "json_schema", "strict": True}
    assert captured[0].kwargs["extra_body"] == {
        "provider": {"data_collection": "deny", "require_parameters": True}
    }


def test_prompt_json_mode_supports_deepseek_without_native_structured_output(
    monkeypatch,
):
    captured = []
    _install_fake_chat(
        monkeypatch,
        [
            AIMessage(
                content=json.dumps(
                    {
                        "decisions": [
                            {
                                "fingerprint": "fingerprint",
                                "classification": "expected_noise",
                            }
                        ]
                    }
                )
            )
        ],
        captured,
    )
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(
        _Vault(
            _connection(
                {
                    "model": "deepseek/deepseek-v4.1-flash",
                    "structured_output_mode": "prompt_json",
                }
            )
        ),
        [finding],
        None,
    )

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert not hasattr(captured[0], "structured_kwargs")
    assert "JSON Schema" in captured[0].prompt
    assert captured[0].kwargs["extra_body"] == {"provider": {"data_collection": "deny"}}


def test_auto_falls_back_once_for_unsupported_native_output(monkeypatch):
    captured = []

    class Unsupported(Exception):
        status_code = 400
        code = "unsupported_parameter"

    class Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            captured.append(self)

        def with_structured_output(self, _schema, **_kwargs):
            return self

        def invoke(self, _prompt):
            if self.kwargs["extra_body"]["provider"].get("require_parameters"):
                raise Unsupported("response_format is unsupported by the provider")
            return AIMessage(
                content=json.dumps(
                    {
                        "decisions": [
                            {
                                "fingerprint": "fingerprint",
                                "classification": "expected_noise",
                            }
                        ]
                    }
                )
            )

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", Client)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert len(captured) == 2
    assert captured[1].kwargs["extra_body"]["provider"] == {"data_collection": "deny"}


def test_auto_falls_back_for_openrouter_no_endpoints_native_response(monkeypatch):
    captured = []
    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(404, request=request)
    native_error = openai.NotFoundError(
        "No endpoints found for this model",
        response=response,
        body={"error": {"message": "No endpoints found for this model"}},
    )

    class Client:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            captured.append(self)

        def with_structured_output(self, _schema, **_kwargs):
            return self

        def invoke(self, _prompt):
            if self.kwargs["extra_body"]["provider"].get("require_parameters"):
                raise native_error
            return AIMessage(
                content=json.dumps(
                    {
                        "decisions": [
                            {
                                "fingerprint": "fingerprint",
                                "classification": "expected_noise",
                            }
                        ]
                    }
                )
            )

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", Client)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert len(captured) == 2


def test_no_endpoints_in_compatibility_mode_remains_permanent(monkeypatch):
    import operations_analyst as analyst

    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(404, request=request)
    error = openai.NotFoundError(
        "No endpoints found for this model",
        response=response,
        body={"error": {"message": "No endpoints found for this model"}},
    )

    assert analyst._normalize_llm_exception(error, "prompt_json") is None


@pytest.mark.parametrize("status", [401, 403])
def test_authentication_and_authorization_failures_escape(monkeypatch, status):
    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(status, request=request)
    error_class = (
        openai.AuthenticationError if status == 401 else openai.PermissionDeniedError
    )
    error = error_class(
        "secret response body", response=response, body={"secret": "body"}
    )

    _install_fake_chat(monkeypatch, [error])
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    with pytest.raises(error_class):
        analyze_findings(_Vault(), [finding], None)


def test_retries_timeout_then_succeeds(monkeypatch):
    calls = []
    _install_fake_chat(
        monkeypatch,
        [
            openai.APITimeoutError(
                httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
            ),
            {
                "decisions": [
                    {"fingerprint": "fingerprint", "classification": "expected_noise"}
                ]
            },
        ],
        calls,
    )
    sleeps = []
    monkeypatch.setattr("operations_analyst.time.sleep", sleeps.append)
    monkeypatch.setattr("operations_analyst.random.uniform", lambda _a, _b: 0.0)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert sleeps == [1.0]


def test_retry_after_is_parsed_and_capped(monkeypatch):
    import operations_analyst as analyst

    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(429, request=request, headers={"Retry-After": "120"})
    error = openai.RateLimitError("provider unavailable", response=response, body={})
    failure = analyst._normalize_llm_exception(error)

    assert failure is not None
    assert analyst._retry_delay(failure, 1) == 60.0

    invalid_response = httpx.Response(
        429, request=request, headers={"Retry-After": "invalid"}
    )
    invalid_error = openai.RateLimitError(
        "provider unavailable", response=invalid_response, body={}
    )
    invalid_failure = analyst._normalize_llm_exception(invalid_error)
    monkeypatch.setattr("operations_analyst.random.uniform", lambda _a, _b: 0.0)

    assert invalid_failure is not None
    assert invalid_failure.retry_after is None
    assert analyst._retry_delay(invalid_failure, 1) == 1.0


def test_rate_limit_exhaustion_is_unresolved_and_reason_specific(monkeypatch, caplog):
    request_url = "https://openrouter.ai/api/v1/chat/completions"
    errors = []
    for _ in range(3):
        request = httpx.Request("POST", request_url)
        response = httpx.Response(429, request=request, headers={"Retry-After": "5"})
        errors.append(
            openai.RateLimitError(
                "secret response body", response=response, body={"secret": "body"}
            )
        )
    calls = []
    _install_fake_chat(monkeypatch, errors, calls)
    sleeps = []
    monkeypatch.setattr("operations_analyst.time.sleep", sleeps.append)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    assert analyzed[0].classification == "unclear"
    assert warnings == [
        "Triage batch 1: provider unavailable after retries; affected findings remain unresolved"
    ]
    assert sleeps == [5.0, 5.0]
    assert all("secret" not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize(
    "payload",
    [
        AIMessage(content=""),
        AIMessage(content="not json"),
        AIMessage(content=json.dumps({"wrong": []})),
        AIMessage(content="refused", additional_kwargs={"refusal": "secret refusal"}),
    ],
)
def test_prompt_json_failures_are_unresolved(monkeypatch, payload):
    _install_fake_chat(monkeypatch, [payload])
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(
        _Vault(_connection({"structured_output_mode": "prompt_json"})),
        [finding],
        None,
    )

    assert analyzed[0].classification == "unclear"
    assert warnings[0].startswith("Triage batch 1: ")
    assert "secret" not in warnings[0]


@pytest.mark.parametrize(
    ("decisions", "expected"),
    [
        ([{"fingerprint": "fingerprint", "classification": "expected_noise"}], None),
        ([], "missing=1, duplicated=0, unexpected=0"),
        (
            [{"fingerprint": "fingerprint", "classification": "expected_noise"}] * 2,
            "missing=0, duplicated=1, unexpected=0",
        ),
        (
            [{"fingerprint": "other", "classification": "expected_noise"}],
            "missing=1, duplicated=0, unexpected=1",
        ),
    ],
)
def test_triage_response_cardinality_is_all_or_nothing(
    monkeypatch, decisions, expected
):
    _install_fake_chat(monkeypatch, [{"decisions": decisions}])
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(_Vault(), [finding], None)

    if expected is None:
        assert analyzed[0].classification == "expected_noise"
        assert not warnings
    else:
        assert analyzed[0].classification == "unclear"
        assert expected in warnings[0]
        assert "fingerprint" not in warnings[0]


def test_reasoning_and_privacy_parameters_are_stage_specific(monkeypatch):
    captured = []
    _install_fake_chat(
        monkeypatch,
        [
            {
                "decisions": [
                    {
                        "fingerprint": "fingerprint",
                        "classification": "actionable_failure",
                    }
                ]
            },
            {
                "additional_log_queries": [],
                "repository_files": [],
                "repository_searches": [],
                "web_queries": [],
            },
            {
                "impact": "low",
                "severity": "low",
                "remediation_kind": "unknown",
                "cause_status": "unknown",
                "confidence": "low",
                "analysis": "insufficient evidence",
                "repair_plan": "collect more evidence",
            },
        ],
        captured,
    )
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    class Corpus:
        def list_repositories(self):
            return []

    analyze_findings(
        _Vault(_connection({"zdr": True, "triage_reasoning_effort": "low"})),
        [finding],
        Corpus(),
    )

    assert captured[0].kwargs["extra_body"] == {
        "provider": {
            "data_collection": "deny",
            "require_parameters": True,
            "zdr": True,
        },
        "reasoning": {"effort": "low"},
    }
    assert captured[1].kwargs["extra_body"] == {
        "provider": {
            "data_collection": "deny",
            "require_parameters": True,
            "zdr": True,
        },
        "reasoning": {"effort": "low"},
    }


def test_real_chat_openai_request_handles_choices_null_without_leaking_secrets(
    monkeypatch, caplog
):
    requests = []

    def transport(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "completion-id",
                "object": "chat.completion",
                "created": 1,
                "model": "test-model",
                "choices": None,
                "usage": {
                    "prompt_tokens": 1,
                    "completion_tokens": 1,
                    "total_tokens": 2,
                },
            },
        )

    real_chat_openai = langchain_openai.ChatOpenAI

    def chat_openai_with_mock_transport(**kwargs):
        kwargs["http_client"] = httpx.Client(transport=httpx.MockTransport(transport))
        return real_chat_openai(**kwargs)

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", chat_openai_with_mock_transport)
    finding = Finding(
        "fingerprint",
        "host",
        "service",
        "job",
        "error",
        "failure",
        1,
        evidence=[Evidence("2026-01-01T00:00:00+00:00", "private log evidence")],
    )

    analyzed, warnings = analyze_findings(
        _Vault(_connection({"structured_output_mode": "json_schema"})),
        [finding],
        None,
    )

    assert analyzed[0].classification == "unclear"
    assert warnings == [
        "Triage batch 1: empty or malformed provider response; affected findings remain unresolved"
    ]
    assert len(requests) == 1
    assert requests[0]["provider"] == {
        "data_collection": "deny",
        "require_parameters": True,
    }
    assert requests[0]["response_format"]["type"] == "json_schema"
    assert requests[0]["model"] == "test-model"
    assert requests[0]["max_completion_tokens"] == 10000
    assert "reasoning_effort" not in requests[0]
    failure_log = next(
        record.getMessage()
        for record in caplog.records
        if "model attempt failed" in record.getMessage()
    )
    assert "stage=triage_batch_1" in failure_log
    assert "structured_output_mode=json_schema" in failure_log
    assert "reason=empty_or_malformed_response" in failure_log
    assert "attempt=1" in failure_log
    assert "max_attempts=3" in failure_log
    assert "elapsed_seconds=" in failure_log
    assert all(
        token not in record.getMessage()
        for record in caplog.records
        for token in ("test-key", "private log evidence")
    )


def test_real_prompt_json_request_omits_native_structured_parameters(monkeypatch):
    requests = []

    def transport(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "completion-id",
                "object": "chat.completion",
                "created": 1,
                "model": "deepseek/deepseek-v4.1-flash",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": json.dumps(
                                {
                                    "decisions": [
                                        {
                                            "fingerprint": "fingerprint",
                                            "classification": "expected_noise",
                                        }
                                    ]
                                }
                            ),
                        },
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    real_chat_openai = langchain_openai.ChatOpenAI

    def chat_openai_with_mock_transport(**kwargs):
        kwargs["http_client"] = httpx.Client(transport=httpx.MockTransport(transport))
        return real_chat_openai(**kwargs)

    monkeypatch.setattr(langchain_openai, "ChatOpenAI", chat_openai_with_mock_transport)
    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)

    analyzed, warnings = analyze_findings(
        _Vault(
            _connection(
                {
                    "model": "deepseek/deepseek-v4.1-flash",
                    "structured_output_mode": "prompt_json",
                }
            )
        ),
        [finding],
        None,
    )

    assert not warnings
    assert analyzed[0].classification == "expected_noise"
    assert requests[0]["provider"] == {"data_collection": "deny"}
    assert "response_format" not in requests[0]
    assert "reasoning_effort" not in requests[0]
    assert "reasoning" not in requests[0]


@pytest.mark.parametrize(
    ("failed_stage", "expected_phrase"),
    [
        ("research_plan", "Deep research plan for host/service: output truncated"),
        ("diagnosis", "Diagnosis for host/service: output truncated"),
    ],
)
def test_research_and_diagnosis_failures_remain_unresolved(
    monkeypatch, failed_stage, expected_phrase
):
    if failed_stage == "research_plan":
        outcomes = [
            {
                "decisions": [
                    {
                        "fingerprint": "fingerprint",
                        "classification": "actionable_failure",
                    }
                ]
            },
            openai.LengthFinishReasonError.__new__(openai.LengthFinishReasonError),
        ]
        outcomes[1].completion = None
    else:
        outcomes = [
            {
                "decisions": [
                    {
                        "fingerprint": "fingerprint",
                        "classification": "actionable_failure",
                    }
                ]
            },
            {
                "additional_log_queries": [],
                "repository_files": [],
                "repository_searches": [],
                "web_queries": [],
            },
            openai.LengthFinishReasonError.__new__(openai.LengthFinishReasonError),
        ]
        outcomes[-1].completion = None
    _install_fake_chat(monkeypatch, outcomes)
    monkeypatch.setattr("operations_analyst.add_log_context", lambda _finding: None)

    class Corpus:
        def list_repositories(self):
            return []

    finding = Finding("fingerprint", "host", "service", "job", "error", "failure", 1)
    analyzed, warnings = analyze_findings(_Vault(), [finding], Corpus())

    assert analyzed[0].classification == "unclear"
    assert warnings[0].startswith(expected_phrase)
