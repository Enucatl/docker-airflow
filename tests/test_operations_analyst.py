from datetime import UTC, datetime, timedelta
import subprocess

import pytest

from automation.pipelines.operations_analyst import (
    CONTROL_PLANE_GUIDANCE,
    Evidence,
    Finding,
    RepositoryCorpus,
    codex_prompt,
    fingerprint,
    redact_web_query,
    render_report,
    weekly_slices,
)


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

    assert body.count("Do not create a commit.") == 1
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

    assert "Actionable diagnoses:\n- None" in body
    assert "Unresolved:\n- [new] host / svc: 3" in body
    assert "Do not create a commit." not in body


def test_historical_docker_host_fallback(monkeypatch):
    import automation.pipelines.operations_analyst as analyst

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
    from automation.pipelines.operations_analyst import (
        MAX_MODEL_TEXT,
        MAX_REPOSITORY_TEXT,
        MAX_TRIAGE_FINDINGS,
        TRIAGE_BATCH_SIZE,
    )

    assert MAX_TRIAGE_FINDINGS <= 200
    assert TRIAGE_BATCH_SIZE <= 25
    assert MAX_MODEL_TEXT <= 1200
    assert MAX_REPOSITORY_TEXT <= 12000
