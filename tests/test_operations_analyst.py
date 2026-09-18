from datetime import UTC, datetime, timedelta
import subprocess

import pytest

from operations_analyst import (
    CONTROL_PLANE_GUIDANCE,
    EPISODE_GAP,
    EPISODE_INVESTIGATION_SCORE_THRESHOLD,
    FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD,
    MAX_DEEP_FINDINGS,
    MAX_LOCAL_CONTEXT_LOGS,
    MAX_LOCAL_CONTEXT_QUERIES_PER_EPISODE,
    Diagnosis,
    EmitterKey,
    EpisodeFingerprint,
    EpisodeLocalContext,
    EpisodeSummary,
    Evidence,
    Finding,
    FingerprintLocalContext,
    LogOccurrence,
    LocalLog,
    RepositoryCorpus,
    ResearchPlan,
    TriageScore,
    analyze_findings,
    aggregate_findings,
    build_fingerprint_jev_state,
    build_operational_episodes,
    codex_prompt,
    derive_fingerprint_context,
    enrich_episode_context,
    fingerprint,
    rank_deep_candidates,
    redact_web_query,
    render_email_report,
    render_report,
    select_context_windows,
    triage_episode_with_jev,
    triage_fingerprint_with_jev,
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
    (source / "uv.lock").write_text("updated generated material")
    subprocess.run(["git", "-C", source, "commit", "-am", "Update"], check=True)

    assert corpus.sync() == []
    assert (
        corpus.read_file("repo", "uv.lock")["content"] == "updated generated material"
    )
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


def _occurrence(
    timestamp: datetime,
    fingerprint_value: str = "fp",
    *,
    host: str = "host",
    service: str = "service",
    source: str = "job",
    level: str = "error",
    service_label: str = "service_name",
    line: str | None = None,
) -> LogOccurrence:
    occurrence_line = line or f"{level} {fingerprint_value} at {timestamp.isoformat()}"
    occurrence_template = (
        fingerprint(occurrence_line)[1] if line else f"template {fingerprint_value}"
    )
    return LogOccurrence(
        timestamp=timestamp,
        line=occurrence_line,
        fingerprint=fingerprint_value,
        template=occurrence_template,
        host=host,
        service=service,
        source=source,
        level=level,
        service_label=service_label,
    )


class _GateProvider:
    def __init__(self, episode_score: float, fingerprint_score: float = 0.0):
        self.episode_score = episode_score
        self.fingerprint_score = fingerprint_score
        self.calls: list[tuple[dict[str, object], str]] = []

    def score(self, state, *, question_name, instructions, criteria):
        self.calls.append((state, question_name))
        score = (
            self.episode_score
            if question_name == "episode_investigation"
            else self.fingerprint_score
        )
        return TriageScore(
            score=score,
            probabilities={0: 0.1, 1: 0.2, 2: 0.3, 3: 0.4},
            confidence=0.8,
        )


def test_episode_gap_and_emitter_rules_are_deterministic():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [
        _occurrence(start + timedelta(minutes=20), "b", service="other"),
        _occurrence(start + timedelta(minutes=10), "a", level="warning"),
        _occurrence(start, "a"),
        _occurrence(start + timedelta(minutes=21), "a"),
        _occurrence(start + timedelta(minutes=9), "a"),
        _occurrence(start, "a", source="another-job"),
    ]

    episodes = build_operational_episodes(occurrences)

    assert EPISODE_GAP == timedelta(minutes=10)
    assert len(episodes) == 4
    same_emitter = [
        episode
        for episode in episodes
        if episode.emitter == EmitterKey("host", "service", "job")
    ]
    assert len(same_emitter) == 2
    assert same_emitter[0].total_events == 3
    assert same_emitter[0].start == start
    assert same_emitter[0].end == start + timedelta(minutes=10)


def test_episode_fingerprint_and_global_fingerprint_identity_are_stable():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    line_a = "2026-01-01T00:00:00Z refused 10.0.0.1 id=abc123456789012345678901"
    line_b = "2026-01-02T00:00:00Z refused 10.0.0.2 id=xyz123456789012345678901"
    digest_a, template = fingerprint(line_a)
    digest_b, _ = fingerprint(line_b)
    assert digest_a == digest_b

    occurrences = [
        _occurrence(start, digest_a, line=line_a),
        _occurrence(start + timedelta(minutes=11), digest_b, line=line_b),
    ]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)

    assert len(episodes) == 2
    assert [item.fingerprints[0].fingerprint for item in episodes] == [
        digest_a,
        digest_b,
    ]
    assert findings[0].fingerprint == digest_a
    assert findings[0].count == 2
    assert findings[0].template == template


def test_stage_one_rejects_before_enrichment(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [_occurrence(start)]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)
    provider = _GateProvider(EPISODE_INVESTIGATION_SCORE_THRESHOLD - 0.01)

    enrich_calls = []
    monkeypatch.setattr(
        "operations_analyst.enrich_episode_context",
        lambda episode: enrich_calls.append(episode) or EpisodeLocalContext(),
    )
    analyzed, warnings = analyze_findings(
        object(), findings, None, episodes=episodes, jev_provider=provider
    )

    assert not warnings
    assert enrich_calls == []
    assert episodes[0].local_context is None
    assert len(provider.calls) == 1
    assert analyzed[0].episode_summaries[0].stage2_score is None


def test_stage_one_threshold_and_routing_use_score_only():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes([_occurrence(start)])[0]

    class Provider:
        def __init__(self, score, probabilities, confidence):
            self.answer = TriageScore(
                score=score, probabilities=probabilities, confidence=confidence
            )

        def score(self, state, **kwargs):
            return self.answer

    low_confidence = triage_episode_with_jev(
        episode, Provider(EPISODE_INVESTIGATION_SCORE_THRESHOLD, {0: 1.0}, 0.1)
    )
    high_confidence = triage_episode_with_jev(
        episode, Provider(EPISODE_INVESTIGATION_SCORE_THRESHOLD, {3: 1.0}, 0.99)
    )

    assert low_confidence.score >= EPISODE_INVESTIGATION_SCORE_THRESHOLD
    assert high_confidence.score >= EPISODE_INVESTIGATION_SCORE_THRESHOLD
    assert low_confidence.confidence != high_confidence.confidence
    assert low_confidence.probabilities != high_confidence.probabilities


def test_stage_one_fail_open_selects_for_enrichment(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [_occurrence(start)]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)

    class FailingStageOne:
        def __init__(self):
            self.calls = 0

        def score(self, state, *, question_name, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("provider unavailable")
            return TriageScore(score=0, probabilities={0: 1.0}, confidence=1.0)

    provider = FailingStageOne()
    monkeypatch.setattr(
        "operations_analyst.enrich_episode_context",
        lambda episode: EpisodeLocalContext(),
    )
    analyzed, warnings = analyze_findings(
        object(), findings, None, episodes=episodes, jev_provider=provider
    )

    assert any("selected fail-open" in warning for warning in warnings)
    assert episodes[0].stage1_score.score == 3
    assert provider.calls == 2
    assert analyzed[0].episode_summaries[0].stage2_score == 0


def test_local_enrichment_has_episode_query_bound_and_reuses_info_warning(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [
        _occurrence(start + timedelta(minutes=index * 9), str(index))
        for index in range(20)
    ]
    episode = build_operational_episodes(occurrences)[0]
    queries = []

    def query(_connection, *, query, start, end, limit):
        queries.append((query, start, end, limit))
        timestamp = int(start.timestamp() * 1_000_000_000)
        return {
            "data": {
                "result": [
                    {
                        "stream": {
                            "job": "job",
                            "service_name": "service",
                            "detected_level": "info",
                        },
                        "values": [[str(timestamp), "service starting"]],
                    },
                    {
                        "stream": {
                            "job": "job",
                            "service_name": "service",
                            "detected_level": "warning",
                        },
                        "values": [[str(timestamp + 1), "retrying connection"]],
                    },
                ]
            }
        }

    monkeypatch.setattr("common.loki.query_loki_range", query)
    context = enrich_episode_context(episode)

    assert len(queries) <= MAX_LOCAL_CONTEXT_QUERIES_PER_EPISODE
    assert context.query_count == len(queries)
    local = derive_fingerprint_context(episode, episode.fingerprints[0], context)
    assert local.info_count >= 1
    assert local.warning_count >= 1
    assert any("service starting" in line for line in local.representative_lines)


def test_local_enrichment_uses_the_service_label_that_defined_the_emitter():
    import operations_analyst as analyst

    start = datetime(2026, 1, 1, tzinfo=UTC)
    container_episode = build_operational_episodes(
        [_occurrence(start, service_label="container_name")]
    )[0]
    job_episode = build_operational_episodes(
        [_occurrence(start, service="job", service_label="job")]
    )[0]

    assert 'container_name="service"' in analyst._emitter_query(container_episode)
    assert "service_name" not in analyst._emitter_query(container_episode)
    assert 'job="job"' in analyst._emitter_query(job_episode)
    assert "service_name" not in analyst._emitter_query(job_episode)


def test_local_enrichment_samples_across_a_noisy_context_window(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes([_occurrence(start)])[0]
    logs = [
        LocalLog(
            timestamp=start + timedelta(seconds=index),
            line=f"log-{index}",
            fingerprint=f"fp-{index}",
            template=f"template-{index}",
            level="info",
        )
        for index in range(MAX_LOCAL_CONTEXT_LOGS + 100)
    ]

    monkeypatch.setattr("operations_analyst.query_episode_context", lambda *_: logs)
    context = enrich_episode_context(episode)

    assert len(context.logs) == MAX_LOCAL_CONTEXT_LOGS
    assert context.logs[0].line == "log-0"
    assert context.logs[-1].line == f"log-{MAX_LOCAL_CONTEXT_LOGS + 99}"


def test_overlapping_context_windows_are_deduplicated():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes(
        [_occurrence(start), _occurrence(start + timedelta(minutes=1), "two")]
    )[0]
    windows = select_context_windows(episode)
    assert len(windows) == 1


def test_stage_two_state_contains_bounded_episode_context():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes([_occurrence(start)])[0]
    context = FingerprintLocalContext(
        representative_lines=["info before", "warning before"],
        nearby_templates=[{"template": "recovered", "count": 1}],
        info_count=1,
        warning_count=1,
    )
    state = build_fingerprint_jev_state(episode, episode.fingerprints[0], context)

    assert set(state) == {"emitter", "episode_summary", "fingerprint", "local_context"}
    assert state["local_context"]["representative_lines"] == [
        "info before",
        "warning before",
    ]
    assert "values" not in str(state)


def test_stage_two_threshold_and_score_only_routing():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes([_occurrence(start)])[0]
    context = FingerprintLocalContext()

    class Provider:
        def score(self, state, **kwargs):
            return TriageScore(
                score=FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD,
                probabilities={0: 1.0},
                confidence=0.01,
            )

    result = triage_fingerprint_with_jev(
        episode, episode.fingerprints[0], context, Provider()
    )
    assert result.score >= FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD


def test_deep_candidate_ranking_prioritizes_score_and_is_deterministic():
    import operations_analyst as analyst

    start = datetime(2026, 1, 1, tzinfo=UTC)
    episode = build_operational_episodes([_occurrence(start, "fp")])[0]
    candidates = []
    for score, count in [(2.1, 100), (3.0, 1), (2.5, 2)]:
        fingerprint_value = EpisodeFingerprint(
            fingerprint=f"fp-{score}",
            template="error",
            count=count,
            levels={"error": count},
            first_seen=start,
            last_seen=start,
        )
        candidates.append(
            analyst.FingerprintEpisodeCandidate(
                fingerprint=fingerprint_value,
                episode=episode,
                local_context=FingerprintLocalContext(),
                score=TriageScore(score=score, probabilities={}, confidence=0.5),
            )
        )

    ranked = rank_deep_candidates(candidates)
    assert [item.score.score for item in ranked] == [3.0, 2.5, 2.1]


def test_repeated_fingerprint_aggregates_but_keeps_episode_summaries():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [
        _occurrence(start, "same"),
        _occurrence(start + timedelta(minutes=11), "same"),
    ]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)

    assert findings[0].count == 2
    assert findings[0].fingerprint == episodes[0].fingerprints[0].fingerprint
    assert len(episodes) == 2


def test_global_deep_budget_limits_reasoning_and_reuses_episode_context(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [
        _occurrence(start + timedelta(seconds=index), f"fp-{index}")
        for index in range(MAX_DEEP_FINDINGS + 1)
    ]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)
    provider = _GateProvider(3.0, 3.0)
    enrichment_calls = []
    model_calls = []

    monkeypatch.setattr(
        "operations_analyst.enrich_episode_context",
        lambda episode: enrichment_calls.append(episode) or EpisodeLocalContext(),
    )

    def invoke(schema, _prompt, stage):
        model_calls.append(stage)
        if schema is ResearchPlan:
            return ResearchPlan()
        return Diagnosis(
            impact="service degraded",
            severity="high",
            remediation_kind="configuration",
            cause_status="likely",
            confidence="high",
            analysis="configuration is invalid",
            repair_plan="restore the managed configuration",
        )

    analyzed, warnings = analyze_findings(
        object(),
        findings,
        None,
        episodes=episodes,
        jev_provider=provider,
        reasoning_invoker=invoke,
    )

    assert len(enrichment_calls) == 1
    assert model_calls.count("research_plan") == MAX_DEEP_FINDINGS
    assert model_calls.count("diagnosis") == MAX_DEEP_FINDINGS
    assert len(model_calls) == MAX_DEEP_FINDINGS * 2
    assert sum(len(finding.diagnoses) for finding in analyzed) == MAX_DEEP_FINDINGS
    assert any("global investigation budget" in warning for warning in warnings)


def test_actionable_episode_diagnosis_is_not_overwritten_by_a_later_one(monkeypatch):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    occurrences = [
        _occurrence(start, "same"),
        _occurrence(start + timedelta(minutes=11), "same"),
    ]
    episodes = build_operational_episodes(occurrences)
    findings = aggregate_findings(occurrences)
    diagnoses = iter(
        [
            Diagnosis(
                impact="service degraded",
                severity="high",
                remediation_kind="configuration",
                cause_status="likely",
                confidence="high",
                analysis="configuration is invalid",
                repair_plan="restore the managed configuration",
            ),
            Diagnosis(
                impact="no durable impact established",
                severity="low",
                remediation_kind="unknown",
                cause_status="unknown",
                confidence="low",
                analysis="later episode was transient",
                repair_plan="",
            ),
        ]
    )
    monkeypatch.setattr(
        "operations_analyst.enrich_episode_context", lambda _: EpisodeLocalContext()
    )

    def invoke(schema, _prompt, _stage):
        return ResearchPlan() if schema is ResearchPlan else next(diagnoses)

    analyzed, _ = analyze_findings(
        object(),
        findings,
        None,
        episodes=episodes,
        jev_provider=_GateProvider(3.0, 3.0),
        reasoning_invoker=invoke,
    )

    finding = analyzed[0]
    assert finding.classification == "actionable_failure"
    assert finding.analysis == "configuration is invalid"
    assert len(finding.diagnoses) == 2


def test_reporting_includes_bounded_episode_summaries():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    finding = Finding(
        "fp",
        "host",
        "service",
        "job",
        "error",
        "failure",
        2,
        classification="actionable_failure",
        severity="high",
        remediation_kind="configuration",
        analysis="configuration drift",
        repair_plan="restore the managed setting",
        episode_summaries=[
            EpisodeSummary(
                "episode-id",
                EmitterKey("host", "service", "job"),
                start,
                start + timedelta(minutes=2),
                2,
                stage1_score=2,
                stage2_score=3,
            )
        ],
    )
    report = render_report([finding], start, start + timedelta(days=7), [])

    assert "Episodes: 1" in report
    assert "stage-2 score 3.00" in report
    assert "episode-id" not in report
