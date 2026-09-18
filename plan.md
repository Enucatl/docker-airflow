Implement a clean architectural refactor of the `operations_analyst` package in:

`packages/operations-analyst/src/operations_analyst/__init__.py`

Repository:

`Enucatl/docker-airflow`

Relevant tests are currently in:

`tests/test_operations_analyst.py`

The goal is to replace the existing OpenRouter-based bulk triage with a two-stage TypeSafe/Jev decision pipeline while keeping the strong reasoning model for actual diagnosis and research.

This is a direct cutover. Do not implement shadow mode, feature flags, parallel old/new paths, compatibility wrappers for deleted behavior, or dead fallback code. After the change, the repository should contain only the new architecture.

---

# 1. Target architecture

The desired pipeline is:

```text
Loki operational logs
        ↓
normalize / fingerprint
        ↓
group by emitter
        ↓
split each emitter into 10-minute-gap episodes
        ↓
build deterministic episode summaries
        ↓
Jev stage 1: episode investigation score
        │
        ├── below threshold → discard from further investigation
        │
        └── above threshold
                    ↓
           enrich episode locally
           with surrounding logs
           including info/warning
                    ↓
           derive fingerprint-local context
                    ↓
           Jev stage 2:
           fingerprint investigation score
                    │
                    ├── below threshold → skip
                    │
                    └── above threshold
                                ↓
                         global MAX_DEEP_FINDINGS
                                ↓
                         existing research planning
                         ├── bounded Loki research
                         ├── repository research
                         └── public web research
                                ↓
                         strong reasoning model
                                ↓
                         fingerprint-level Diagnosis
                                ↓
                         aggregate/report by fingerprint
                         with episode summaries
```

There are three important units:

```text
Emitter
= host + service + job

Episode
= temporally connected operational log activity from one emitter

Fingerprint
= existing normalized repeated log-message template
```

The deep-analysis unit remains the fingerprint, but a fingerprint is analyzed in the context of the episode in which it appeared.

---

# 2. Preserve the useful deterministic preprocessing

Keep the existing fingerprint normalization concept.

The existing code strips ANSI escape sequences and masks variable fields such as timestamps, UUIDs, IP addresses, hashes and long identifiers before hashing the normalized message.

Keep this behavior unless tests reveal an actual defect.

The current fingerprint remains the stable cross-run identity used for:

* trend tracking
* report aggregation
* previous-state comparison
* grouping repeated instances of the same log pattern

Do not replace the fingerprint with a model-generated category.

---

# 3. Change collection so temporal episodes can be constructed correctly

The current collector aggregates directly into `Finding` objects across the whole reporting window.

That loses the complete temporal event sequence needed to build episodes.

Refactor collection so that normalized log occurrences are retained long enough to build episodes before global aggregation.

Introduce an internal representation similar to:

```python
@dataclass(frozen=True)
class EmitterKey:
    host: str
    service: str
    source: str  # current job label


@dataclass
class LogOccurrence:
    timestamp: datetime
    line: str
    fingerprint: str
    template: str
    host: str
    service: str
    source: str
    level: str
```

Use the existing service derivation:

```text
service_name
or container_name
or job
or "unknown"
```

Use the current fallback host behavior.

`level` is deliberately NOT part of `EmitterKey`.

Errors, warnings and other levels from the same service may be part of one operational event.

---

# 4. Keep the primary collection selector focused on serious logs

The initial candidate collection can continue to use the existing operational selector that targets:

```text
error
critical
fatal
emergency
```

and excludes the existing noisy jobs.

Do not broaden the entire reporting-window collection to info/debug logs.

Lower-severity logs are only pulled later as local enrichment for episodes that survive Jev stage 1.

This is important for keeping Loki volume and memory use bounded.

---

# 5. Build emitter groups

Group collected `LogOccurrence` objects by:

```python
EmitterKey(
    host=occurrence.host,
    service=occurrence.service,
    source=occurrence.source,
)
```

Do not group by host alone.

Do not include level in the emitter key.

Do not introduce model inference to determine emitter identity.

---

# 6. Split each emitter into temporal episodes

Within each emitter:

1. sort occurrences chronologically
2. start a new episode whenever the gap between consecutive candidate occurrences is greater than 10 minutes
3. otherwise append the occurrence to the current episode

Use exactly:

```python
EPISODE_GAP = timedelta(minutes=10)
```

The comparison should conceptually be:

```python
if current.timestamp - previous.timestamp > EPISODE_GAP:
    start_new_episode()
```

A gap of exactly 10 minutes remains part of the same episode.

Create a model similar to:

```python
@dataclass
class EpisodeFingerprint:
    fingerprint: str
    template: str
    count: int
    levels: dict[str, int]
    first_seen: datetime
    last_seen: datetime
    evidence: list[Evidence]


@dataclass
class OperationalEpisode:
    emitter: EmitterKey
    start: datetime
    end: datetime
    total_events: int
    fingerprints: list[EpisodeFingerprint]
```

Do not assign random UUIDs.

If an episode identifier is useful for logging/tracing, derive it deterministically from the emitter key and episode start timestamp.

---

# 7. Keep global fingerprint aggregation for trends/reporting

Episodes are an analysis structure, not a replacement for stable fingerprint identity.

Continue to build global `Finding`-level aggregates for the reporting window.

A fingerprint may appear in several episodes.

The same fingerprint should still map to the same stable cross-run fingerprint used by `apply_trends()` and previous-run state.

Refactor data flow cleanly so both are available:

```text
occurrences
    ├── episode structure for analysis
    └── global fingerprint aggregate for trends/reporting
```

Do not fake episodes from the current five sampled `Evidence` records. Build them from the actual collected occurrence timestamps before sampling.

---

# 8. Replace OpenRouter triage with TypeSafe/Jev

Delete the existing OpenRouter fingerprint triage mechanism.

Specifically remove obsolete concepts that are only used for bulk triage, including as applicable:

```text
TriageDecision
TriageBatch
TRIAGE_BATCH_SIZE
TRIAGE_MAX_COMPLETION_TOKENS
MAX_TRIAGE_FINDINGS
triage_reasoning_effort
triage_batch_* stages
batch decision cardinality checking
OpenRouter triage prompts
triage-specific structured-output compatibility logic
```

Do not delete OpenRouter reliability/error handling that is still needed by:

```text
ResearchPlan
Diagnosis
```

Refactor shared infrastructure if necessary so remaining reasoning-model code is simpler and no longer contains branches for a deleted triage stage.

---

# 9. Add the official TypeSafe Python SDK

Use the current official TypeSafe Python SDK and API documented by TypeSafe.

Do not implement the HTTP protocol manually unless the repository has a compelling existing abstraction that makes that preferable.

Add the required package dependency to the appropriate package/workspace configuration and update the repository lockfile.

Use normal repository secret/configuration conventions.

The TypeSafe API key must come from configuration/Vault/environment rather than source code.

Keep TypeSafe configuration independent from:

`operations_analyst_openrouter`

The two providers have different responsibilities.

---

# 10. Create a small Jev provider boundary

Do not leak SDK-specific result objects throughout `operations_analyst`.

Create a thin local abstraction that converts Jev results to application-owned models.

At minimum retain for every routing Score:

```python
class TriageScore(BaseModel):
    score: float
    probabilities: dict[str, float] | dict[int, float]
    confidence: float
```

Adapt the exact probability-key type to the real SDK response.

Do not throw away:

* score
* probability distribution
* confidence

These values must be available for:

* logging
* tracing
* future threshold calibration
* tests
* later routing improvements

For v1, however, routing must use only the `score`.

Do NOT branch on confidence.

Do NOT branch directly on individual probabilities.

Do NOT implement confidence fallback behavior in v1.

Store and observe those values, but the decision rule is score-only.

---

# 11. Use explicit Score rubrics rather than vague labels

Use Jev `Score` rather than generative classification for both gates.

The Score rubric should use concrete operational descriptions, not just:

```text
low
medium
high
```

Create one primary routing Score per gate.

It is acceptable to ask additional atomic Jev questions in the same request for observability if useful, but routing in v1 must depend solely on the primary investigation score.

Use named application constants for thresholds.

For example:

```python
EPISODE_INVESTIGATION_SCORE_THRESHOLD = ...
FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD = ...
```

Do not scatter numeric thresholds throughout the implementation.

Choose the initial values to satisfy the following policy:

* stage 1 may be moderately aggressive
* stage 1 should eliminate clearly routine operational episodes before any extra Loki queries
* stage 2 can be more selective because it has richer local evidence
* the purpose is cost/query reduction without making the episode gate so permissive that nearly every episode triggers context queries

Use the actual Score scale defined by the chosen Jev rubric/API rather than assuming an undocumented normalized range.

Document the rubric and threshold relationship in code.

Tests should make threshold behavior explicit.

---

# 12. Jev stage 1: episode-level gate

Stage 1 runs before any extra Loki query.

Its input is constructed entirely from already-collected candidate logs.

Build a compact deterministic episode state.

Example shape:

```python
{
    "emitter": {
        "host": ...,
        "service": ...,
        "source": ...,
    },
    "episode": {
        "start": ...,
        "end": ...,
        "duration_seconds": ...,
        "total_error_events": ...,
        "unique_fingerprints": ...,
        "levels": {
            "error": ...,
            "critical": ...,
            ...
        },
    },
    "fingerprints": [
        {
            "fingerprint": ...,
            "template": ...,
            "count": ...,
            "levels": ...,
            "first_seen": ...,
            "last_seen": ...,
        },
        ...
    ],
}
```

Bound very large states.

Do not send hundreds or thousands of full raw log lines.

For each fingerprint, the normalized template and aggregate statistics are the important first-stage evidence.

If there are many unique fingerprints, include all compact records if the resulting state remains sane; otherwise use a deterministic bounded strategy that preserves:

* highest-severity fingerprints
* highest-count fingerprints
* rare fingerprints

Do not simply drop low-count fingerprints because rare failures can be important.

Avoid arbitrary random sampling.

The primary Jev question should be conceptually:

```text
How strongly does this operational episode warrant investigation of its individual error fingerprints?
```

The ordered rubric should distinguish concrete cases approximately like:

```text
0:
Routine/expected operational noise with no indication that individual
fingerprints warrant investigation.

1:
Mostly routine/transient behavior; individual inspection is unlikely
to reveal a durable operational problem.

2:
Plausible operational problem or unusual behavior; inspecting the
fingerprints could reveal a durable issue.

3:
Clear evidence of service degradation, repeated failure, crash/restart
behavior, dependency failure, or another condition that warrants
individual investigation.
```

Use whatever exact number of levels fits the current SDK cleanly, but preserve this semantic progression.

Stage-1 routing:

```python
if score < EPISODE_INVESTIGATION_SCORE_THRESHOLD:
    skip episode
else:
    enrich episode locally
```

Again: only score controls this branch.

Retain probabilities and confidence.

---

# 13. Stage 1 must help prevent query explosion

This is an explicit architecture requirement.

Do not perform `add_log_context()` for every collected fingerprint before stage 1.

Do not query surrounding logs for episodes rejected by stage 1.

Log counters showing:

```text
episodes collected
episodes rejected by stage 1
episodes selected for enrichment
```

The implementation should make it easy to observe whether the first gate is sufficiently aggressive.

---

# 14. Replace per-fingerprint `add_log_context()` query behavior

The current implementation issues local Loki context queries after triage on individual findings.

Do not simply move that function earlier and call it once per fingerprint.

That would create query explosion when one episode contains many fingerprints.

Instead implement episode-level local-context collection.

Create something conceptually like:

```python
def enrich_episode_context(
    episode: OperationalEpisode,
) -> EpisodeLocalContext:
    ...
```

This function should retrieve surrounding logs from the SAME emitter and make them reusable for every fingerprint in the episode.

The context query should include lower-severity messages such as:

```text
info
warning
warn
error
critical
fatal
emergency
```

Do not include unrestricted debug/trace output unless existing labels make doing so harmless.

---

# 15. Bound Loki enrichment queries per episode

An episode can theoretically remain open for a long time if errors occur less than 10 minutes apart continuously.

Therefore do not query the full episode range without bounds.

Use representative local windows.

Select up to three deterministic anchor times per episode, such as:

```text
first significant occurrence
middle representative occurrence
last significant occurrence
```

or equivalent anchors that maximize coverage.

For each anchor, query a small bounded window, preferably approximately:

```text
anchor - 5 minutes
anchor + 5 minutes
```

Deduplicate overlapping windows before querying.

Define:

```python
MAX_LOCAL_CONTEXT_QUERIES_PER_EPISODE = 3
```

Do not issue more than that many normal enrichment queries for one episode.

This limit is independent from the later:

```python
MAX_ADDITIONAL_LOG_QUERIES
```

used by deep research.

Cache/reuse results within the episode.

If several fingerprints share the same context window, they must not trigger duplicate Loki calls.

---

# 16. Query by emitter during local enrichment

The local enrichment query should identify the emitter using the same identity established earlier:

```text
host
service
job/source
```

Use available Loki labels consistently with current repository conventions.

Be careful that not every stream necessarily exposes exactly the same service label.

Reuse existing escaping helpers.

Do not construct broad unbounded LogQL queries.

---

# 17. Derive fingerprint-local context from cached episode logs

After episode context has been fetched once, derive a compact context for each fingerprint without further Loki calls.

For example:

```python
class FingerprintLocalContext(BaseModel):
    representative_lines: list[str]
    nearby_templates: list[dict[str, object]]
    event_count: int
    warning_count: int
    info_count: int
    error_count: int
    related_fingerprints: list[str]
```

The precise schema may differ, but the goal is:

```text
one bounded episode enrichment
→ many fingerprint contexts
```

not:

```text
one Loki query sequence per fingerprint
```

Use deterministic summarization.

Do not ask another LLM to summarize surrounding logs.

---

# 18. Include info/warning context in stage 2

This is an explicit requirement.

The second Jev gate should see context such as:

```text
service starting
retrying connection
configuration reload
dependency unavailable
health check degrading
connection restored
service shutdown/restart
warning preceding error
informational recovery message after error
```

These messages can radically change whether an error fingerprint deserves deep investigation.

Do not broaden stage-1 candidate collection globally to achieve this.

Only fetch these lower-severity logs for episodes that survive stage 1.

---

# 19. Jev stage 2: fingerprint-level gate

For every fingerprint in an episode that survives stage 1:

Build state similar to:

```python
{
    "emitter": ...,
    "episode_summary": ...,
    "fingerprint": {
        "fingerprint": ...,
        "template": ...,
        "count_in_episode": ...,
        "levels": ...,
        "first_seen": ...,
        "last_seen": ...,
    },
    "local_context": ...,
}
```

Do not send the entire raw Loki payload.

Include bounded representative lines and deterministic context summaries.

The primary Score question should be conceptually:

```text
How strongly does this fingerprint warrant deep operational diagnosis,
given the service, episode and surrounding logs?
```

Use a concrete ordered rubric approximately like:

```text
0:
Expected/routine/transient behavior with no durable repair justified.

1:
Probably non-actionable operational noise; deeper repository/log/web
research is unlikely to produce a useful remediation.

2:
Potentially actionable failure; deeper investigation may identify a
durable code, configuration or operational fix.

3:
Strongly actionable failure or clear service-impacting symptom that
warrants deep diagnosis.
```

Again, use the actual Jev Score semantics rather than assuming an undocumented scale.

Routing is exclusively:

```python
if score >= FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD:
    candidate_for_deep_analysis = True
else:
    candidate_for_deep_analysis = False
```

Do not use confidence in the branch.

Do not use probability mass in the branch.

Retain both for telemetry.

---

# 20. Deduplicate deep candidates appropriately

The same fingerprint can occur in several episodes.

Represent the deep-analysis candidate as conceptually:

```python
FingerprintEpisodeCandidate(
    fingerprint=...,
    episode=...,
    triage_score=...,
    triage_probabilities=...,
    triage_confidence=...,
    local_context=...,
)
```

This allows the same stable fingerprint to receive episode-specific analysis where context materially differs.

Do not collapse all episode context before the second Jev gate.

---

# 21. Apply the existing global deep-analysis budget

Keep:

```python
MAX_DEEP_FINDINGS
```

as a global budget.

Do not make it per emitter.

Do not make it per episode.

After stage 2, rank selected deep candidates by the stage-2 investigation score.

Use deterministic tie-breaking, for example:

1. higher stage-2 score
2. higher severity level
3. higher episode count
4. stable fingerprint / timestamp ordering

Then take:

```python
deep_candidates[:MAX_DEEP_FINDINGS]
```

The stage-2 Jev score should therefore become the primary prioritization signal.

Do not prioritize only by raw event count.

A one-off severe failure must be able to outrank a high-volume harmless fingerprint.

---

# 22. Remove the old MAX_TRIAGE_FINDINGS behavior

Delete the current architecture where only the top 100 findings receive triage.

Every collected episode should receive cheap stage-1 Jev triage.

Every fingerprint in a selected episode should be eligible for stage-2 Jev triage.

The expensive budget belongs after the cheap stages:

```text
MAX_DEEP_FINDINGS
MAX_ADDITIONAL_LOG_QUERIES
repository evidence bounds
web evidence bounds
reasoning-model calls
```

Do not keep a count-based cap that prevents rare fingerprints from reaching Jev.

---

# 23. Keep deep diagnosis at fingerprint level

Do not switch the strong reasoning model to one diagnosis per episode.

Deep diagnosis remains fingerprint-level.

However, supply the episode context and local enrichment to the existing research planning and diagnosis stages.

The reasoning model should understand:

```text
this specific normalized failure symptom
within this specific service episode
with these related logs around it
```

rather than seeing the fingerprint in isolation.

---

# 24. Reuse local context in deep research

Do not re-query the same basic surrounding-log evidence after stage 2.

Pass the already collected episode/fingerprint local context into the deep-analysis evidence payload.

The existing `ResearchPlan` may still request up to:

```python
MAX_ADDITIONAL_LOG_QUERIES
```

when additional targeted evidence is genuinely needed.

Those research queries are conceptually different from local pre-triage enrichment.

Avoid duplicate queries whenever the requested evidence already exists in the cached local context.

---

# 25. Preserve existing repository research architecture

Keep the existing `RepositoryCorpus` concept and control-plane guidance.

The existing preference for:

`puppet-control-repo`

as the authoritative infrastructure control plane remains useful.

Do not rewrite repository synchronization or search architecture unless required by the data-model changes.

Continue to bound repository content sent to the reasoning model.

---

# 26. Preserve web research only for deep candidates

Keep Tavily/public web research behind the stage-2 gate and global deep-analysis budget.

No stage-1 or stage-2 Jev call should trigger web searches.

Web queries must continue to avoid leaking:

* private addresses
* hostnames
* credentials
* unique identifiers

Retain existing redaction behavior.

---

# 27. Keep strong-model ResearchPlan and Diagnosis

The strong OpenRouter model remains responsible for:

```text
ResearchPlan
Diagnosis
```

These are genuinely reasoning/generative tasks and should not move to Jev.

The final structured `Diagnosis` should continue to contain fields such as:

```text
impact
severity
remediation_kind
cause_status
confidence
analysis
repair_plan
affected_repositories
verification
```

Do not use Jev to generate repair plans or root-cause narratives.

---

# 28. Simplify remaining OpenRouter client code

The current OpenRouter wrapper contains triage-specific behavior.

Refactor it so the remaining code serves only deep reasoning stages.

Delete:

```text
stage.startswith("triage_batch")
triage reasoning configuration
triage completion-token branching
triage batch mode handling
```

Keep:

```text
structured-output compatibility
provider retry handling
safe normalized failures
ResearchPlan invocation
Diagnosis invocation
```

Do not retain branches that can no longer execute.

---

# 29. Reporting semantics for repeated fingerprints

The final report remains primarily fingerprint-oriented, not episode-oriented.

For each fingerprint, aggregate:

```text
total count across reporting window
trend
number of episodes
episode time ranges
emitters involved, normally stable
highest stage-2 score
```

Include concise episode summaries.

A useful presentation is conceptually:

```text
Fingerprint: database connection refused
Count: 184
Episodes: 3
Trend: recurring

Episodes:
- 2026-09-02 08:31–08:38 — score ...
- 2026-09-11 14:02–14:07 — score ...
- 2026-09-16 22:41–22:44 — score ...

Diagnosis:
...
```

Do not dump all raw local context into the email.

---

# 30. Handle materially different diagnoses

A fingerprint may receive deep analysis in more than one episode.

If multiple episode-specific diagnoses are materially equivalent, collapse them into one report diagnosis with multiple supporting episode references.

Use deterministic structured fields to determine equivalence.

At minimum, diagnoses should be considered materially different when one or more of these differ:

```text
severity
remediation_kind
cause_status
affected_repositories
```

Normalize repository ordering before comparison.

Do not use another LLM solely to determine whether two diagnoses are equivalent.

If materially different, render distinct episode-specific diagnosis subsections under the same fingerprint.

Do not create fake new fingerprints.

---

# 31. Trends remain fingerprint based

Keep trend comparison based on stable fingerprint identity.

`apply_trends()` should still answer questions such as:

```text
new
recurring
resolved
```

using the global fingerprint across reporting windows.

Episodes must not become cross-run trend keys.

A different episode of the same fingerprint should normally remain a recurring occurrence of that fingerprint.

---

# 32. Extend Finding or introduce a report model cleanly

Do not overload `Finding` with dozens of awkward optional internal fields if that makes the code difficult to reason about.

Either extend it cleanly or introduce internal models such as:

```python
EpisodeSummary
FingerprintEpisodeCandidate
FingerprintAnalysis
```

and convert to a report-oriented structure at the end.

Prefer explicit typed models over nested anonymous dictionaries where practical.

However, avoid a large unrelated domain-model rewrite.

---

# 33. Observability

Add logs/traces for the new funnel.

At run level record:

```text
candidate log occurrences
emitters
episodes
unique fingerprints

episodes sent to Jev stage 1
episodes skipped by stage 1
episodes enriched locally

local Loki enrichment queries executed

fingerprints sent to Jev stage 2
fingerprints skipped by stage 2
fingerprints selected by stage 2

deep candidates before MAX_DEEP_FINDINGS
deep candidates actually analyzed

ResearchPlan calls
Diagnosis calls
```

Also calculate useful reduction ratios:

```text
episode-stage reduction %
fingerprint-stage reduction %
deep-analysis reduction %
```

---

# 34. Record Jev decision telemetry

For every stage-1 result record at least:

```text
episode identifier
host
service
source
score
probabilities
confidence
routing threshold
selected yes/no
```

For every stage-2 result record:

```text
fingerprint
episode identifier
host
service
score
probabilities
confidence
routing threshold
selected yes/no
```

Do not log API secrets.

Do not emit huge state payloads in normal logs.

Where Phoenix/OpenTelemetry integration exists, add useful numeric attributes without creating high-cardinality raw-log attributes unnecessarily.

---

# 35. Jev failure behavior

Implement explicit, safe failure behavior for TypeSafe requests.

A Jev outage must not silently classify something as harmless.

For stage 1:

```text
if Jev evaluation fails:
    treat the episode as selected for local enrichment
```

For stage 2:

```text
if Jev evaluation fails:
    treat the fingerprint as selected for deep-analysis candidacy
```

This fallback is an availability/safety behavior, not score routing.

The normal successful routing path remains score-only.

Emit a warning when fail-open behavior occurs.

Do not route based on confidence.

Do not preserve the old OpenRouter triage as a fallback.

---

# 36. Query explosion safeguards

The final design must satisfy all of these:

```text
No surrounding-log queries before stage 1.

No per-fingerprint duplicate Loki context query pattern.

At most MAX_LOCAL_CONTEXT_QUERIES_PER_EPISODE normal
local-enrichment queries.

Overlapping local-context windows are deduplicated.

One episode's fetched context is reused across its fingerprints.

Stage 1 is moderately aggressive so clearly routine episodes never
reach Loki enrichment.

Stage 2 happens before repository/web/deep-reasoning work.

MAX_DEEP_FINDINGS remains global.
```

Add tests around query counts.

---

# 37. Sampling raw evidence

Continue to retain bounded representative evidence lines.

When choosing representative lines for an episode fingerprint, sample deterministically across its occurrence range rather than only the first or last burst.

The current code samples across the reporting window; preserve that useful property at the appropriate episode/global level.

Never use random evidence sampling.

---

# 38. Keep untrusted-data protections

All log lines, repository content, and external text remain untrusted evidence.

Preserve prompts/instructions that tell the reasoning model not to execute instructions embedded in logs or repository data.

Jev state should likewise contain data fields, not concatenated pseudo-instructions derived from logs.

Do not let a log line alter the Jev question/rubric.

---

# 39. Tests: episode construction

Add deterministic unit tests covering:

```text
same emitter, gaps < 10 minutes → same episode

same emitter, gap exactly 10 minutes → same episode

same emitter, gap > 10 minutes → new episode

different service → different emitter/episode

different job/source → different emitter/episode

different severity level → can remain same episode

out-of-order Loki rows → sorted correctly before episode construction
```

---

# 40. Tests: fingerprinting

Keep and update existing fingerprint tests.

Verify variable fields still collapse as intended.

Verify the same normalized fingerprint remains stable across episodes.

Verify episode creation does not modify fingerprint identity.

---

# 41. Tests: stage-1 Jev routing

Mock the TypeSafe client.

Test at least:

```text
score below threshold → no local Loki enrichment

score equal to threshold → episode selected

score above threshold → episode selected

high confidence vs low confidence with same score → identical routing

different probability distributions with same score → identical routing

Jev failure → episode selected fail-open
```

Also verify score/probabilities/confidence are retained.

---

# 42. Tests: local enrichment query bounds

Create an episode with many fingerprints.

Assert that enrichment does NOT issue one context query per fingerprint.

Test:

```text
one short episode → bounded context query count

long episode → at most 3 context queries

overlapping anchor windows → deduplicated

10+ fingerprints in one episode → still at most the episode query cap

stage-1-skipped episode → zero enrichment queries
```

Verify info/warning lines can appear in the derived local context even though initial collection targets error-level logs.

---

# 43. Tests: stage-2 Jev routing

Mock TypeSafe responses.

Test:

```text
score below threshold → no deep candidacy

score equal threshold → deep candidacy

score above threshold → deep candidacy

confidence does not change routing

probabilities do not change routing

Jev failure → deep candidacy fail-open
```

Verify the Jev input includes:

```text
emitter
episode summary
fingerprint summary
local surrounding-log context
```

and does not contain an unbounded raw Loki response.

---

# 44. Tests: deep-analysis budget

Construct more than `MAX_DEEP_FINDINGS` stage-2-selected candidates.

Verify:

* exactly `MAX_DEEP_FINDINGS` proceed to deep analysis
* ranking is primarily by stage-2 Jev score
* raw event count is not the sole ranking signal
* tie-breaking is deterministic

Keep `MAX_DEEP_FINDINGS` global.

---

# 45. Tests: no obsolete triage cap

Create more than 100 cheap candidates.

Verify they can all receive stage-1/stage-2 Jev evaluation as appropriate.

There must no longer be behavior equivalent to:

```text
only top MAX_TRIAGE_FINDINGS by count receive triage
```

---

# 46. Tests: deep analysis reuses local context

Verify an investigated fingerprint:

* receives already-fetched episode context
* does not repeat basic local context queries
* may still execute bounded `ResearchPlan.additional_log_queries`
* repository research remains bounded
* web research remains deep-stage only

---

# 47. Tests: repeated fingerprint across episodes

Construct one fingerprint appearing in several episodes.

Verify:

```text
same stable fingerprint
separate episode-specific stage-2 decisions
global count aggregated correctly
trend remains fingerprint based
episode summaries retained
```

For equivalent deep diagnoses, verify report aggregation.

For materially different structured diagnoses, verify the report renders separate episode-specific diagnosis subsections.

---

# 48. Tests: reporting

Update existing report tests to cover:

```text
fingerprint count
trend
episode count
episode ranges
selected diagnosis
multiple materially distinct diagnoses
warnings
global deep-analysis cap
```

Do not render every skipped fingerprint/episode in verbose detail.

The final email should remain operationally useful and bounded.

---

# 49. Delete obsolete code and tests

After implementation, search for old architecture remnants.

Delete unused:

```text
TriageDecision
TriageBatch
MAX_TRIAGE_FINDINGS
TRIAGE_BATCH_SIZE
TRIAGE_MAX_COMPLETION_TOKENS
triage_reasoning_effort handling
old triage prompt
old batch cardinality validator
triage-specific OpenRouter client branches
obsolete mocks
obsolete fixtures
unused imports
```

Do not comment them out.

Do not leave "legacy" helpers.

---

# 50. Keep unrelated functionality stable

Avoid unrelated rewrites of:

```text
repository synchronization
email transport
watermark/state storage
Tavily search mechanics
OpenRouter diagnosis schema
control-plane repository guidance
existing log exclusions
```

Change them only where necessary to integrate the new data flow.

---

# 51. Suggested high-level implementation shape

The final code should read conceptually like:

```python
occurrences = collect_operational_occurrences(start, end)

global_findings = aggregate_findings(occurrences)

episodes = build_operational_episodes(
    occurrences,
    gap=EPISODE_GAP,
)

deep_candidates = []
warnings = []

for episode in episodes:
    episode_triage = triage_episode_with_jev(episode)

    record_episode_triage(episode, episode_triage)

    if episode_triage.score < EPISODE_INVESTIGATION_SCORE_THRESHOLD:
        continue

    try:
        episode_context = enrich_episode_context(episode)
    except Exception:
        episode_context = empty_or_partial_context(...)
        warnings.append(...)

    for episode_fingerprint in episode.fingerprints:
        local_context = derive_fingerprint_context(
            episode,
            episode_fingerprint,
            episode_context,
        )

        fingerprint_triage = triage_fingerprint_with_jev(
            episode,
            episode_fingerprint,
            local_context,
        )

        record_fingerprint_triage(...)

        if (
            fingerprint_triage.score
            < FINGERPRINT_INVESTIGATION_SCORE_THRESHOLD
        ):
            continue

        deep_candidates.append(
            FingerprintEpisodeCandidate(...)
        )

deep_candidates = rank_candidates(deep_candidates)

for candidate in deep_candidates[:MAX_DEEP_FINDINGS]:
    research_plan = create_research_plan(
        candidate,
        existing_local_context=candidate.local_context,
        ...
    )

    research_evidence = execute_bounded_research(...)

    diagnosis = diagnose_fingerprint(
        candidate,
        research_evidence,
        ...
    )

    attach_episode_diagnosis(...)

apply_trends(global_findings, previous_state)

report = aggregate_episode_results_by_fingerprint(...)

render_report(...)
```

If existing function boundaries make a slightly different organization cleaner, use judgment, but preserve the architecture.

---

# 52. Expected helper responsibilities

Aim for clear functions with responsibilities approximately like:

```python
fingerprint_copy(...)
fingerprint(...)

collect_operational_occurrences(...)

aggregate_findings(...)

build_operational_episodes(...)

build_episode_jev_state(...)
triage_episode_with_jev(...)

select_context_windows(...)
query_episode_context(...)
enrich_episode_context(...)

derive_fingerprint_context(...)

build_fingerprint_jev_state(...)
triage_fingerprint_with_jev(...)

rank_deep_candidates(...)

execute_research_plan(...)
diagnose_fingerprint(...)

aggregate_diagnoses_by_fingerprint(...)

apply_trends(...)
render_report(...)
```

Do not create unnecessary classes or a framework where straightforward functions are clearer.

---

# 53. Acceptance criteria

The refactor is complete only when all of the following are true:

1. Operational candidate logs are still deterministically fingerprinted.

2. Emitter identity is exactly based on host + service + job/source.

3. Episodes split after more than 10 minutes without a candidate event.

4. The first Jev gate operates on compact episode summaries before extra Loki enrichment.

5. Stage 1 is allowed to discard clearly routine episodes and therefore prevents unnecessary local context queries.

6. No old OpenRouter triage remains.

7. `MAX_TRIAGE_FINDINGS` no longer limits cheap classification coverage.

8. Stage-1 Jev results retain score, probabilities and confidence.

9. Stage-1 routing uses score only.

10. Selected episodes receive bounded local Loki enrichment including info/warning context.

11. Local enrichment performs at most three context queries per episode.

12. Overlapping context windows are deduplicated.

13. Context fetched for an episode is reused across its fingerprints.

14. There is no one-Loki-query-per-fingerprint enrichment pattern.

15. Stage-2 Jev operates at fingerprint level with episode and local-log context.

16. Stage-2 results retain score, probabilities and confidence.

17. Stage-2 routing uses score only.

18. Jev confidence and probability distributions do not affect v1 routing.

19. Jev request failure fails open toward further investigation rather than silently skipping.

20. The same stable fingerprint may be analyzed separately in multiple episodes.

21. Deep diagnosis remains fingerprint-level rather than episode-level.

22. Deep candidates are prioritized primarily by stage-2 score.

23. `MAX_DEEP_FINDINGS` remains a global cap.

24. Repository, additional Loki and web research occur only for deep candidates.

25. Existing local context is reused during deep analysis.

26. ResearchPlan and Diagnosis continue to use the strong reasoning model.

27. Trend tracking remains based on stable fingerprint identity.

28. Final reporting aggregates primarily by fingerprint and includes concise episode summaries.

29. Materially different episode diagnoses remain separately visible under the same fingerprint.

30. No shadow mode, old/new feature flag, legacy path or dead migration code remains.

31. Tests cover episode formation, both Jev gates, score-only routing, probability/confidence retention, query bounds, repeated fingerprints, deep-analysis budgeting and reporting.

32. The repository formatter, linter, type checker and complete relevant test suite pass.

---

# 54. Implementation discipline

Before editing, inspect the current package configuration, workspace dependency conventions and existing tests.

Use the current official TypeSafe Python SDK rather than guessing its API.

Keep the diff focused on this architecture.

When finished:

1. run formatting
2. run lint/type checks used by the repository
3. run `tests/test_operations_analyst.py`
4. run any broader relevant repository test suite
5. remove dead imports/code exposed by the refactor
6. summarize the final architecture and any threshold constants chosen

Do not stop after scaffolding. Implement the complete cutover, tests included.
