# Implementation Plan: Jev-First `cyber_analyst` Architecture

## Goal

Replace the current single-model cyber-analysis flow with a clean three-stage architecture:

```text
Suricata alerts
    ↓
normalize + group by signature
    ↓
Jev signature gate
    ├── ignore
    └── investigate / uncertain
            ↓
       rehydrate individual alerts
            ↓
       local enrichment
       ├── source identity
       ├── destination identity
       └── summarized Suricata context
            ↓
       Jev alert gate
       ├── skip
       └── investigate / uncertain
                ↓
          external enrichment
          ├── GreyNoise / AbuseIPDB
          └── CVE lookup
                ↓
          strong reasoning model
                ↓
          AlertFinding
```

This is a direct replacement of the existing architecture.

Do **not**:

* keep the old LLM-based signature triage
* add shadow mode
* add feature flags for old vs. new behavior
* run both systems in parallel
* preserve compatibility helpers that are no longer used
* leave commented-out old code
* retain dead abstractions solely for rollback convenience

The repository should contain only the new implementation after the change.

---

# 1. Keep the top-level workflow shape

Retain the existing top-level functions:

```python
collect_signatures(...)
analyze_signatures(...)
render_email_report(...)
run(...)
```

Do not redesign the surrounding scheduler/report flow unnecessarily.

The main refactor belongs inside `analyze_signatures()` and supporting helpers.

---

# 2. Introduce two distinct model roles

Replace the current shared `ChatOpenAI` usage with two clearly independent clients:

```python
triage_client
reasoning_llm
```

Their responsibilities must be explicit.

## `triage_client`

Use TypeSafe/Jev.

Responsibilities:

* signature-level routing
* alert-level routing

It must never produce the final security verdict.

## `reasoning_llm`

Use the current strong reasoning provider/model or the configured replacement.

Responsibilities:

* analyze fully enriched alerts
* produce `AlertFinding`
* provide evidence
* provide recommended follow-up

Do not use this model for routing decisions.

---

# 3. Remove the old signature triage implementation

Delete the current implementation of:

```python
decide_signature(...)
```

that calls:

```python
llm.with_structured_output(SignatureDecision)
```

Do not retain it under another name.

Replace it with a Jev-backed function, for example:

```python
def triage_signature(
    signature_group: dict[str, object],
) -> SignatureTriageResult:
    ...
```

---

# 4. Define explicit routing models

Create typed application-level result models.

For example:

```python
class SignatureTriageResult(BaseModel):
    decision: Literal[
        "ignore_background_noise",
        "investigate",
    ]
    security_relevance_probability: float
    background_noise_probability: float
    confidence: float
    reasoning_summary: str | None = None
```

And:

```python
class AlertTriageResult(BaseModel):
    decision: Literal[
        "skip",
        "investigate",
    ]
    security_relevance_probability: float
    confidence: float
    reasoning_summary: str | None = None
```

If Jev exposes a richer native result type, normalize that into these application models at the provider boundary.

Keep provider-specific response shapes out of the rest of the workflow.

---

# 5. Signature-level Jev gate

The first Jev stage operates on the existing compact signature representation.

Input should include only fields useful for signature-level routing:

```python
{
    "signature": ...,
    "alert_count": ...,
    "categories": ...,
    "signature_ids": ...,
    "representative_categories": ...,
    "first_seen": ...,
    "last_seen": ...,
}
```

Do not rehydrate alerts before this step.

Use multiple atomic Jev judgments rather than one large free-form prompt.

Conceptually evaluate questions such as:

```text
Is this signature consistent with routine background network noise?

Could ignoring all alerts belonging to this signature conceal a security-relevant event?

Does this signature warrant inspection of its individual alerts?
```

Compose the results in normal Python.

The routing policy must favor recall.

Example policy:

```python
investigate = (
    security_relevance_probability >= SIGNATURE_INVESTIGATE_THRESHOLD
    or background_noise_probability < SIGNATURE_BACKGROUND_THRESHOLD
    or confidence < SIGNATURE_MIN_CONFIDENCE
)
```

Low confidence must route to investigation.

Do not rely on an LLM-generated textual recommendation to control routing.

---

# 6. Separate report skipping from suppression

Do not treat:

```text
not worth investigating this month
```

as equivalent to:

```text
safe to suppress permanently
```

These are different decisions.

The signature gate may decide:

```python
ignore_background_noise
```

for the current report.

Suppression candidates should only be generated under stricter conditions.

If the current implementation has no robust basis for that stricter decision, prefer to remove automatic suppression-candidate generation rather than imply that every skipped signature is safe to suppress.

If suppression candidates remain, introduce a separate explicit result such as:

```python
suppression_recommended: bool
suppression_confidence: float
```

with a significantly higher threshold than the report-investigation threshold.

Do not infer suppression directly from `ignore_background_noise`.

---

# 7. Rehydrate only signatures that pass stage 1

Keep the existing `rehydrate_signature_group()` behavior.

Only call it for signatures routed to investigation.

The rehydrated result should contain the individual normalized alerts required for stage 2.

---

# 8. Split enrichment into local and external phases

Refactor the current `analyze_one_alert()` so enrichment is no longer performed inside one monolithic function.

Create two explicit layers.

## Local enrichment

Create:

```python
def enrich_alert_locally(
    alert_row: dict[str, object],
) -> LocalAlertContext:
    ...
```

This must include:

```python
src_identity = resolve_network_identity(...)
dest_identity = resolve_network_identity(...)
suricata_context = query_suricata_context(...)
```

These calls are local and cheap enough to perform before the second Jev decision.

They should be used to give Jev substantially better context before deciding whether an expensive investigation is justified.

---

# 9. Summarize Suricata context before passing it to Jev

Do not pass the raw Loki result containing up to hundreds of lines directly into Jev.

Create a deterministic summarizer, for example:

```python
def summarize_suricata_context(
    alert_row: dict[str, object],
    payload: dict[str, object],
) -> SuricataContextSummary:
    ...
```

Include useful aggregate features such as:

```python
class SuricataContextSummary(BaseModel):
    event_count: int
    same_signature_count: int
    distinct_src_ips: int
    distinct_dest_ips: int
    distinct_dest_ports: list[int]
    distinct_protocols: list[str]
    related_signatures: list[str]
    first_event_at: str | None
    last_event_at: str | None
```

Add other high-value deterministic features where straightforward.

Examples:

```text
number of alerts involving the same source
number involving the same destination
number of unique destination ports
whether activity looks like fan-out / scanning
whether the same signature repeats rapidly
whether other alert signatures occur around the same time
```

Keep this summarization deterministic.

The objective is to give Jev **signal**, not a large raw token payload.

---

# 10. Normalize identity context for stage 2

Likewise, do not expose unnecessary raw lookup material.

Build compact identity summaries.

For example:

```python
class AssetIdentitySummary(BaseModel):
    ip: str | None
    hostname: str | None
    mac_address: str | None
    mac_vendor: str | None
    lookup_path: str | None
    resolved: bool
```

The alert-level Jev input should look conceptually like:

```python
{
    "alert": {
        "signature": ...,
        "signature_id": ...,
        "category": ...,
        "src_ip": ...,
        "dest_ip": ...,
        "src_port": ...,
        "dest_port": ...,
        "protocol": ...,
        "observed_at": ...,
    },
    "src_identity": ...,
    "dest_identity": ...,
    "suricata_context": ...,
}
```

---

# 11. Add the alert-level Jev gate

Create:

```python
def triage_alert(
    alert_row: dict[str, object],
    local_context: LocalAlertContext,
) -> AlertTriageResult:
    ...
```

Again, use atomic Jev judgments.

Useful questions include:

```text
Is the observed activity unexpected for the identified source asset?

Does the surrounding Suricata activity make this alert more security-relevant?

Could skipping further investigation conceal suspicious behavior?

Is there enough evidence here to justify deeper investigation?
```

Compose the results deterministically.

Example routing rule:

```python
investigate = (
    security_relevance_probability >= ALERT_INVESTIGATE_THRESHOLD
    or confidence < ALERT_MIN_CONFIDENCE
)
```

Any low-confidence result must route to investigation.

Do not let Jev produce the final:

```text
proof_of_malware
suspicious_monitor
false_positive_monitor
```

verdict.

---

# 12. Perform external enrichment only after stage 2

For alerts that pass the Jev alert gate, perform external lookups.

Create:

```python
def enrich_alert_externally(
    alert_row: dict[str, object],
) -> ExternalAlertContext:
    ...
```

Move into this function:

```python
threat_intel = check_threat_intel(...)
cve_context = search_cve(...)
```

This keeps external latency, rate limiting, and API failure modes away from alerts that have already been filtered out.

---

# 13. Keep optional CVE lookup optimization simple

It is acceptable to pull CVE context forward into stage 2 when the signature clearly identifies a vulnerability or exploit.

Implement this only if it can be done deterministically and cleanly.

For example:

```python
def signature_looks_cve_related(signature: str) -> bool:
    ...
```

Then:

```python
if signature_looks_cve_related(alert_row["signature"]):
    local_context.cve_hint = search_cve(...)
```

Do not introduce another LLM decision just to decide whether to look up a CVE.

If this complicates the implementation materially, leave all CVE lookup after stage 2 for the first version.

---

# 14. Refactor final reasoning into a dedicated function

Extract the current final reasoning call from `analyze_one_alert()`.

Create:

```python
def analyze_enriched_alert(
    alert_row: dict[str, object],
    local_context: LocalAlertContext,
    external_context: ExternalAlertContext,
) -> AlertFinding:
    ...
```

This function should invoke only the strong reasoning model.

Its prompt should contain:

```text
original normalized alert
source identity summary
destination identity summary
summarized Suricata context
GreyNoise / AbuseIPDB information
CVE context
lookup warnings
```

Retain the existing `AlertFinding` schema unless there is a concrete reason to change it.

The reasoning model should continue to produce:

```python
verdict: Literal[
    "proof_of_malware",
    "suspicious_monitor",
    "false_positive_monitor",
]
evidence: str
recommended_follow_up: str
```

---

# 15. Do not duplicate local enrichment

The local enrichment performed before Jev stage 2 must be reused by the final reasoning stage.

Do not query DHCP/NDP/Loki a second time after the alert passes triage.

The data flow should be:

```text
local enrichment
      ↓
alert Jev gate
      ↓
same local context object
      ↓
final reasoning
```

---

# 16. Update LangGraph around the new flow

The graph should represent the new architecture directly.

A suitable shape is:

```text
START
  ↓
signature_triage
  ├── ignore → END
  ↓
rehydrate
  ↓
analyze_alerts
  ↓
END
```

It is not necessary to create one graph node per helper if doing so makes the graph harder to understand.

Inside `analyze_alerts`, each alert should follow:

```text
local enrichment
    ↓
Jev alert triage
    ├── skip
    └── external enrichment
            ↓
       strong-model analysis
```

Prefer simple control flow over excessive LangGraph abstraction.

---

# 17. Update state models

Replace the existing state shape as necessary so the names reflect the new architecture.

For example:

```python
class SignatureAgentState(TypedDict, total=False):
    signature_group: dict[str, Any]
    signature_triage: dict[str, Any]
    per_alert_results: list[dict[str, Any]]
    unresolved_lookups: list[str]
```

Do not retain legacy state fields solely for compatibility with deleted code.

---

# 18. Preserve useful lookup warnings

Continue collecting warnings from:

```text
identity resolution
GreyNoise
AbuseIPDB
CVE lookup
```

Attach relevant warnings to the final `AlertFinding`.

For alerts skipped by Jev stage 2, external lookup warnings obviously do not exist and should not be fabricated.

---

# 19. Add observability for routing and cost reduction

Extend the existing tracing/logging.

Record at minimum:

## Signature level

```text
signature
alert_count
Jev decision
security relevance probability
confidence
```

## Alert level

```text
signature
signature_id
src_ip
dest_ip
Jev decision
security relevance probability
confidence
```

## Run-level counters

Emit:

```text
total signatures
signatures skipped by Jev
signatures rehydrated

total alerts in rehydrated signatures
alerts skipped by Jev
alerts sent to external enrichment
alerts sent to reasoning model
```

Also emit derived percentages where convenient:

```text
signature reduction %
alert reduction %
reasoning-call reduction %
```

Do not log huge raw Jev payloads or raw 200-line Loki contexts.

---

# 20. Configuration

Add explicit configuration for the Jev provider/client.

Keep model/provider configuration separate from the reasoning model.

Use Vault/environment configuration consistent with the repository's existing conventions.

For example:

```text
cyber_analyst_triage
cyber_analyst_openrouter
```

or equivalent clear naming.

Required Jev settings should include whatever the TypeSafe SDK needs, such as:

```text
API endpoint
API key/token
model/config identifier if applicable
```

Add routing thresholds as normal application constants/configuration.

For example:

```python
SIGNATURE_INVESTIGATE_THRESHOLD = ...
SIGNATURE_BACKGROUND_THRESHOLD = ...
SIGNATURE_MIN_CONFIDENCE = ...

ALERT_INVESTIGATE_THRESHOLD = ...
ALERT_MIN_CONFIDENCE = ...
```

Keep thresholds centralized rather than scattering numeric literals through routing code.

---

# 21. Conservative initial thresholds

Initial configuration should favor security recall over cost savings.

In particular:

```text
uncertain → investigate
low confidence → investigate
borderline security relevance → investigate
```

Do not aggressively tune for maximum filtering in the first implementation.

The architecture should make future threshold adjustment trivial.

---

# 22. Tests

Update the existing cyber analyst tests rather than retaining tests for deleted behavior.

Remove tests whose only purpose is validating the old ChatOpenAI signature classifier.

Add tests for the new architecture.

## Signature triage

Test:

```text
obvious background noise → skip
security-relevant signature → investigate
low-confidence result → investigate
borderline result → conservative investigation
```

Mock Jev responses.

Do not require network access.

## Local enrichment

Test:

```text
source identity is resolved
destination identity is resolved
missing identity remains valid
Suricata context is summarized correctly
raw context is not needed downstream
```

## Suricata summarization

Test deterministic cases including:

```text
single event
repeated identical alerts
multiple destination ports
multiple destinations
related signatures
empty context
```

## Alert triage

Test:

```text
clear noise → skip
interesting local context → investigate
low confidence → investigate
```

## External enrichment

Verify that:

```text
skipped alerts do not call GreyNoise
skipped alerts do not call AbuseIPDB
skipped alerts do not call Tavily/CVE search
```

## Final reasoning

Verify that an investigated alert receives:

```text
alert
local context
external context
```

and yields the existing `AlertFinding` structure.

## No duplicate local lookups

Add a test proving that an alert which passes stage 2 does not repeat:

```text
resolve_network_identity
query_suricata_context
```

during final reasoning.

---

# 23. End-to-end workflow tests

Add or update an integration-style test using mocked providers.

Scenario:

```text
3 signatures
```

where:

```text
signature A
    → stage 1 skip

signature B
    → stage 1 investigate
    → 3 alerts
       → 2 stage 2 skips
       → 1 full investigation

signature C
    → stage 1 investigate
    → 1 alert
       → low-confidence stage 2
       → full investigation
```

Assert:

```text
only B and C are rehydrated

local enrichment runs for all rehydrated alerts

external enrichment runs for only 2 alerts

reasoning model runs exactly twice

report contains the expected signature decisions and findings
```

---

# 24. Delete obsolete code

After the refactor, explicitly search the package for unused pieces introduced by the old architecture.

Delete:

```text
old LLM signature decision prompt
old shared triage/reasoning model variable
unused SignatureDecision fields
unused helper functions
unused imports
unused test fixtures
obsolete configuration
```

Run the repository's formatter, linter, type checker and tests.

The final diff must not leave dead compatibility code.

---

# 25. Reporting

Keep the monthly plaintext report conceptually compatible, but adapt it to the new routing structure.

For skipped signatures, report the signature-level decision.

For investigated signatures, report meaningful per-alert findings.

Do not fill the report with every stage-2 skipped alert unless existing report semantics require it.

Prefer aggregate information such as:

```text
Signature: ...
Alerts observed: 128
Alerts selected for deep investigation: 3
Alerts filtered after local-context triage: 125
```

Then show the three actual findings.

This makes the cost-saving behavior visible without producing a verbose report.

---

# 26. Expected final structure

The code should conceptually end up with helpers along these lines:

```python
def triage_signature(...):
    ...

def rehydrate_signature_group(...):
    ...

def resolve_network_identity(...):
    ...

def query_suricata_context(...):
    ...

def summarize_suricata_context(...):
    ...

def enrich_alert_locally(...):
    ...

def triage_alert(...):
    ...

def check_threat_intel(...):
    ...

def search_cve(...):
    ...

def enrich_alert_externally(...):
    ...

def analyze_enriched_alert(...):
    ...
```

The main per-signature execution should read approximately like:

```python
signature_triage = triage_signature(signature_group)

if signature_triage.decision == "ignore_background_noise":
    return skipped_signature_result(...)

rehydrated = rehydrate_signature_group(signature_group)

findings = []

for alert in rehydrated["alerts"]:
    local_context = enrich_alert_locally(alert)

    alert_triage = triage_alert(
        alert,
        local_context,
    )

    if alert_triage.decision == "skip":
        continue

    external_context = enrich_alert_externally(alert)

    finding = analyze_enriched_alert(
        alert,
        local_context,
        external_context,
    )

    findings.append(finding)
```

Prefer this straightforward structure unless LangGraph materially improves readability.

---

# 27. Acceptance criteria

The implementation is complete when all of the following are true:

1. The old LLM-based signature triage no longer exists.

2. Jev performs all signature-level routing.

3. Only signatures selected by Jev are rehydrated.

4. Every alert belonging to a selected signature receives local enrichment before its alert-level routing decision.

5. The alert-level Jev decision sees:

   * normalized alert fields
   * source identity
   * destination identity
   * summarized Suricata context

6. Raw Suricata context is not dumped wholesale into Jev.

7. Alerts skipped by stage 2 perform no GreyNoise, AbuseIPDB, CVE-search or reasoning-model calls.

8. Alerts selected by stage 2 receive external enrichment and strong-model analysis.

9. Local enrichment is not repeated after stage 2.

10. Low-confidence Jev decisions route toward investigation.

11. Jev never generates the final security verdict.

12. The reasoning model no longer performs routing.

13. No shadow mode, legacy path, feature flag, fallback implementation or dead migration code remains.

14. Tests cover both routing stages and verify that expensive calls are avoided for filtered alerts.

15. Logs/traces expose enough counters to measure how many signatures and alerts reach each stage.

16. Existing report generation continues to work with the new analysis result structure.

17. The full repository test/lint/type-check suite passes.

---

# Implementation principle

Keep the architecture asymmetric:

```text
cheap evidence collection
        +
cheap probabilistic routing
        ↓
expensive investigation only when justified
```

Jev should answer narrow routing questions from good evidence.

Python should make the routing decision.

The strong reasoning model should only analyze alerts that survive both filters.

Do not preserve the previous architecture in parallel.
