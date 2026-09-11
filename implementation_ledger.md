# Operations Analyst LLM Reliability Implementation Ledger

This ledger converts the production review of the operations analyst's OpenRouter/LangChain structured-output fix into implementation-ready work. Complete tasks in dependency order unless a task explicitly says it may be done independently.

## Non-negotiable behavior

- Preserve every collected `Finding` and its original evidence. An LLM failure must never silently turn a finding into a successful diagnosis or remove it from the report.
- When model analysis cannot be completed after the allowed retries, set the affected finding to `classification="unclear"`, add a user-visible warning, and continue producing the weekly report.
- Fail the service for permanent configuration or programming errors such as invalid credentials, authorization failures, an invalid locally generated schema, or an unsupported local configuration. A report that is degraded solely because the deployment is broken must not appear successful.
- Keep triage batching and all existing evidence-size bounds. Do not increase model input or output limits as part of this work.
- Continue supporting `deepseek/deepseek-v4.1-flash`, including a route that works when its selected OpenRouter provider does not support JSON Schema structured output.
- Never log prompts, raw model responses, credentials, repository contents, or raw log evidence.
- Use the existing uv workspace and dependencies. Add a dependency only if the installed OpenAI, LangChain, LangChain OpenAI, Pydantic, and standard-library APIs cannot solve the task.

## Task 1 — Add native and compatibility structured-output modes

**Priority:** High
**Depends on:** None
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

`ChatOpenAI.with_structured_output()` uses JSON Schema and therefore sends `response_format.type=json_schema`. `provider.require_parameters=true` correctly filters OpenRouter routing to endpoints that support every submitted parameter, but it cannot make an unsupported endpoint support structured output. If no eligible endpoint exists, the current pipeline fails instead of using DeepSeek in a compatibility mode.

### Implementation specification

1. Refactor the nested structured invocation logic into a small, typed helper that supports these modes:
   - `json_schema`: call `with_structured_output(schema, method="json_schema", strict=True)` and retain `provider.require_parameters=true`.
   - `prompt_json`: call the ordinary chat model without `with_structured_output()` and without any `response_format` parameter. Append an explicit instruction containing the schema's JSON Schema and require exactly one JSON object with no Markdown fence or commentary. Extract text from the returned `AIMessage`, decode it with `json.loads`, and validate it using `schema.model_validate(...)`.
2. Choose the initial mode from the OpenRouter connection's `extra` mapping. Add one optional key named `structured_output_mode` with accepted values `auto`, `json_schema`, and `prompt_json`; default to `auto`.
3. Mode behavior:
   - `json_schema`: only use native JSON Schema and treat provider/model incompatibility as a permanent configuration failure.
   - `prompt_json`: never send `response_format` or `provider.require_parameters`; perform local Pydantic validation.
   - `auto`: try native JSON Schema first. Fall back once to `prompt_json` only when OpenRouter/OpenAI reports that the model or all eligible providers do not support `response_format`, JSON Schema, or another required request parameter. Do not fall back for authentication, authorization, malformed local schema, timeout, rate-limit, generic 5xx, refusal, or arbitrary parser errors.
4. Build separate `ChatOpenAI` clients for native and compatibility requests so a native client's `extra_body` cannot leak `provider.require_parameters` into compatibility calls. Preserve base URL, API key, model, temperature, timeout, token limits, and the stage-appropriate reasoning setting.
5. Keep Pydantic as the final trust boundary in both modes. Return only a validated instance of the requested schema.
6. Do not fetch model capabilities from OpenRouter on every invocation. The explicit connection mode plus narrow unsupported-parameter fallback is sufficient for this patch.

### Tests and acceptance criteria

- Native mode calls `with_structured_output(..., method="json_schema", strict=True)`.
- Compatibility mode does not call `with_structured_output()` and successfully validates a JSON text response into each requested Pydantic schema.
- Compatibility mode rejects invalid JSON and schema-invalid JSON through the common recoverable parsing path implemented in Task 2.
- `auto` falls back exactly once for a simulated unsupported-parameter/provider error and succeeds with prompt JSON.
- `auto` does not fall back for 401, 403, 429, timeout, or arbitrary `TypeError` failures.
- A test proves `deepseek/deepseek-v4.1-flash` can be configured with `structured_output_mode="prompt_json"` and complete triage without native structured output.

## Task 2 — Introduce an explicit LLM failure taxonomy

**Priority:** High
**Depends on:** Task 1
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

Only `LengthFinishReasonError` and one exact `TypeError` become unresolved findings. Empty responses, invalid JSON, Pydantic validation failures, refusals, content filtering, transport failures, and OpenRouter API errors can still terminate the weekly job without an intentional policy decision.

### Implementation specification

1. Replace the single generic `StructuredOutputError` usage with a small internal exception hierarchy or a typed error carrying a stable reason enum. It must distinguish at least:
   - `truncated`
   - `empty_or_malformed_response`
   - `invalid_json`
   - `schema_validation_failed`
   - `refusal`
   - `content_filtered`
   - `transient_provider_failure`
   - `unsupported_structured_output`
2. Normalize known exceptions from the installed OpenAI, LangChain OpenAI, LangChain Core, JSON, and Pydantic versions. Inspect the installed classes rather than guessing import paths. Include:
   - `LengthFinishReasonError`
   - `ContentFilterFinishReasonError`
   - LangChain's structured-output refusal error
   - JSON decoding/output-parser errors
   - Pydantic `ValidationError`
   - OpenAI connection, timeout, rate-limit, and API-status exceptions
3. Treat `choices is None`, `choices=[]`, a missing parsed value, empty message content in compatibility mode, and otherwise unusable successful HTTP responses as `empty_or_malformed_response`.
4. Stage policy after Task 3 retries are exhausted:
   - The recoverable categories above mark the affected triage batch, research plan, or diagnosis unresolved and allow the report to continue.
   - Authentication/authorization errors, invalid local request/schema errors, unknown exception types, and programming errors must propagate and fail the service.
5. Ensure every warning uses a stable human-readable reason without embedding provider response bodies.

### Tests and acceptance criteria

- Parameterize tests across malformed JSON, empty content, absent parsed output, refusal, content filtering, timeout, rate limit, and a representative 5xx.
- Assert each recoverable failure produces an unresolved finding and a reason-specific warning after retry exhaustion.
- Assert 401, 403, invalid local schema/request, and an unexpected exception still escape.
- Assert no test warning contains prompt text, API keys, or raw response content.

## Task 3 — Add bounded retries for transient failures

**Priority:** High
**Depends on:** Task 2
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

Both clients set `max_retries=0`, and there is no application retry. One timeout, connection interruption, 408, 409, 429, or 5xx response can fail or degrade an entire stage.

### Implementation specification

1. Keep OpenAI SDK retries disabled so retry count and logging remain controlled in one place.
2. Add an application-level retry loop around one logical model invocation:
   - Maximum three total attempts.
   - Retry connection failures, timeouts, HTTP 408, HTTP 409, HTTP 429, and HTTP 5xx.
   - Honor a valid `Retry-After` header, capped at 60 seconds.
   - Otherwise use exponential backoff with jitter and a maximum delay of 30 seconds.
   - Do not retry truncation, invalid JSON, schema-validation failure, refusal, content filtering, 4xx errors other than 408/409/429, or programming errors.
3. Make sleeping injectable or patchable so tests do not wait in real time.
4. A retry must repeat only the LLM request. It must not repeat Loki queries, repository reads, or web searches.
5. After exhaustion, raise the normalized `transient_provider_failure` so the calling stage applies the unresolved policy.

### Tests and acceptance criteria

- A timeout followed by success makes two calls and returns the validated result.
- Three rate-limit or 5xx responses make exactly three calls, then leave the stage unresolved.
- A valid `Retry-After` value is honored within the cap; invalid or excessive values use/cap the calculated delay safely.
- A 401 and a validation error make one attempt.
- Tests patch sleep and randomness and remain deterministic.

## Task 4 — Make the original `choices=None` guard provenance-safe

**Priority:** Medium
**Depends on:** Task 2
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

Matching only `str(exc) == "'NoneType' object is not iterable"` can hide an unrelated bug from callbacks, tracing, LangChain, or local code. It is also fragile if the SDK changes the error text.

### Implementation specification

1. Prefer detecting invalid raw response shape before generic downstream parsing where the installed APIs permit it.
2. If the OpenAI SDK still throws the bare `TypeError` before a raw response is exposed, normalize it only when both conditions hold:
   - The message exactly matches the known production error.
   - The traceback contains the OpenAI chat-completion parsing frame, specifically `openai/lib/_parsing/_completions.py` in `parse_chat_completion` or the installed equivalent.
3. Convert only that provenance-checked error to `empty_or_malformed_response`. Re-raise all other `TypeError`s unchanged.
4. Keep the original exception as `__cause__` for diagnostics.

### Tests and acceptance criteria

- A realistic SDK parsing path for `choices:null` is normalized.
- A locally raised `TypeError` with the same text but no OpenAI parser frame propagates.
- A different `TypeError` propagates.

## Task 5 — Add a real-client HTTP regression test

**Priority:** Medium
**Depends on:** Tasks 1, 2, and 4
**Primary files:** `tests/test_operations_analyst.py`

### Problem

The current fake client proves the outer catch but does not exercise LangChain, the OpenAI parser, or the serialized OpenRouter request body.

### Implementation specification

1. Add a test using the real installed `ChatOpenAI` and OpenAI SDK with a mocked HTTP transport. Do not make a network request and do not consume OpenRouter credits.
2. Capture the outgoing native-mode request JSON and assert:
   - `provider.require_parameters` is top-level and `true`.
   - `response_format.type` is `json_schema`.
   - The expected model, token limit, and stage-specific reasoning setting are present.
   - No credential appears in logged output.
3. Return an OpenAI-compatible HTTP 200 body with `choices: null` matching the production incident. Assert the finding remains unresolved and the warning uses `empty_or_malformed_response` semantics.
4. Add equivalent small stage tests proving research-plan and diagnosis failures are caught. It is acceptable to use focused fakes for these two tests after the HTTP boundary is covered once.
5. Retain a test that an unrelated `TypeError` fails loudly.

### Tests and acceptance criteria

- The test fails if LangChain stops passing `extra_body.provider` into the OpenRouter request.
- The test fails if native structured output stops adding JSON Schema `response_format`.
- The test exercises the production parser stack instead of manually raising the target exception.

## Task 6 — Validate triage response cardinality and identity

**Priority:** Medium
**Depends on:** Task 2
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

The triage model may omit fingerprints, duplicate them, or return fingerprints not present in the current batch. The code silently accepts partial output, leaving some findings unresolved without explaining why.

### Implementation specification

1. After Pydantic validation, compare decision fingerprints with the exact fingerprint set sent in that batch.
2. Treat duplicate, missing, and unexpected fingerprints as an invalid triage result for the whole batch. Do not apply a partially valid set of decisions because this makes the result dependent on response order and validation timing.
3. Leave every finding in the invalid batch as `unclear` and append a warning that reports counts only: number missing, duplicated, and unexpected. Do not include model-provided identifiers in logs or report warnings.
4. Preserve the current behavior for a complete one-to-one response.

### Tests and acceptance criteria

- Cover one complete response, one omitted decision, one duplicate, and one unexpected fingerprint.
- Invalid cardinality/identity never changes any finding in that batch away from `unclear`.
- The warning contains aggregate counts and no fingerprint values.

## Task 7 — Normalize OpenRouter reasoning parameters

**Priority:** Medium
**Depends on:** Task 1
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

Analysis currently sends top-level `reasoning_effort="low"`, while triage sends OpenRouter's normalized `reasoning={"effort":"none"}`. With `require_parameters=true`, inconsistent parameters may select different provider subsets, and mandatory-reasoning models may reject `none`.

### Implementation specification

1. Use OpenRouter's normalized `extra_body.reasoning` object consistently for both native and compatibility clients.
2. Use `{"effort": "low"}` for research planning and diagnosis.
3. For triage, omit the reasoning object by default rather than forcing `none`. This preserves compatibility with mandatory-reasoning models. If the existing connection `extra` explicitly supplies a valid triage reasoning effort, pass it through after validating it against the OpenRouter-supported effort strings.
4. Ensure construction merges `provider` and `reasoning` without either overwriting the other.
5. Do not send both top-level `reasoning_effort` and `extra_body.reasoning`.

### Tests and acceptance criteria

- Captured request JSON contains exactly one reasoning mechanism.
- Native analysis contains both `provider.require_parameters=true` and `reasoning.effort=low`.
- Default triage sends no forced `reasoning.effort=none`.
- Compatibility requests retain the appropriate reasoning object but omit `provider.require_parameters` and `response_format`.

## Task 8 — Add safe failure observability

**Priority:** Medium
**Depends on:** Tasks 2 and 3
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

Successful calls record duration, while failures omit duration, attempt, exception category, HTTP status, and safe request identifiers. The generic warning makes provider incidents hard to diagnose.

### Implementation specification

1. Emit one structured log for each failed attempt with fields represented through parameterized logging:
   - stage
   - model
   - structured-output mode
   - normalized failure reason
   - attempt and maximum attempts
   - elapsed seconds
   - HTTP status and provider error code when available
   - OpenRouter/OpenAI request ID when available from response headers or exception metadata
2. Emit a final warning when a stage is downgraded to unresolved.
3. Never log prompts, raw response bodies, API keys, headers containing authorization, log evidence, repository content, web-query content, or exception string representations that may contain a response body.
4. Keep report warnings concise and sanitized; detailed safe metadata belongs in service logs.

### Tests and acceptance criteria

- Use `caplog` to assert the safe fields exist on failure and duration is recorded.
- Seed fake exceptions with a secret, prompt, and raw response body and assert none appears in logs.
- Successful-call logging remains present.

## Task 9 — Enforce privacy-aware OpenRouter routing

**Priority:** Medium
**Depends on:** Task 1
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`; deployment/Vault configuration if it stores connection extras

### Problem

Operations prompts contain private log and repository evidence. OpenRouter's default provider policy may allow providers that retain request data. Capability filtering alone does not enforce a retention policy.

### Implementation specification

1. Set `provider.data_collection="deny"` on all OpenRouter operations-analyst requests, both native and prompt-JSON compatibility modes.
2. Preserve `provider.require_parameters=true` only for native JSON Schema mode. Compatibility mode must send `provider={"data_collection":"deny"}` without `require_parameters`.
3. Allow the existing connection `extra` mapping to opt into the stricter `provider.zdr=true` using a boolean key named `zdr`; default it to false because ZDR can eliminate all routes for a model.
4. Do not add a configuration that permits `data_collection="allow"` for this pipeline; changing that privacy decision should require a reviewed code change.
5. If privacy filtering leaves no eligible provider, treat it as a permanent deployment/configuration failure rather than silently relaxing the policy.

### Tests and acceptance criteria

- Native and compatibility captured request bodies both contain `provider.data_collection="deny"`.
- Native mode also contains `require_parameters=true`; compatibility mode does not.
- `zdr=true` is passed only when explicitly configured.
- No fallback removes or weakens the privacy settings.

## Task 10 — Preserve reason-specific report warnings

**Priority:** Low
**Depends on:** Task 2
**Primary files:** `packages/operations-analyst/src/operations_analyst/__init__.py`, `tests/test_operations_analyst.py`

### Problem

The current triage warning says only that the response could not be parsed, even when the actual failure is truncation, refusal, timeout, rate limiting, or malformed output.

### Implementation specification

1. Map normalized failure reasons to stable user-facing phrases. At minimum distinguish:
   - output truncated
   - empty or malformed provider response
   - invalid model JSON/schema output
   - request refused or content filtered
   - provider unavailable after retries
2. Include stage or triage batch number and state that affected findings remain unresolved.
3. Do not include exception messages, provider response bodies, prompts, request identifiers, or secrets in the report.
4. Keep the phrases stable so operators can compare weekly reports and tests can assert exact output.

### Tests and acceptance criteria

- Each normalized category produces the expected sanitized phrase.
- Truncation is no longer reported merely as a parsing failure.
- Existing report rendering continues to list the affected findings under `Unresolved`.

## Final integration verification

After all tasks are complete:

1. Run formatting and static checks:

   ```bash
   uv run ruff format .
   uv run ruff check .
   ```

2. Run the targeted suite:

   ```bash
   uv run pytest tests/test_operations_analyst.py
   ```

3. Run the full suite and record any pre-existing unrelated failure separately:

   ```bash
   uv run pytest
   ```

4. In a staging or manually invoked systemd run, verify native and `prompt_json` modes using `deepseek/deepseek-v4.1-flash`. Confirm that the success watermark advances only after the report is delivered.
5. Inject or simulate `choices:null`, empty choices, malformed JSON, refusal, content filtering, timeout, 429, 5xx, unsupported structured output, and invalid credentials. Confirm recoverable failures yield explicit unresolved findings while permanent deployment failures fail the service.
6. Inspect safe request telemetry to confirm routing parameters, reasoning configuration, retries, and failure categories without exposing operational evidence.

## Definition of done

- All ten tasks' acceptance criteria pass.
- The targeted suite, Ruff format, and Ruff check pass.
- The full suite has no new failure.
- Both native JSON Schema and non-structured DeepSeek compatibility paths are proven.
- A provider outage cannot erase or falsely resolve a finding.
- Authentication, authorization, schema-programming, and privacy-policy failures cannot be silently converted into a successful degraded report.
