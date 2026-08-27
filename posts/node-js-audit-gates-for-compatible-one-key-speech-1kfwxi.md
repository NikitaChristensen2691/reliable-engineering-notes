# Node.js Audit Gates for Compatible One-Key Speech-to-Text Invoice Intake Jobs

Short answer: treat speech-to-text as an admitted capability, not as a promise implied by an OpenAI-compatible credential; read the model catalog into a typed registry, combine it with explicit feature flags and region policy, and select a fallback before accepting a supplier-invoice audio job.

For a healthtech intake service, the consequential result isn't “the request reached an AI endpoint.” It is a transcript whose origin, policy decision, and downstream extraction can be reconstructed without guessing. A single key may simplify authentication, but it doesn't prove that every model family or every deployment region is available behind that key. Admission control should therefore run before durable work is claimed, while the audio object's identifier, tenant, requested residency, catalog revision, and selected model can still be recorded as one decision.

This note records that architecture decision for a Node.js application whose runtime policy is implemented as a small, language-neutral boundary. The examples are in Go because the selector is easiest to audit as a typed, pure function; the Node.js worker can call the same boundary or implement the identical contract.

## How can Node.js detect speech-to-text support from one key, model lists, and feature flags?

Start with three separate assertions. Discovery says a model identifier is visible to the credential. A capability declaration says the model accepts audio and produces text. Policy says that this tenant may use that model in the requested region. None substitutes for another, and compatibility at the wire level proves only that a request shape is understood; it says nothing about residency approval or the semantic correctness of an invoice transcript.

The model list should be normalized into an internal catalog rather than consulted ad hoc by every worker. Each entry needs a stable provider alias, model identifier, declared capabilities, deployment region, and the revision or observation time of the catalog. Feature flags then disable or enable an already discovered route for a tenant or workload. They shouldn't manufacture a capability that discovery did not establish. If either input is absent, the conservative state is `unknown`, not `supported`.

That distinction matters during rollout. Suppose the EU catalog contains an audio-capable model, the US catalog contains another, and a tenant is restricted to EU processing. A global fallback array would appear healthy in a unit test yet could cross the residency boundary in production. The selector must filter by capability and region before it applies preference order. No shortcut.

Model identifiers also make poor feature tests when they are parsed by substring. A name containing `audio` is evidence about naming, not an interface contract. Store the capability as data, preserve the source catalog revision in the audit record, and require a fresh admission decision when that revision changes. I'm not sure how often any particular upstream catalog will change; that uncertainty is precisely why the revision belongs in state rather than in an engineer's memory.

## The invoice admission record

The first invariant is idempotency: one invoice-audio object and one extraction schema version produce one logical transcription job. A retry may repeat transport, but it must not create a second logical result that silently replaces the first. Use an idempotency key derived from immutable internal identifiers, never from a filename supplied by a user, and persist the key before dispatch.

The second invariant is provenance. An accepted job records the catalog revision, provider alias, model identifier, region, feature-policy revision, input object digest, and schema version. Credentials and raw audio do not belong in that audit row. If an invoice can contain protected health information, the system design must account for the access-control and audit-control requirements in 45 CFR Part 164; whether a particular invoice is actually regulated depends on its contents and the organization's role, so counsel and the compliance owner must resolve that boundary rather than an SDK default.

The third invariant is monotonic state. A job moves from `admitted` to `transcribing` to either `transcribed` or a classified terminal outcome. A provider rejection does not send it back to an unrecorded queue head. The fallback decision is appended, with a reason such as `capability_absent`, `region_disallowed`, or `policy_disabled`, and the next candidate is chosen from the same frozen policy snapshot. This is an exactly-once mindset implemented over at-least-once machinery: duplicate delivery is expected, while duplicate business effects are prohibited.

Failures divide at the admission boundary. An empty or stale catalog, an unknown capability, and a disallowed region reject admission without consuming audio. Authentication failures invalidate the catalog snapshot and stop selection; treating them as a signal to try a different geography could evade policy. Failures after admission retain the original decision record and may retry only under an explicit retry budget. If policy permits switching providers after admission, the switch becomes a new attempt under the same logical job, not a new job.

Structured output correctness begins after transcription but shares the same ledger. Keep the raw transcript immutable, validate extracted invoice fields against a versioned schema, and store validation errors beside the extraction attempt. A medically relevant supplier name, invoice date, purchase-order identifier, currency, and line-item total should not be accepted merely because they are syntactically strings or numbers. Cross-field rules, such as line totals reconciling to the stated total under the configured rounding policy, belong in deterministic code. Speech confidence can inform review routing, but it cannot prove an accounting identity.

Consider a hypothetical job `inv_7f2c`, with audio digest `sha256:…91ab`, schema revision `invoice-v4`, and an EU-only policy. Catalog revision 41 exposes two entries: provider alias `primary` declares speech-to-text in the EU, while alias `reserve` declares it only in the US. Policy revision 12 enables both aliases and orders `primary` before `reserve`. Admission records `primary`, its model ID, `eu`, revisions 41 and 12, and the job idempotency key; it does not record the credential. If two workers race, the unique idempotency key permits one admission row and makes the other worker read that row. If a later retry sees catalog revision 42, it still follows the frozen revision-41 decision unless a separately recorded policy transition authorizes reselection. If `primary` is absent from the original snapshot, the job receives the internal outcome `capability_absent`; `reserve` is not selected because its US location fails before preference is evaluated. After a transcript is stored, extraction attempt 1 may produce line items that do not reconcile with the invoice total. That is not a transcription success promoted into an accepted invoice. The immutable transcript stays attached to the job, the extraction attempt records its schema and reconciliation failure, and the work moves to the configured review path. This single example is why capability, residency, idempotency, and structured correctness belong to one trace: each control answers a different question, and none can be reconstructed reliably from the final text alone.

There are four credible ways to place that admission boundary:

| Option | Admission evidence | Audit quality | Main limitation | Appropriate use |
|---|---|---|---|---|
| Call the preferred speech model and fall back on rejection | Runtime response only | Weak unless every attempt is reconstructed | Work is accepted before region and capability are established | Low-risk prototypes with no residency constraint |
| Infer support from model names | Catalog names | Repeatable but semantically weak | Naming conventions can change and do not declare input/output modes | Temporary diagnostics, never production admission |
| Normalize discovery, capability flags, and region policy | Typed catalog plus policy snapshot | Strong: the decision inputs can be retained | More control-plane state and a conservative `unknown` state | Regulated, multi-region invoice intake |
| Pin one deployment with no fallback | Deployment configuration | Simple | Planned maintenance or capacity policy can halt intake | Environments where operational simplicity outweighs continuity |

The third option is the decision here, but its cost is real. It introduces a catalog refresh path, policy revisioning, and an operator-visible quarantine for jobs that cannot be admitted. Teams unable to operate those pieces should pin one approved deployment and accept lower availability rather than pretend a fallback is safe.

## Selection as a pure Go function

The critical selector is deliberately boring. Catalog collection and authentication happen elsewhere; this function receives a validated snapshot, refuses ambiguity, and emits enough identifiers for an append-only decision record. The illustrative policy values are local configuration, not claims about any provider.

```go
package admission

import (
	"errors"
	"fmt"
)

type Region string

const (
	RegionEU Region = "eu"
	RegionUS Region = "us"
)

type Model struct {
	Provider       string
	ID             string
	Region         Region
	SpeechToText   bool
	CatalogRevision string
}

type Policy struct {
	AllowedRegion  Region
	SpeechEnabled  map[string]bool // Keyed by provider alias.
	Preference     []string
	PolicyRevision string
}

type Decision struct {
	Provider        string
	ModelID         string
	Region          Region
	CatalogRevision string
	PolicyRevision  string
}

var ErrNoAdmissibleModel = errors.New("no admissible speech-to-text model")

func Select(models []Model, policy Policy) (Decision, error) {
	byProvider := make(map[string][]Model)
	for _, model := range models {
		if !model.SpeechToText || model.Region != policy.AllowedRegion {
			continue
		}
		if !policy.SpeechEnabled[model.Provider] {
			continue
		}
		byProvider[model.Provider] = append(byProvider[model.Provider], model)
	}

	for _, provider := range policy.Preference {
		candidates := byProvider[provider]
		if len(candidates) == 0 {
			continue
		}
		if len(candidates) > 1 {
			return Decision{}, fmt.Errorf(
				"provider %q has %d admissible models; policy must choose one: %w",
				provider, len(candidates), ErrNoAdmissibleModel,
			)
		}

		model := candidates[0]
		return Decision{
			Provider:        model.Provider,
			ModelID:         model.ID,
			Region:          model.Region,
			CatalogRevision: model.CatalogRevision,
			PolicyRevision:  policy.PolicyRevision,
		}, nil
	}

	return Decision{}, ErrNoAdmissibleModel
}
```

Do not let a map iteration decide which model wins; preference is ordered data, and ambiguity is an error. In production, the returned decision is inserted with the job's idempotency key in the same database transaction that changes the job to `admitted`. A unique constraint on that key makes concurrent workers converge on the recorded decision. The worker reads that record rather than running selection again, which prevents a catalog refresh between retry attempts from rewriting history.

The Node.js edge should expose the outcome as an internal typed result: admitted, temporarily unavailable because catalog evidence is stale, or rejected by policy. Avoid forwarding raw upstream errors into queue semantics. An administrative browser view can receive catalog-revision changes over Server-Sent Events, which MDN documents as a one-way server-to-client event stream, but SSE is an observability convenience here — it is not the source of truth and it must not mutate admission state.

Test the selector with table-driven cases covering an empty catalog, a disabled provider, the wrong region, multiple admissible models under one alias, and an ordered fallback. Then test the transaction boundary with two workers claiming the same idempotency key. For extraction, maintain a small, access-controlled corpus of representative audio and expected structured fields; compare normalized field values and reconciliation outcomes, not transcript similarity alone. The acceptance threshold is a product and compliance decision, and mileage will vary with microphones, accents, vocabulary, and invoice layout.

## Where request-first probing belongs

We reject “try the default model, then inspect the error” for this system because it entangles capability detection with the handling of potentially sensitive audio, cannot prove region eligibility before dispatch, and leaves an audit trail whose meaning depends on upstream error taxonomy. We also reject using a feature flag alone: flags express local intent, while discovery supplies evidence that the intended model exists for the credential and region.

The catch is that preflight admission is not suitable for every workload. A disposable developer tool processing synthetic audio may reasonably choose request-first probing, especially when its catalog is short-lived and no regulated data crosses the boundary. Stick with one pinned, approved deployment when the organization cannot yet operate catalog freshness, policy revisions, and quarantined jobs. That choice sacrifices automated continuity, but its smaller state space can be easier to audit honestly.

For the healthtech invoice path, correctness wins. Accept audio only after capability and region are proven, preserve the chosen inputs as an immutable decision, and make provider fallback a policy event under the original idempotency key. The extraction stage can then fail closed on schema or reconciliation errors without confusing a transcription attempt with a valid payable record.

## References

- https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164

## Further reading

- MDN, “Using server-sent events”: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- Electronic Code of Federal Regulations, 45 CFR Part 164: https://www.ecfr.gov/current/title-45/subtitle-A/subchapter-C/part-164
