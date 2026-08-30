# Next.js Error Tracking: Grouped Exceptions and Searchable Node.js Events Under GDPR

Short answer: choose the error-tracking service only after a replay-free trial proves that every exception can be grouped by a stable application fingerprint, searched by a deliberately small field set, and charged back to the property account and nightly pipeline run that produced it. For a Next.js and React front end with a Node.js backend in Europe, the decisive artifact is not a feature matrix; it is an auditable event contract that keeps personal data out, preserves enough context to reconcile duplicate delivery, and exports per-tenant usage.

That rule is intentionally stricter than "the SDK caught an error." A property-management pipeline can fail after processing 8,417 lease rows, retry the same batch, and emit the same exception twice. If the service groups by a mutable stack trace, hides the raw searchable events behind an issue count, or cannot associate ingestion with `property_account_id` and `pipeline_run_id`, the operations team cannot distinguish one economic failure from two deliveries of the same evidence.

No replay is required. No source maps are required either.

## What invariants should a Next.js React Node backend require for grouped searchable exceptions under GDPR?

The first invariant is identity: the producer assigns an `event_id` once, and every retry carries that same value. The receiver may provide at-least-once transport, but the ledger of accepted events must behave idempotently. Exactly-once delivery is not a credible assumption across a browser, an API, a queue, and an external service; exactly-once effect, enforced by a unique event identifier and an append-only acceptance record, is the useful target. If an evaluation cannot demonstrate this with a forced retry, stop there.

The second invariant is grouping. Define a stable `fingerprint` from application-owned dimensions such as exception class, normalized operation, and a schema version. Don't use tenant, user, request ID, raw message text, line number, or deployment hash in that fingerprint: those values turn one defect into thousands of groups. Sentry documents both automatic grouping and application-supplied fingerprints; that is useful evidence that grouping deserves an explicit contract, but the contract should remain portable rather than inherit one service's hidden defaults.

The third invariant is minimization. The allowed event contains operational identifiers, bounded error taxonomy, release, component, and timestamps; it excludes names, email addresses, lease text, addresses, cookies, request bodies, and browser replay. This is an architectural control — not a claim that a particular payload is automatically GDPR-compliant. Lawful basis, retention, access, deletion, processor terms, international transfers, and the organization's role still require review by the responsible privacy and legal teams. I'm not sure any static vendor questionnaire can settle those obligations; a captured test payload, a data-flow record, and the executed contract are stronger evidence.

Finally, cost attribution is an invariant rather than a dashboard preference. Every accepted event needs low-cardinality `property_account_id`, `pipeline_name`, and `environment` dimensions, while every export or usage report needs to preserve enough of those dimensions to reconcile the service invoice against the internal run ledger. High-cardinality search has a cost, so `pipeline_run_id` should be searchable for investigation but should not automatically become a billing aggregation key.

Keep the contract small.

## Cost attribution: test the invoice before trusting the console

The trial should exercise boundaries, not a polished happy path. Send the same `event_id` twice and verify one logical event remains. Send two occurrences with different run IDs but the same fingerprint and verify one issue group retains two searchable occurrences. Remove an optional field and confirm the schema still accepts the envelope. Add a forbidden field such as `tenant_email` and require the gateway to reject it before any third-party transfer. Rotate the application release and confirm the group survives unless the underlying error taxonomy changed.

Use concrete acceptance records: request timestamp, payload schema version, payload hash, outcome, and external receipt identifier when one exists. A timeout is ambiguous; it does not prove rejection, so retry with the same event ID. A local `422` for a prohibited field is different: record the validation result, correct the producer, and do not transmit the rejected body. These distinctions matter during reconciliation because an operator must explain why the pipeline ledger has one exception while a remote console appears to have zero, one, or two deliveries.

There is a catch. Searchable custom fields can increase privacy exposure and indexing cost, while aggressive normalization can erase the clue that separates a property-specific data defect from a shared parser defect. Start with an allowlist, give every field an owner and retention purpose, then add a field only after a real investigation shows that the existing contract cannot answer a defined question. Your mileage may vary for regulated lease documents, but raw document content is a poor debugging field regardless.

Three named services may appear on an initial procurement list — Sentry, Rollbar, and Bugsnag — yet product names are not evidence of fit. Run the identical signed test corpus through each candidate and retain the results. The comparison must be reproducible by another engineer.

Consider one hypothetical nightly run, because the accounting failure is easier to see with actual states than with a checklist. Run `pm-eu-import-0042` starts for property account `acct-017`, reaches row 8,417, and emits event `evt-9f2` with fingerprint `LeaseParser.InvalidTerm.v1`; the worker loses its acknowledgement and retries, preserving both identifiers. The acceptance ledger should show two delivery attempts, one accepted payload hash, one external event identity, and one billable logical event. A second run for `acct-023` then emits `evt-a61` with the same fingerprint. The issue view should now show one grouped defect with two searchable occurrences, while the usage export must allocate one logical event to each property account. If the console reports one issue, the search returns two occurrences, the gateway ledger records three delivery attempts, and the allocation report totals two accepted events, all four views reconcile. If any view cannot expose the dimensions needed to prove that equation, the candidate hasn't passed — even though its headline issue count looks correct.

Then reconcile.

## Integration boundary: one ledger across three ingestion paths

| Option | Grouping control | Search and cost attribution | Audit boundary | Appropriate use |
|---|---|---|---|---|
| Direct browser and backend SDKs | Often split across runtimes and service defaults | Fast to start, but tenant dimensions can drift between producers | Each producer owns filtering and delivery evidence | A small, low-risk application with one team and no chargeback requirement |
| Internal event gateway | One versioned fingerprint policy for React, Next.js, and Node.js | Allowlisted searchable dimensions can match the internal cost ledger | Central validation, deduplication, and receipt log | Multi-tenant property systems where privacy review and cost attribution are material |
| Structured logs in an existing log platform | Application controls the complete record | Strong fit when nightly pipeline logs already carry tenant and run dimensions | Existing log retention and access controls can remain authoritative | Teams that need searchable events but do not need a specialized exception workflow |

For this system, record the decision to use an internal event gateway in front of whichever service passes the trial. The extra component is justified by one boundary: browsers and backend workers must not independently decide which tenant identifiers, personal fields, grouping inputs, and delivery receipts are acceptable. A gateway also permits a service change without rewriting the event contract in every producer.

This isn't free complexity. The team owns gateway availability, schema evolution, key rotation, backpressure, and its acceptance ledger. A single application with no sensitive payloads, no tenant chargeback, and a small on-call group may rationally choose direct SDK integration instead. Likewise, stick with the existing structured-log platform when exception triage is occasional and searchable nightly events are the real job; adding a separate issue console can create two retention policies and two incomplete audit trails.

The selection score should therefore weight contract tests above screenshots: deterministic grouping, event-level search, regional processing and storage terms that legal reviewers can verify, retention controls, exportability, access audit evidence, and usage data that can be reconciled by tenant. Price can be compared only after candidates pass those gates, because an inexpensive unallocatable invoice is still an accounting defect.

## Reliability protocol: close the acknowledgement gap in Go

The gateway below demonstrates the narrow control point. It uses only the Go standard library, accepts a versioned envelope, rejects unknown JSON fields, verifies required attribution dimensions, computes a payload hash for the audit record, and applies idempotency before forwarding. The `Store` and `Sink` interfaces are deliberate: their implementations can be tested against a database and any candidate service without putting a vendor route or SDK into the domain model.

```go
package events

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "errors"
    "net/http"
    "strings"
    "time"
)

type Event struct {
    SchemaVersion     int               `json:"schema_version"`
    EventID           string            `json:"event_id"`
    Fingerprint       string            `json:"fingerprint"`
    ExceptionClass    string            `json:"exception_class"`
    Operation         string            `json:"operation"`
    PropertyAccountID string            `json:"property_account_id"`
    PipelineRunID     string            `json:"pipeline_run_id"`
    PipelineName      string            `json:"pipeline_name"`
    Environment       string            `json:"environment"`
    Release           string            `json:"release"`
    OccurredAt        time.Time         `json:"occurred_at"`
    Attributes        map[string]string `json:"attributes,omitempty"`
}

type Receipt struct {
    EventID     string
    PayloadHash string
    AcceptedAt  time.Time
    SinkID      string
}

type Store interface {
    Receipt(context.Context, string) (Receipt, bool, error)
    Commit(context.Context, Receipt) error
}

type Sink interface {
    Send(context.Context, Event) (string, error)
}

type Handler struct {
    Store Store
    Sink  Sink
    Now   func() time.Time
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
    dec.DisallowUnknownFields()

    var event Event
    if err := dec.Decode(&event); err != nil {
        http.Error(w, "invalid event envelope", http.StatusUnprocessableEntity)
        return
    }
    if err := validate(event); err != nil {
        http.Error(w, err.Error(), http.StatusUnprocessableEntity)
        return
    }

    canonical, err := json.Marshal(event)
    if err != nil {
        http.Error(w, "invalid event envelope", http.StatusUnprocessableEntity)
        return
    }
    sum := sha256.Sum256(canonical)
    payloadHash := hex.EncodeToString(sum[:])

    existing, found, err := h.Store.Receipt(r.Context(), event.EventID)
    if err != nil {
        http.Error(w, "receipt lookup unavailable", http.StatusServiceUnavailable)
        return
    }
    if found {
        if existing.PayloadHash != payloadHash {
            http.Error(w, "event_id reused with different payload", http.StatusConflict)
            return
        }
        writeReceipt(w, existing)
        return
    }

    sinkID, err := h.Sink.Send(r.Context(), event)
    if err != nil {
        http.Error(w, "delivery unavailable", http.StatusServiceUnavailable)
        return
    }
    receipt := Receipt{
        EventID: event.EventID, PayloadHash: payloadHash,
        AcceptedAt: h.Now().UTC(), SinkID: sinkID,
    }
    if err := h.Store.Commit(r.Context(), receipt); err != nil {
        http.Error(w, "receipt commit unavailable", http.StatusServiceUnavailable)
        return
    }
    writeReceipt(w, receipt)
}

func validate(event Event) error {
    if event.SchemaVersion != 1 {
        return errors.New("unsupported schema_version")
    }
    required := []string{
        event.EventID, event.Fingerprint, event.ExceptionClass,
        event.Operation, event.PropertyAccountID, event.PipelineRunID,
        event.PipelineName, event.Environment,
    }
    for _, value := range required {
        if strings.TrimSpace(value) == "" {
            return errors.New("required attribution field is empty")
        }
    }
    return nil
}

func writeReceipt(w http.ResponseWriter, receipt Receipt) {
    w.Header().Set("Content-Type", "application/json")
    _ = json.NewEncoder(w).Encode(receipt)
}
```

One subtle point remains: sending before committing leaves a crash window in which the sink accepts an event but the local receipt is absent. The production design should close that window with a transactional outbox: commit the normalized event and its pending delivery state in one local transaction, let a worker send it with the stable event ID, and mark the receipt after acknowledgement. The sink may still observe retries, so its deduplication behavior belongs in the trial. This is the exact place where an "exactly once" slogan usually collapses into an auditable state machine.

Roll out changes to the allowlist or fingerprint version behind a feature toggle, compare old and new grouping on a scrubbed test corpus, and record who enabled the new contract. Feature toggles add configuration that must itself be managed; Martin Fowler's treatment is a useful reminder that toggle categories and lifetimes differ. Remove the migration toggle after the new schema is established.

## Trade-off record: direct SDK ingestion still has a valid use case

The ADR decision is an internal gateway plus a replaceable error-event sink, selected by a replay-free contract trial. The acceptance criteria are stable grouping, searchable occurrences, explicit data minimization, idempotent effects, exportable receipts, and cost attribution by property account and pipeline. The nightly pipeline ledger remains the source of truth for run completion; error tracking supplies investigative evidence and must reconcile back to that ledger.

The rejected default is direct SDK integration from every React, Next.js, and Node.js runtime. It is not universally wrong. Choose it for a small system when the same team owns every producer, privacy classification is straightforward, and cost allocation stops at one application. Do not choose the gateway merely to imitate a larger architecture; choose it when centralized policy, audit evidence, and tenant-level reconciliation repay its operational cost.

A self-hosted log platform is also a valid alternative when searchable structured events matter more than issue workflow, and it may keep an established access and retention model intact. It is not suitable when on-call engineers need mature exception grouping but the team is unwilling to build that workflow. Conversely, a specialized tracker is not suitable when its event export cannot reproduce the tenant and run dimensions needed for invoice reconciliation.

The final procurement record should contain the event schema, the scrubbed corpus, observed grouping results, duplicate-delivery results, retention and regional-processing evidence reviewed by the responsible teams, an export sample, and a cost-allocation rehearsal. A console demonstration is ephemeral. Those artifacts can be rerun.

## References

- https://martinfowler.com/articles/feature-toggles.html
- https://docs.sentry.io/concepts/data-management/event-grouping/
