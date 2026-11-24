# Logistics Leaked API Key Grace Window Drills With Spend and Refusal Accounting

Revoke a leaked API key immediately when continued acceptance cannot be bounded by an enforceable spend ceiling; consider rotation with a grace window only when the drill can prove that the old credential stays inside that ceiling and that its remaining authority is acceptable. Refused logistics traffic belongs in the decision record alongside financial exposure. A shipment update rejected during containment still needs an owner and a recovery path.

TL;DR: For a confirmed leak, immediate revocation is the default decision in this record. A grace window is an explicit, expiring exception with reserved exposure, restricted authority, and an abort condition. Rotation creates a replacement credential; containment additionally requires removing the leaked credential's authority. Keep those outcomes separate in the evidence.

The OWASP Secrets Management Cheat Sheet treats revocation, rotation, auditing, and incident response as parts of secret lifecycle management. It supports the lifecycle framing here; the numerical limits and logistics workflow below are proposed exercise inputs, not reported incidents, service guarantees, or compliance thresholds. The editorial angle is drill acceptance: establish what evidence would let an incident commander declare containment without silently losing shipment work.

## Decision record and invariants

The proposed system receives carrier events and submits shipment-processing work through an authenticated downstream service. Its application may run in Node.js, but credential acceptance, outstanding requests, and retry identity must be evaluated across the whole request path; restarting one process does not establish that the leaked key has lost authority. The critical-path example below uses Go to make the admission contract explicit, independently of the application's runtime.

Choose immediate revocation for the exercise baseline. Introduce a grace-window branch only to test a documented exception, such as preserving a bounded stream of shipment acknowledgments while replacement credentials reach workers. The invariant is that the old credential can authorize no new work after its rejection boundary. Requests already accepted require separate accounting, because changing future authentication does not itself prove that an earlier request stopped or that an external side effect disappeared.

Count outstanding work.

A second invariant is that every admitted logical shipment operation owns a durable reservation before it can consume the remaining exposure allowance. An idempotency key identifies that operation across replacement credentials and retries, while a request digest binds the identifier to its payload; reusing an identifier with different shipment data must produce a conflict rather than a replay. This is an exactly-once mindset expressed through deduplication, explicit uncertainty, and reconciliation, rather than a claim that a network delivers exactly once.

The third invariant concerns evidence. Record the incident identifier, a nonsecret credential identifier, policy version, authorization decision, operation identifier, reservation amount, and eventual settlement or unresolved status. Never copy the leaked key into the drill transcript. An audit trail supports investigation, but neither this record nor the presence of logs establishes compliance with a particular retention, privacy, or payment-control obligation; those requirements need their own review.

## Should a leaked API key response use immediate revocation or a grace window?

Use an illustrative drill budget of 100 exposure units for the compromised credential, with 70 units already settled and 20 units reserved for accepted but unresolved work. Only 10 units remain available. These units represent a conservative internal exposure measure, not a provider price. If the next operation requires a reservation of 12 units, refuse it even if the grace timer has time remaining. A timer is not a spend ceiling.

The following comparison records choices for this exercise.

The limitation is enforcement: bounded grace is not suitable when the downstream acceptance path cannot enforce the reservation policy. Choose immediate revocation in that case. The trade-off is explicit: stricter containment can increase refused shipment traffic, while overlap preserves some availability at the cost of continued credential authority.

| Option | Exposure boundary | Refused traffic consequence | Acceptance evidence |
| --- | --- | --- | --- |
| Immediate revocation | Reject new authorization with the leaked credential; reconcile previously accepted work separately | Workers using the old key must pause or retry after migration | Old-key probes fail across the acceptance path; outstanding operations have dispositions |
| Rotation with bounded grace | Deadline, restricted authority, and atomic exposure reservations all apply | Authorized work can continue until any boundary rejects it | Reservations remain within the ceiling; expiry and explicit abort both reject the old key |
| Rotation with time-only overlap | Exposure during overlap has no demonstrated financial bound | Fewer migration-related refusals may occur during the overlap | Suitable for the planned replacement exercise described below, but rejected for confirmed-leak containment |

Measure refused logical operations separately from refused attempts. A worker retrying one shipment 30 times should create one recovery obligation and 30 attempt records, not 30 shipments in the recovery queue. In the drill report, distinguish an authentication rejection from a budget rejection and an unresolved downstream result, since each requires a different operator action. Record both the numerator and denominator when presenting a refusal rate; a percentage without the attempted workload conceals the operational size of the interruption.

There is a hard boundary to the grace proposal: if an attacker can submit the leaked key directly to a downstream service, a budget check inside the logistics application cannot constrain that attacker. The old credential's actual acceptance path must enforce the ceiling and expiry. Otherwise select revocation. Evidence of abuse is also an abort condition in this proposed policy, even when unused allowance remains, because a spend cap does not bound unauthorized access to shipment data.

## Critical path for bounded admission

The admission boundary needs a durable transaction, not an in-process counter. It must serialize the policy check and reservation with concurrent admissions, and it must persist the decision's audit event in the same transaction.

A failed audit append therefore fails admission in this design.

That choice sacrifices availability at the boundary to preserve an explainable authorization history.

This Go fragment defines the critical contract; it is not a storage implementation or a downstream client. The storage implementation must enforce the documented transaction semantics. Exposure uses integer units, and the subtraction form avoids overflowing a sum when comparing an already validated reservation against the remaining allowance.

```go
package containment

import (
    "context"
    "errors"
    "time"
)

var ErrRefused = errors.New("credential admission refused")
var ErrConflict = errors.New("operation payload conflict")

type Request struct {
    IncidentID string
    KeyID      string // Nonsecret credential identifier.
    Operation  string // Stable across retries and replacement keys.
    Digest     string // Digest of the canonical operation payload.
    Reserve    int64
}

type Policy struct {
    Revoked   bool
    ExpiresAt time.Time
    Remaining int64 // Ceiling minus settled and outstanding exposure.
}

type Tx interface {
    // Lookup is scoped to the authenticated account, not the key.
    Lookup(operation string) (digest string, exists bool, err error)
    Policy(keyID string) (Policy, error)
    Reserve(Request) error
    Audit(Request, string) error
}

type Store interface {
    // Serialize admissions and revocation; commit writes atomically.
    // The callback receives authoritative transaction time.
    InTx(context.Context, func(Tx, time.Time) error) error
}

func Admit(ctx context.Context, db Store, r Request) (bool, error) {
    if r.IncidentID == "" || r.KeyID == "" ||
        r.Operation == "" || r.Digest == "" || r.Reserve <= 0 {
        return false, ErrRefused
    }
    var decision error
    replay := false
    err := db.InTx(ctx, func(tx Tx, now time.Time) error {
        p, err := tx.Policy(r.KeyID)
        if err != nil {
            return err
        }
        if p.Revoked || !now.Before(p.ExpiresAt) {
            decision = ErrRefused
            return tx.Audit(r, "credential_refused")
        }
        digest, exists, err := tx.Lookup(r.Operation)
        if err != nil {
            return err
        }
        if exists {
            if digest != r.Digest {
                decision = ErrConflict
                return tx.Audit(r, "payload_conflict")
            }
            replay = true
            return tx.Audit(r, "existing_operation")
        }
        if p.Remaining < 0 || r.Reserve > p.Remaining {
            decision = ErrRefused
            return tx.Audit(r, "budget_refused")
        }
        if err := tx.Reserve(r); err != nil {
            return err
        }
        return tx.Audit(r, "reserved")
    })
    if err != nil {
        return false, err
    }
    return replay, decision
}
```

Authentication, account authorization, scope restrictions, and a trusted calculation of the requested reservation precede this function. The caller cannot choose its own cheaper reservation. A successful replay result means retrieve the existing operation's authorized status; it does not mean submit the shipment again. Notice that credential rejection precedes deduplication, so knowledge of an old operation identifier does not let a revoked key retrieve a prior result. The transaction adapter must invoke the callback once per call, returning any serialization conflict to the caller for a fresh admission attempt.

A reservation must also create durable pending work, or a durable handoff record, in the transaction implemented behind `Reserve`. Dispatch happens afterward. If the downstream request times out, retain the reservation until reconciliation establishes its outcome; releasing it merely because the caller stopped waiting would reopen budget while the original operation might still complete. If a downstream service cannot deduplicate or resolve an uncertain submission, stop automatic resubmission and make that uncertainty visible to the operator. This is the awkward boundary the drill should expose.

## Run the drill through reconciliation

Before exercising either branch, obtain the allowed interruption envelope from the logistics owner: which shipment operations can wait, how they will be retained, and who owns recovery. Use synthetic credentials and shipment identifiers in a controlled environment. Inventory the actual consumers, including scheduled workers and queued jobs, because the drill's migration evidence should cover the consumers that can still present the old credential. Prepare a replacement with the required scope and record its delivery without recording its value.

Run the immediate-revocation branch first, capturing the authoritative revocation decision and probing each identified acceptance path with the old credential. Then submit replacement-key traffic and replay a previously accepted synthetic operation. The expected result is one logical operation, with attempts linked in the audit record. Test a changed payload under the same identifier too. That must produce a conflict.

For the grace branch, use the 100-unit example to stage concurrent reservations near the remaining allowance. Verify that only reservations fitting the available 10 units commit, then exercise both deadline expiry and an explicit abuse-triggered abort. Inject an audit-storage failure and an uncertain downstream completion as separate cases. The former should block admission under this design; the latter should retain its reservation and appear in reconciliation. Restart workers during the exercise to establish that neither revocation state nor deduplication depends on one process staying alive. Do not declare containment from a replacement-key success alone. Close the drill only when the old credential is rejected, each refused logical shipment has a recovery disposition, and settled exposure plus outstanding reservations reconciles to the admission ledger. Preserve unresolved items explicitly rather than converting them into successes for the report. If a probe still succeeds after the intended boundary, the exercise has found an acceptance-path discrepancy; the incident record needs its owner and resolution before closure.

## Rejected option and its valid use case

This record rejects time-only overlap for a confirmed leak because it provides no demonstrated spend bound and deliberately leaves the compromised credential usable until expiry. It remains a reasonable option to evaluate for planned credential replacement when compromise is not suspected, the permitted overlap is documented, and the purpose is to measure consumer migration. That is a different exercise objective.

For the leaked-key drill, the final decision rests on enforceable exposure limits and recoverable refused shipment work. If either cannot be demonstrated, revoke immediately, preserve pending work, and reconcile before replay.

## References

- OWASP Secrets Management Cheat Sheet. Lifecycle guidance on rotation, revocation, auditing, and incident response: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
