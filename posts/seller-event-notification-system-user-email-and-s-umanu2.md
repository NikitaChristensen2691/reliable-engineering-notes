# Seller Event Notification System — User Email and SMS Opt-Out Template Ownership

TL;DR: Keep seller-order templates under the ownership of the team that defines the order event, but place channel preference, opt-out, and suppression enforcement in a shared dispatch boundary. Resolve those controls before rendering, persist the chosen template version and decision as one auditable record, and let transport adapters deliver only an already-authorized message. This split prevents a template editor or retry worker from quietly restoring a channel that the seller disabled.

A new-order alert looks harmless: render an email, perhaps render an SMS, then send both. The hard constraint is temporal. A seller can opt out after the order event is accepted but before a retry runs; a transport can reject a destination; and a template can change while an old job remains queued. If the worker reconstructs intent from whatever happens to be current, the same order can produce two different effects without any durable explanation.

The design target is not literal exactly-once delivery, which an application cannot infer from a timeout alone. It is one durable dispatch decision per `(order_event, recipient, channel)`, followed by idempotent attempts whose outcomes remain attributable to that decision. Small distinction. Large consequence.

## How should a Node.js event notification system enforce user channel preferences?

The marketplace domain team knows what a new-order message means: which order state qualifies, which fields may be disclosed, and when a correction requires a new event. That team should therefore own the semantic template and its version history. A central communications team can own rendering safety, adapter contracts, and operational policy, but it should not redefine the order lifecycle through a template toggle.

Keep that line sharp.

Ownership does not mean unrestricted publication. Treat a template version as reviewed application code: it has a stable identifier, an input schema, channel-specific output, and an activation record. The event producer emits an immutable order-event identifier and template key; the dispatcher resolves an active version once and stores that version beside the authorization decision. A later copy edit applies to later decisions, not to an in-flight retry.

The control order matters. Permanent suppression outranks a saved channel preference because a preference answers what this seller usually wants, while suppression answers whether this destination may be contacted through this channel now. An explicit opt-out should update the authoritative suppression list before another queued attempt is eligible. Rendering comes afterward, so personal data is not needlessly expanded into a message that policy will discard. In a Node.js implementation, these records may live behind repository interfaces rather than the Go interfaces shown below; the transaction and ownership boundaries remain the same.

This creates an intelligible audit trail. For every candidate channel, retain the event ID, recipient ID, normalized destination reference or protected lookup key, preference revision, suppression revision, template version, decision, reason code, and decision time. Keep transport attempts in a related append-only stream. Do not overwrite “accepted” with “failed”; those are different observations made at different times. Retention and access must follow the applicable compliance policy because this trail can contain contact and order metadata.

## Make one decision, then make retries boring

The following Go example deliberately leaves database and transport details behind interfaces. Its important property is transaction shape: serialize the dispatch key, read the applicable controls, insert one decision, and return the existing decision when the same event is evaluated again. The outbox writer can then publish authorized decisions after commit.

```go
package notifications

import (
    "context"
    "fmt"
    "time"
)

type Channel string

const (
    Email Channel = "email"
    SMS   Channel = "sms"
)

type OrderEvent struct {
    ID       string
    SellerID string
    OrderID  string
}

type Controls struct {
    PreferenceRevision string
    SuppressionRevision string
    Enabled             map[Channel]bool
    Suppressed          map[Channel]bool
}

type Decision struct {
    Key                 string
    EventID             string
    SellerID            string
    Channel             Channel
    TemplateVersion     string
    PreferenceRevision  string
    SuppressionRevision string
    Authorized          bool
    Reason              string
    DecidedAt           time.Time
}

type Store interface {
    InTransaction(context.Context, func(Tx) error) error
}

type Tx interface {
    FindDecision(string) (Decision, bool, error)
    LoadControlsForUpdate(string) (Controls, error)
    ActiveTemplateVersion(string, Channel) (string, error)
    InsertDecision(Decision) error
    AddToOutbox(Decision) error
}

type Dispatcher struct {
    Store Store
    Now   func() time.Time
}

func (d Dispatcher) Decide(ctx context.Context, event OrderEvent, channel Channel) (Decision, error) {
    key := fmt.Sprintf("%s:%s:%s", event.ID, event.SellerID, channel)
    var result Decision

    err := d.Store.InTransaction(ctx, func(tx Tx) error {
        prior, found, err := tx.FindDecision(key)
        if err != nil {
            return err
        }
        if found {
            result = prior
            return nil
        }

        controls, err := tx.LoadControlsForUpdate(event.SellerID)
        if err != nil {
            return err
        }
        result = Decision{
            Key: key, EventID: event.ID, SellerID: event.SellerID, Channel: channel,
            PreferenceRevision: controls.PreferenceRevision,
            SuppressionRevision: controls.SuppressionRevision,
            DecidedAt: d.Now().UTC(),
        }

        switch {
        case controls.Suppressed[channel]:
            result.Reason = "destination_suppressed"
        case !controls.Enabled[channel]:
            result.Reason = "channel_disabled"
        default:
            version, err := tx.ActiveTemplateVersion("seller.new_order", channel)
            if err != nil {
                return err
            }
            result.Authorized = true
            result.Reason = "authorized"
            result.TemplateVersion = version
        }

        if err := tx.InsertDecision(result); err != nil {
            return err
        }
        if result.Authorized {
            return tx.AddToOutbox(result)
        }
        return nil
    })
    return result, err
}
```

A unique constraint on the decision key is the final guard against concurrent workers. The application check improves the normal path; the constraint establishes the invariant. If two transactions race, one insert wins and the other reloads the committed record under the repository's duplicate-key handling. That behavior deserves a concurrency test, not an optimistic comment.

There is one intentional trade-off: the decision freezes policy at authorization time. If the seller opts out a millisecond later, a queued authorized item may still exist. The implementation therefore has 2 explicit suppression checkpoints: one inside the authorization transaction and one immediately before transport delivery. At the second checkpoint, the worker records the suppression revision it observed and cancels without rendering or sending when the destination is now blocked. It must not silently mutate the original decision; append a cancellation observation, retain the original reason, and connect both records with the same dispatch key. This preserves current consent enforcement and historical truth while making the race visible during reconciliation instead of hiding it behind the final row state.

Never rewrite history.

For SMS, consent and opt-out handling are operational constraints rather than copy preferences. CTIA's messaging guidance is a primary reference for messaging ecosystem expectations, but applicable legal and contractual limits depend on jurisdiction, message class, and program. Resolve those requirements with qualified compliance owners and encode the resulting rules as versioned policy, not as scattered worker conditionals.

## Compare ownership models after defining the invariant

Once the dispatch invariant is clear, template ownership becomes a bounded choice rather than a tooling contest.

| Model | Semantic change authority | Operational advantage | Main control risk |
|---|---|---|---|
| Domain-owned templates | Order domain maintainers | Event schema and copy evolve together | Rendering and compliance rules can diverge across teams |
| Central communications ownership | Shared platform maintainers | Consistent review and rendering controls | The central team can become a bottleneck or misread order semantics |
| Split ownership | Domain owns meaning; platform enforces publication and dispatch contracts | Clear policy boundary with local domain context | Version contracts and escalation paths require explicit maintenance |

For seller-order alerts, split ownership is usually the defensible default because message meaning belongs near the order state machine, while channel authorization and delivery evidence must stay consistent across the marketplace. This is a governance conclusion, not a vendor selection. A small organization may reasonably begin with one owning team, provided the stored records still separate template version, policy revision, and transport attempt.

Transport adapters should accept a rendered envelope plus a stable attempt key and return a classified observation: accepted, rejected, or indeterminate. An indeterminate result is not proof of failure and must not trigger an unbounded blind resend. Retry policy should be channel-specific, capped, and observable; terminal outcomes should enter reconciliation so operators can compare authorized decisions, outbox publication, attempts, and final observations. Provider documentation defines individual request and error contracts, but those contracts should remain behind the adapter.

Testing follows the same boundaries. Table tests cover preference and suppression precedence. Transaction tests force two workers onto the same decision key. Contract tests verify that each template version accepts its declared event schema. A delayed-job test activates version B after a version A decision and confirms that the retry still renders A. Finally, an opt-out race test changes the suppression revision between authorization and delivery and expects an appended cancellation, with no transport call.

## Roll out through shadow decisions and reconciliation

Start by recording decisions without sending, then compare them with the existing system's intended recipients. Investigate every mismatch by reason code: different preference revision, missing suppression, template resolution, or duplicate event identity. Aggregate counts help locate a class of error, but sampled records are needed to prove why it happened.

Next, enable one channel for a narrow seller cohort while preserving the old path as a non-sending observer. Reconcile four counts for each interval: eligible events, authorized decisions, outbox records, and terminal delivery observations. Do not demand that all four counts match blindly; suppressed and disabled decisions are expected differences, and indeterminate transport outcomes need their own aging queue.

Expand only after opt-out propagation, duplicate-event replay, template rollback, and transport timeout drills produce the expected ledger entries. The completion criterion is compact: any seller-order event can be traced to one channel decision, one fixed template version when authorized, and a chronological set of delivery observations. That is the evidence an operator, auditor, or compliance reviewer can actually use.

## Sources

References:

- https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
- https://resend.com/docs/introduction
