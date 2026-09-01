# US/EU SaaS Queue Recovery: Redrive Failed Weekly Digest Jobs in Node.js

Short answer: for a small US/EU SaaS sending a weekly customer digest, choose the least elaborate queue that can isolate dead letters and redrive a selected item, but make the application database decide whether a digest may be sent; the queue should guarantee another opportunity to work, while a durable delivery ledger prevents that opportunity from becoming a duplicate email.

That division of responsibility is the important trade-off. A scheduler can initiate each weekly run, and asynchronous messaging can separate digest production from delivery, but neither mechanism proves that a customer-visible effect happened exactly once. If a worker loses contact after handing a message to an email system, it may be unable to distinguish “not accepted” from “accepted but not confirmed.” Blind retry risks two digests; blind acknowledgement risks none. The smallest credible recovery design therefore needs a stable send identity, an auditable state transition, and a redrive policy before it needs a sophisticated control plane.

Keep the promise narrow.

## Integrate the delivery ledger before the queue

Model one logical delivery as `(digest_week, customer_id, digest_revision)`, not as a queue message ID. The logical identity must survive a retry, a process restart, and movement through a dead-letter queue (DLQ). A replacement message may have different transport metadata while still representing the same intended email, so transport identity is useful for diagnosis but unsafe as the idempotency boundary.

For an active-customer digest, eligibility also has a time dimension. The scheduler can produce candidate jobs from a snapshot, yet a customer might become inactive or unsubscribe before a delayed job is redriven. The worker should recheck current authorization immediately before delivery. This does mean that the recovered run can differ from the original snapshot — a deliberate choice in favor of present consent over historical campaign completeness — and the audit record should preserve both the original eligibility decision and the later suppression decision. The uncertainty cannot be repaired by another retry; it is resolved by the system of record.

A practical job envelope stays compact:

```go
type DigestJob struct {
	DeliveryID    string // Stable across retries and redrive.
	CustomerID    string
	DigestWeek    string
	Revision      int
	EligibilityAt time.Time
	TraceID       string
}
```

The message contains identifiers, not a fully rendered archive of customer data. The worker reads authoritative state, renders the appropriate digest, and records the outcome. For US/EU operation, this boundary also makes data placement explicit: queue payloads, delivery records, and rendering inputs can be assigned retention and regional handling policies independently. The applicable contractual and compliance limits vary by organization, so they belong in a documented control reviewed by the responsible legal and security owners, not in an assumed default hidden inside retry code.

Exactly-once processing is an attractive phrase and a poor substitute for an effect model. The relevant invariant is more precise: for each delivery ID, the application authorizes at most one customer-visible send, and every attempt leaves enough evidence to reconcile the result. A queue may deliver work again. That is acceptable. Duplicate execution becomes dangerous only when the application cannot recognize the logical effect.

Use a database uniqueness constraint on the delivery ID and commit the claim before invoking the sender. Record state changes such as `eligible`, `claimed`, `accepted`, `suppressed`, and `review_required`, each with a timestamp and correlation data. Do not erase failed attempts after success; append attempt records so an operator can reconstruct why a redrive occurred. This is the same exactly-once mindset used around financial effects, adjusted to an email consequence whose correct response to uncertainty may be suppression and review rather than aggressive replay.

There is a hard edge here. A local transaction cannot normally make a remote email submission and a database commit indivisible. If submission is accepted but the worker cannot persist confirmation, automatic replay may duplicate the digest. The conservative policy is to move that ambiguous delivery to `review_required`, reconcile it against whatever durable acceptance evidence the sender makes available, and only then authorize redrive. I'm not sure every sender exposes evidence with the same useful lifetime; that capability has to be verified during selection and tested in the recovery runbook.

The following Go sketch keeps queue mechanics behind an interface and puts the claim in durable storage. It is intentionally focused on the state boundary; rendering, transport authentication, and regional deployment configuration remain outside it.

```go
package digest

import (
	"context"
	"errors"
	"time"
)

var ErrAlreadyClaimed = errors.New("delivery already claimed")

type Job struct {
	DeliveryID string
	CustomerID string
	Week       string
	Revision   int
}

type Claim struct {
	DeliveryID string
	AttemptID  string
	ClaimedAt  time.Time
}

type Ledger interface {
	// Claim inserts DeliveryID under a unique constraint in one transaction.
	Claim(ctx context.Context, job Job) (Claim, error)
	MarkAccepted(ctx context.Context, claim Claim, receipt string) error
	MarkSuppressed(ctx context.Context, claim Claim, reason string) error
	MarkForReview(ctx context.Context, claim Claim, reason string) error
}

type Customers interface {
	MayReceiveDigest(ctx context.Context, customerID string) (bool, error)
}

type Sender interface {
	Send(ctx context.Context, job Job, idempotencyKey string) (string, error)
}

type Worker struct {
	Ledger    Ledger
	Customers Customers
	Sender    Sender
}

func (w Worker) Handle(ctx context.Context, job Job) error {
	claim, err := w.Ledger.Claim(ctx, job)
	if errors.Is(err, ErrAlreadyClaimed) {
		return nil
	}
	if err != nil {
		return err
	}

	allowed, err := w.Customers.MayReceiveDigest(ctx, job.CustomerID)
	if err != nil {
		return w.Ledger.MarkForReview(ctx, claim, "eligibility unavailable")
	}
	if !allowed {
		return w.Ledger.MarkSuppressed(ctx, claim, "customer not eligible")
	}

	receipt, err := w.Sender.Send(ctx, job, job.DeliveryID)
	if err != nil {
		return w.Ledger.MarkForReview(ctx, claim, "send result uncertain")
	}
	return w.Ledger.MarkAccepted(ctx, claim, receipt)
}
```

The compact early return on `ErrAlreadyClaimed` is what makes an ordinary duplicate harmless, but production code needs a richer claim state than this excerpt shows: an old claim may represent an active worker, an abandoned attempt, or a send awaiting reconciliation. Don't turn a timeout into permission to steal the claim. Lease expiry, worker liveness, and operator approval have to produce an explicit transition, because otherwise the “recovery” mechanism quietly becomes a second sender.

Auditability also changes the useful metrics. Queue depth and oldest-message age describe transport pressure; they do not describe customer outcomes. Track the count of unique delivery IDs by terminal state, the age of `review_required` records, suppressions after eligibility recheck, redrive approvals, and reconciliation mismatches. Compare the candidate population with accepted plus intentionally suppressed deliveries for each digest week. That equation is far more informative than a green worker process.

## Which failed job classes permit queue retry or DLQ redrive?

The simplest service is the one whose recovery semantics the team can explain at 03:00, not necessarily the one with the fewest configuration fields. Before selecting anything, run the same four failure classes through the proposed design.

| Failure class | Automatic action | Redrive condition | Evidence retained |
| --- | --- | --- | --- |
| Temporary dependency failure before submission | Retry with bounded delay | Retry budget remains | Attempt, timing, error class |
| Invalid job or invariant violation | Move out of the worker path | Payload or code is corrected and reviewed | Original identity, validation result, approval |
| Submission result is ambiguous | Stop automatic retries | Acceptance is reconciled or a human authorizes the risk | Claim, trace, available receipt evidence, decision |
| Customer is no longer eligible | Suppress permanently | None for the same delivery revision | Eligibility checks and suppression reason |

This table separates retry from redrive. Retry is an automated response to a failure class already judged transient; redrive is a controlled reintroduction after the cause or uncertainty has been resolved. A DLQ is evidence storage and workload isolation, not a magic repair stage. It should preserve the job long enough for the team's response target, expose selective rather than all-or-nothing recovery, and keep original correlation data available when an item returns to the normal consumer.

## Governance defines the service boundary

For this weekly workload, compare three generic operating models. An in-process scheduler plus a database job table has few moving parts and supports transactional claims, but one team must own polling, leases, cleanup, and capacity. A self-operated queue can be appropriate when the team already runs its storage layer and understands its durability envelope, though that operational dependency is real. A managed regional messaging service reduces queue operations and can separate publishers from subscribers, while identity policy, regional availability, retention, and redrive controls still need verification. None of these choices removes the delivery ledger.

The catch is scope. A plain queue is not suitable when the digest is really a long-lived workflow with several dependent steps, compensation, human approvals, or waits spanning business processes; use a durable workflow model in that case. Conversely, a workflow engine can be unnecessary machinery for one weekly fan-out followed by one independently idempotent send per customer. Stick with a database-backed job table when volume is modest, the database is already highly available, and the team can operate fair polling without starving transactional traffic. Choose a separate queue when isolation, backpressure, or independent scaling justifies another subsystem. Your mileage may vary because on-call maturity is part of the architecture, even though it never appears on a feature matrix.

Cron is only the trigger. It is a time-based job scheduler, so use it to create a uniquely identified weekly campaign run; do not let every scheduler replica independently emit an unbounded customer fan-out. Acquire a durable campaign claim, snapshot or enumerate candidates with resumable checkpoints, and enqueue one logical delivery per eligible customer. If the scheduler starts twice, the campaign uniqueness constraint should turn the second start into a recorded no-op. Small system. Strong invariant.

## Rollout and rollback preserve the evidence

Begin with a shadow ledger for one digest cycle: derive delivery IDs, record intended transitions, and compare them with existing send outcomes without changing delivery. Next, enable claims for a low-risk cohort and inject duplicate messages, worker termination before and after submission, stale eligibility, and a corrected poison job. A successful test proves more than eventual queue emptiness; it shows one terminal business state per delivery ID and a complete audit chain from campaign creation through reconciliation.

Then grant redrive narrowly. Require an operator to select delivery IDs, state the resolved cause, attach an approval identity, and cap each batch so its result can be reconciled before the next batch. Deploy the consumer change before enabling the control, retain a rollback path that stops new claims without deleting evidence, and alert on growing review age rather than automatically draining uncertain work. Weekly digests tolerate delay better than duplicates sent after an unsubscribe, which makes restraint the correct delivery guarantee.

After one full cycle, review false transient classifications, unreconciled claims, regional data handling, queue retention against response time, and the operator steps that were unclear. Only then widen the cohort. The final selection criterion is mundane but defensible: the service must preserve stable identity, isolate poison work, allow selective redrive, expose enough correlation data for the ledger, and fit the team's actual operating boundary.

## Sources

- https://en.wikipedia.org/wiki/Cron
- https://cloud.google.com/pubsub/docs/overview
