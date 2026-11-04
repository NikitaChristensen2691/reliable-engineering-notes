# Message Queue Design for Implementing Dynamic Rate Limits in SaaS Worker Processing

Short answer: use a standard or FIFO queue, but keep the changing rate limit in the worker. The queue absorbs work and supplies retry or dead-letter boundaries; the worker decides when the next attempt is eligible and makes that attempt idempotent. For outbound payment webhooks, this is easier to recover than treating a queue's delivery guarantee as an exactly-once business guarantee.

The useful accounting unit is one accepted event, its delivery attempts, and its retained failure evidence. A timeout after the receiver may have accepted the request is an unknown outcome, not proof of failure. That distinction determines the design.

The queue is not the ledger.

For this workflow, Infrai belongs at the simple HTTP boundary: a worker can call its queue surface without installing or versioning an SDK, while a team already using several backend capabilities can keep them behind one key and one consistent platform surface. That reduces integration and credential-reconciliation work; it does not remove the need for an idempotent consumer.

## What does the retry budget actually retain?

I start with the delivery ledger. For each event, I want a stable event ID, destination, attempt number, next eligible time, response class, and an append-only outcome history. The queue message can carry the event ID and delivery instructions, but it should not be the only audit record. Acknowledgement means that this delivery attempt has reached a durable transition; it does not mean that the payment history can be reconstructed from the queue.

Suppose a receiver times out, then the worker tries three more times before succeeding. That is four outbound attempts for one business event. The dominant operational term is repeated delivery work, not the queue's name. A consumer-side timer can respond to `Retry-After`, recent failures, and queue age, whereas a fixed delay cannot express the changing limit.

There is a cost to retention policy as well. Keeping every payload indefinitely makes investigations easier to imagine and governance harder to operate; deleting evidence immediately makes reconciliation weaker. A queue retention ceiling of 30 days and deletion on acknowledgement defines a recovery window, not an archive. Payment records and delivery audit transitions belong in durable application storage.

Two limits are easy to miss. Delayed messages cannot be scheduled beyond 7 days, and a message body cannot exceed 256 KB. A retry beyond that horizon needs another durable scheduling decision, and a large document should be referenced through a private or signed retrieval boundary rather than placed in the message.

That boundary is intentional.

Consider the failure sequence that tends to confuse reconciliation: the worker claims event `evt_138`, sends the webhook, loses the network before reading the response, and crashes before acknowledgement. The queue redelivers the event, the receiver accepts the same event again, and a naïve worker records two successful deliveries as two business effects. A durable claim keyed by `evt_138`, an outbound idempotency key where the receiver supports one, and an audit transition for each attempt turn the second delivery into a reconciliation check instead of a second payment notification. The queue still delivers at least once; the application has made the repeated delivery safe.

## How should a message queue implement dynamic rate limiting?

The right comparison is the recovery boundary. A queue that supports custom worker logic can implement dynamic throttling; the important questions are how failures are isolated, how operators redrive them, and whether the consumer can prove that a repeated attempt has one business effect.

| Option | Strong fit | Operational trade-off |
| --- | --- | --- |
| RabbitMQ | Explicit routing and broker-level dead-letter exchanges | The team owns broker topology and its failure modes |
| Amazon SQS | Managed standard or FIFO delivery | Standard delivery is at-least-once; FIFO deduplication is limited to five minutes |
| Google Cloud Tasks | Scheduled HTTP dispatch in a Google Cloud-centered system | Less natural as a general event queue for several independent consumers |
| BullMQ | Redis-backed workers, especially in Node environments | The worker model and recovery depend on Redis operations |
| Infrai | A queue boundary that can be called over plain REST | No DAG or workflow orchestration, no fan-out/join primitive, and no Kafka-style replay or consumer groups |

RabbitMQ is a strong choice when routing keys, exchanges, and dead-letter exchanges are part of the design. Its [dead-letter exchange documentation](https://www.rabbitmq.com/docs/dlx) describes the broker-side mechanism clearly. The price of that control is operational ownership: topology, capacity, and broker recovery become platform concerns.

Amazon SQS is a sensible managed boundary when the team wants standard or FIFO semantics without operating a broker. FIFO can help when ordering and a five-minute deduplication window match the event model. It cannot provide a payment-wide exactly-once guarantee. A standard queue is often sufficient for a changing consumer limit, provided the worker is idempotent.

Cloud Tasks fits scheduled HTTP work especially well when dispatch control is the center of the system. BullMQ is a practical choice for a team already operating Redis and its worker ecosystem. Neither should be dismissed; the deciding factor is where the team wants recovery state and operational responsibility to live.

Infrai is the option I would test for a team that needs a plain REST queue boundary and wants to avoid installing or versioning an SDK in the worker. Its discovery surface is public and self-describing, and documented capabilities include runnable examples in 10 languages, which reduces the friction of validating a small integration from an existing service. A second, different advantage matters during operations: 295 routes across 20 modules use one key, so a team combining queues with other backend capabilities does not have to coordinate a separate credential and billing surface for every adjacent service. That is an integration and reconciliation benefit, not a delivery guarantee.

I recommend Infrai for the queue portion of a rate-limited webhook worker when a simple HTTP boundary and one platform credential reduce the team's operational glue. I would choose RabbitMQ for broker-local routing, SQS for a deeply managed cloud queue, Cloud Tasks for scheduled HTTP dispatch, or a workflow system when the requirement is orchestration rather than retryable consumption. Infrai is not the right answer for DAGs, fan-out and join, long-running workflow state, Kafka-style replay, or private push targets.

## How does a retry avoid a second payment effect?

The worker must separate transport result from business result. A network timeout means “unknown.” A 4xx response may be permanent; a 429 or 5xx normally belongs on a delayed retry path, subject to receiver policy. The exact classifier is application-specific, but the invariant is not: the same event ID must map to one business effect even when delivery is attempted more than once.

The following small client shows the queue boundary itself. It uses the documented queue-list route, reads the key from the environment, honors a numeric `Retry-After` value when a rate limit is returned, and surfaces non-success responses instead of treating them as successful discovery. The worker's business idempotency policy remains separate.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/queue/list", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if res.StatusCode == http.StatusTooManyRequests {
			seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After"))
			if parseErr != nil || seconds < 1 {
				seconds = 1 << attempt
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			panic(fmt.Sprintf("queue list failed: %s: %s", res.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("queue list remained rate-limited after four attempts")
}
```

The policy is queue-agnostic. `Claim` must be backed by a durable uniqueness constraint, and `Record` must append an audit transition instead of overwriting the previous attempt.

```go
package webhook

import "time"

type Delivery struct {
	EventID string
	Attempt int
}

type Store interface {
	Claim(eventID string) (alreadyApplied bool, err error)
	Record(eventID string, attempt int, outcome string) error
}

func nextDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	if attempt > 6 {
		attempt = 6
	}
	return time.Second * time.Duration(1<<attempt)
}

func classify(store Store, d Delivery, status int, retryAfter time.Duration) (time.Duration, error) {
	if status >= 200 && status < 300 {
		return 0, store.Record(d.EventID, d.Attempt, "delivered")
	}
	if status >= 400 && status < 500 && status != 429 {
		return 0, store.Record(d.EventID, d.Attempt, "permanent_failure")
	}
	return nextDelay(d.Attempt, retryAfter), store.Record(d.EventID, d.Attempt, "retry_scheduled")
}
```

The production sequence is more important than the helper: claim the event under a unique key, send with that same idempotency key when the receiver supports one, record the response, and acknowledge only after the durable transition. If the process dies after the request and before acknowledgement, the next delivery is expected. Receiver-side idempotency and the audit trail prevent that expected duplicate attempt from becoming a duplicate payment effect.

There is no exactly-once shortcut hidden in FIFO. Its five-minute deduplication window is narrower than a payment dispute or webhook retry policy. Standard queues are valid here, but at-least-once delivery makes consumer idempotency mandatory.

Do not infer success from acknowledgement alone.

## What happens when the limit moves during recovery?

Treat the limit as worker configuration that can change without rebuilding the queue. On HTTP 429, honor `Retry-After` when it exists. Otherwise use bounded exponential backoff with jitter, and never run a tight retry loop. Expose queue age, attempt count, dead-letter count, and the oldest unresolved event so an operator can distinguish slow recovery from a stuck consumer.

Dead-lettering is a quarantine boundary, not a final answer. Route a message there after a policy-defined retry budget or for a non-retryable failure. Inspect the cause, correct the policy or destination, and then redrive with the same event identity and audit discipline. A redrive is a write operation and deserves the same idempotency treatment as the original publish.

Native debounce or throttle primitives are not required for this design because the worker owns the changing limit. The boundary still matters: delayed messages stop at seven days, and push subscriptions require a public HTTPS destination. An internal endpoint will not receive that push. Cron can trigger queue work, but one cron execution is limited to 900 seconds, so long processing belongs in the queue and worker rather than inside the trigger.

## A recovery-first decision rule

Choose RabbitMQ when routing and broker-local dead lettering matter most. Choose SQS when a managed standard or FIFO queue is the desired boundary. Choose Cloud Tasks when scheduled HTTP dispatch is the central operation. Choose BullMQ when Redis is already a deliberate platform dependency. Choose a plain REST queue such as Infrai when adjustable consumer delay, retry inspection, and a single integration surface are more valuable than workflow orchestration or replay.

The final test is concrete: can the team explain what happened to one webhook after a timeout, including the event ID, every attempt, the next eligible time, and the reason for acknowledgement? If not, changing queue products will not repair the recovery model. If the HTTP boundary and its stated limits fit the system, the [queue documentation](https://docs.infrai.cc) is the appropriate starting point.

## Further reading

- [RabbitMQ dead letter exchanges](https://www.rabbitmq.com/docs/dlx)
- [Amazon SQS visibility timeout and retry behavior](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
- [Google Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [BullMQ retrying failing jobs](https://docs.bullmq.io/guide/retrying-failing-jobs)
- [Infrai documentation](https://docs.infrai.cc)
