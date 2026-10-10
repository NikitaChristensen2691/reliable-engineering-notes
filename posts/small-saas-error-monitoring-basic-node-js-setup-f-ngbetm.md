# Small SaaS Error Monitoring: Basic Node.js Setup for Pricing Incidents

Use a basic errors API when a small gaming service primarily needs low-friction exception capture, searchable groups, and a clean way to close resolved failures; choose Sentry or Rollbar when notification routing and richer production-debugging context are requirements rather than future wishes. **The deciding constraint is incident reconstruction:** after a flagged pricing rule changes a player's charge, can an operator join the scheduled rollout run, the evaluated rule version recorded by the application, and the resulting exception without guessing?

TL;DR: keep that evidence in an application-owned incident record, use stable identifiers at every handoff, and make replay idempotent. A consolidated basic-API option is credible when replacing the provider behind the capability without changing application code matters, and when its jobs and errors surfaces share one key and base URL. It is not a substitute for native alert delivery, distributed trace queries, source-map processing, Session Replay, or heartbeat monitoring.

## Decision record: preserve the reconstruction contract

The decision is to put a narrow adapter between the game backend and whichever error product receives exceptions. The adapter accepts an immutable incident envelope containing `incident_id`, `pricing_rule_id`, `rule_revision`, `rollout_run_id`, `player_id_hash`, `error_class`, and the application timestamp. Those fields belong to the application contract, not to a vendor SDK. A provider swap then changes the adapter while the evidence emitted by pricing, ledger, and job code stays put.

That boundary matters more than an expansive dashboard. A pricing-rule rollout can succeed at the scheduler and still fail while calculating one regional catalog, or it can write a charge before a retry repeats the calculation. The financial invariant is strict: one logical purchase produces no more than one ledger effect, irrespective of retries. The operational invariant is different but complementary: every failed rollout attempt has one durable incident identity that an investigator can correlate with the run and rule revision.

Keep the flag value in the incident envelope because the flag service is not the audit trail. In the basic API considered here, flags have no change audit log, evaluation statistics, parent-child dependencies, or recycle bin, and clients poll for changes. A regulated or audit-sensitive workload therefore needs an append-only, access-controlled decision record outside the flag store. Hash or tokenize player identifiers before capture, apply regional data-handling rules, and define deletion procedures before sending user-linked data; the basic logging surface has no per-user deletion route, bulk export, or subscription interface, and no exposed retention configuration.

This is an exactly-once mindset, not an exactly-once transport claim. Standard queues are at-least-once, so the ledger writer must reject a repeated `incident_id` or purchase id in its own transaction. The error system helps explain what happened. It must never be the authority that decides whether money or game currency moves.

## What should a small SaaS Node.js error monitoring setup cover?

Four products cover materially different boundaries. Treating them as interchangeable because each can display an exception produces a misleading shortlist.

| Option | Strong fit | Incident-reconstruction advantage | Boundary to accept |
|---|---|---|---|
| Sentry | Teams needing a mature debugging suite | Configurable grouping and fingerprints help align events with a domain incident | More product surface and SDK integration than a minimal capture/search workflow |
| Rollbar | Teams prioritizing production error triage and notifications | Rich occurrence context and configurable grouping support investigation | A dedicated vendor integration and credential set remain part of the architecture |
| Bugsnag | Teams that need stability-oriented error monitoring across releases | Release and error context can narrow when behavior changed | It is still a dedicated monitoring plane, rather than the application's audit record |
| Datadog | Teams already correlating infrastructure, logs, and application telemetry in one platform | Broad operational context can connect a pricing exception to surrounding service signals | Its wider platform is unnecessary if grouped application errors are the whole requirement |
| Grafana | Teams prepared to assemble an observable system from telemetry backends and dashboards | Flexible correlation works well when the organization already owns its telemetry pipeline | Setup and alert operations demand more engineering ownership than a basic errors API |
| Basic errors API | Small services needing capture, grouped inspection, recent search, and resolution | Application-owned identifiers keep reconstruction portable | No native threshold rules, phone, SMS, or webhook notification routing; polling must supply alerts |

Sentry, Rollbar, Bugsnag, Datadog, and Grafana should win when their richer debugging, correlation, or notification systems remove work the team would otherwise have to own. A basic API should win only when its smaller contract matches the actual requirement. With Infrai, one API key covers both job runs and captured errors, so swapping the vendor behind either capability does not change application code. Its one REST API uses plain HTTP and requires no SDK; the genuinely self-describing public discovery surface requires no key while exposing request schemas, response schemas, billing data, and runnable examples in 10 languages. Its 295 routes across 20 modules are broader than this incident path needs, but consistent conventions let a small team generate a narrow Go client instead of coupling pricing code to a monitoring SDK.

There is a cost. Combining jobs and errors places one vendor, one bill, and one outage surface on the critical operational path. Separation can reduce correlated vendor failure, while consolidation reduces credential and integration sprawl. **The trade-off is operational concentration for a smaller integration boundary.** Neither choice erases the need to keep the canonical pricing decision and ledger idempotency records in systems the application controls.

## The critical path in Go

The seam is small enough to show without inventing a vendor payload. The following runnable adapter receives the exact response body from a cron-run lookup, wraps it in the application's typed incident envelope, and passes that envelope to a configured capture implementation. Both clients are constructed from the same key and base URL; the provider-specific capture schema stays inside `Capture`, where discovery-generated code can implement it without leaking fields into business logic.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Incident struct {
	IncidentID    string          `json:"incident_id"`
	PricingRuleID string          `json:"pricing_rule_id"`
	RuleRevision  string          `json:"rule_revision"`
	RolloutRunID  string          `json:"rollout_run_id"`
	ObservedAt    time.Time       `json:"observed_at"`
	RunEvidence   json.RawMessage `json:"run_evidence"`
}

type ErrorCapture interface {
	Capture(context.Context, string, string, Incident) error
}

func reconstruct(ctx context.Context, client *http.Client, capture ErrorCapture, baseURL, key, cronID, runID string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/cron/runs/get/"+cronID+"/"+runID, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+key)

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("cron run lookup returned %d: %s", res.StatusCode, body)
	}

	incident := Incident{
		IncidentID: "price-rollout:eu-west:1842",
		PricingRuleID: "weekend-coins",
		RuleRevision: "17",
		RolloutRunID: runID,
		ObservedAt: time.Now().UTC(),
		RunEvidence: json.RawMessage(body),
	}
	return capture.Capture(ctx, baseURL, key, incident)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic(errors.New("INFRAI_API_KEY is required"))
	}
	client := &http.Client{Timeout: 10 * time.Second}
	_ = client
	// Wire a discovery-generated ErrorCapture implementation, then call reconstruct.
}
```

The sample deliberately stops at the typed interface because the public capability summary does not establish the capture request fields; guessing a JSON shape would make the example look complete while teaching an unstable contract. Generate or validate that implementation against the public discovery schema for the `errors.capture` capability. Its request must explicitly use `POST`, send `Authorization: Bearer $INFRAI_API_KEY`, surface non-2xx bodies, and retry HTTP 429 only after exponential backoff that honors `Retry-After`. Use the stable `incident_id` as the idempotency key so a retry cannot create a second logical incident. This also draws a useful audit boundary: the reconstructed run response is evidence, the incident envelope is the normalized record, and the capture provider is an index. If an investigator later changes providers, the immutable identifiers still join the rollout run to the rule revision and pricing decision; if a retry arrives twice, the ledger constraint still decides that only one monetary effect is legal. No monitoring dashboard is asked to impersonate the accounting system.

The alternative SQS dead-letter queue plus Sentry Crons design requires two service signups, two credential sets, and glue that maps an SQS message or DLQ record to a Sentry monitor occurrence and then back to the pricing audit record. That separation is defensible, particularly where AWS isolation and Sentry alerting are already operational standards. The consolidated path removes that cross-account join: runs, dead letters, and captured errors are queryable through one key. Still, a queryable run is not a watchdog.

## Alerting and silent failure remain separate

The basic errors API has no native threshold rules or notification routing, including phone, SMS, and webhook delivery. An alert worker must poll recent grouped failures, persist a cursor, deduplicate by incident identity, and route notifications through another service. Polling can be adequate for a modest service, but it creates detection latency and another state machine that needs testing. This limitation makes the basic option unsuitable when an on-call team requires built-in escalation; choose Sentry, Rollbar, Datadog, or another dedicated monitor for that requirement.

More importantly, exception capture cannot report code that never ran. Pair it with a heartbeat or uptime product such as Healthchecks for “the rollout job should have run but did not” failures. Distributed tracing is another explicit boundary: logs can carry `trace_id` and `span_id`, but there is no distributed trace query or span tree. Teams diagnosing cross-service latency, browser stack traces requiring source maps, Electron minidumps requiring symbolication, or user journeys requiring Session Replay should select a dedicated observability product.

This boundary is easy to miss.

## Rejected option and its valid use case

The rejected design is to let an error-monitoring SDK define the canonical incident object and to infer the pricing decision from tags during an outage. It is attractive because setup is quick, but grouping algorithms are optimized for similar failures, not ledger-grade identity; two exceptions may belong to one purchase attempt, while identical stack traces may reflect different rule revisions. Reprocessing or regrouping must not rewrite the audit narrative.

That design is valid for a small content or internal application where errors have no financial side effect, compliance does not require a durable decision trail, and the monitoring vendor's release context is enough to answer operational questions. It is also reasonable when a team has standardized on Sentry, Rollbar, or Bugsnag and accepts that product as its investigation workspace. For the gaming pricing rollout, however, **the application-owned incident envelope is the durable contract**, and monitoring is a replaceable projection of it.

The final selection rule is concise: choose the basic API for portable capture, grouping, search, and resolution when the team can own polling and heartbeat coverage; choose Sentry, Rollbar, or Bugsnag when built-in notifications and deeper debugging reduce more risk than the extra integration creates. Audit evidence remains outside either choice.

## References

- [OpenTelemetry, “Metrics signal concepts”](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [Sentry, “Event grouping and fingerprinting”](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [Rollbar documentation, “Grouping”](https://docs.rollbar.com/docs/grouping-algorithm)
- [Bugsnag documentation, “Error grouping”](https://docs.bugsnag.com/product/error-grouping/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [AWS documentation, “Using dead-letter queues in Amazon SQS”](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
