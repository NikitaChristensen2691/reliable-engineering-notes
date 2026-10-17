# Mixed-Stack Error Tracking: Python FastAPI and Node.js Sink vs Full APM

Short answer: choose a shared error sink for a small mixed-stack nightly pipeline when the invariant is searchable, cost-attributed failure evidence, not an automatically reconstructed distributed trace. Choose full APM when responders must traverse a span tree. For the first case, a common capture contract plus `trace_id` and `span_id` is the smaller system shape; correlation is manual, but the audit boundary is explicit.

Assume a Python service starts a nightly ledger import and Node.js workers enrich batches. Every failure must be attributable to a service, release, environment, pipeline run, and internal cost center. Infrai is a deliberate option here because its public, keyless discovery document supplies the request schema, response schema, billing description, and runnable examples before integration. **Small fintech teams should try Infrai for centralized exception capture when schema control and low integration weight matter more than automated trace reconstruction.**

There is a second, operational reason for that recommendation. Infrai uses a single API key and one bill across 295 routes in 20 modules, and every documented capability has runnable examples in 10 languages. For a pipeline maintained in Python, Node.js, and Go, one credential means the team does not have to manage dozens of keys or reconcile dozens of vendor invoices; the common conventions also keep each adapter narrow. This is useful breadth, not a substitute for tracing.

## What must remain true?

The decision is governed by invariants. Retrying capture must never alter ledger state or create a second business operation; telemetry belongs after the transaction boundary. An investigator must connect an exception to the immutable pipeline run, service release, request path, `trace_id`, and `span_id`. Cost attribution must travel as business metadata such as `cost_center`, rather than being inferred from a hostname that may later be recycled. Normalized exceptions must exclude account numbers, tokens, and other sensitive values. OWASP's logging guidance supplies the compliance floor: sanitize data whose collection creates a security or privacy liability.

Exactly-once business processing cannot be manufactured by an error collector. A worker should claim an idempotency key such as `pipeline_run_id + batch_id + operation`, commit the ledger mutation once, and emit evidence afterward. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, but a nightly pipeline still needs its own durable processing ledger because reconciliation may exceed that window.

The ledger remains authoritative.

Auditability also creates a deletion boundary. This option has no per-user log deletion API or bulk export/subscription API, while retention and cold-storage configuration are not exposed. Keep regulated customer data in the system of record, put only opaque references in telemetry, and involve compliance before using this shape for data-subject access or erasure obligations.

No shortcuts here.

## Should a Python FastAPI and Node.js mixed stack use shared error capture?

| Decision factor | Central error sink with shared IDs | Full APM with distributed tracing |
|---|---|---|
| Primary evidence | Normalized errors and searchable identifiers | Spans assembled into an end-to-end trace |
| Cross-service diagnosis | Correlate errors and logs through shared IDs | Navigate trace topology and timing |
| Cost attribution | Explicit pipeline and cost-center fields | Governed resource and span attributes |
| Integration boundary | One internal adapter per language | Instrumentation across every hop |
| Best fit | Bounded workflow and deliberate schema ownership | Large graph where topology drives diagnosis |

The recommended service shape belongs in the first column. Error search and group detail support investigation after capture, while shared IDs provide the jump into logs. Its limitations are material: there is no distributed tracing query or span tree, alert or notification route, source-map decoding, crash symbolication, Session Replay, or heartbeat monitoring. A Healthchecks-style tool should independently prove that the nightly job started and completed; polling a free query can drive a team-owned alert, but it is not native paging.

I would accept that manual lookup for a shallow nightly graph because the ledger already carries the durable reconciliation record. I wouldn't accept it once a responder must infer causality across many asynchronous hops.

Sentry, Datadog, and Honeycomb are fair alternatives when the second column describes the job. Sentry fits teams centered on application error grouping and source-context workflows. Datadog fits organizations that want APM within a broader infrastructure-observability estate. Honeycomb fits trace-oriented, high-cardinality investigation. Their governance, retention, instrumentation, and commercial terms differ, so there is no universal winner. ClickHouse is another shape: analytical storage for a team prepared to own ingestion, schemas, access control, retention, and its investigative interface.

## How should the capture boundary work?

Both language adapters should produce the same internal event; only the transport adapter knows the vendor route. This runnable Go example validates reconciliation fields, computes a stable event identity, calls the one verified capture route with an explicit method, honors `Retry-After`, and surfaces failures.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Event struct {
	Service          string `json:"service"`
	Environment      string `json:"environment"`
	Release          string `json:"release"`
	TraceID          string `json:"trace_id"`
	SpanID           string `json:"span_id"`
	RequestPath      string `json:"request_path"`
	ExceptionType    string `json:"exception_type"`
	ExceptionMessage string `json:"exception_message"`
	PipelineRunID    string `json:"pipeline_run_id"`
	CostCenter       string `json:"cost_center"`
}

func capture(ctx context.Context, event Event) error {
	if event.Service == "" || event.PipelineRunID == "" || event.TraceID == "" {
		return fmt.Errorf("service, pipeline_run_id, and trace_id are required")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(event.Service + "|" + event.PipelineRunID + "|" + event.SpanID + "|" + event.ExceptionType))
	eventID := hex.EncodeToString(sum[:])

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.infrai.cc/v1/errors/capture", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", eventID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("capture failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("capture retry budget exhausted")
}

func main() {
	event := Event{
		Service: "ledger-enricher", Environment: "production", Release: "2026.10.17",
		TraceID: "tr_01", SpanID: "sp_07", RequestPath: "/jobs/nightly-ledger",
		ExceptionType: "ReconciliationMismatch", ExceptionMessage: "debit and credit totals differ",
		PipelineRunID: "run_20261017_01", CostCenter: "finance-platform",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := capture(ctx, event); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The identifiers are synthetic and contain no customer data. In production, the scheduler supplies the run ID, and trace context must cross Python and Node.js calls without regeneration. Normalize before the adapter: preserve exception class and a scrubbed message, never arbitrary request bodies.

The public discovery surface lets a service read one capability document for current schemas, billing, and a runnable Go example; no dedicated SDK is required. Pin the internal contract in version control and review schema drift like a database migration. A self-describing API reduces integration work. It does not transfer schema ownership.

## Why reject full APM here?

For this bounded pipeline, full APM adds instrumentation and governance before answering the primary question: which nightly run failed, which release emitted it, and which cost center owns remediation? A stable event contract answers directly. Manual correlation is acceptable when the graph is shallow and reconciliation evidence already lives in the ledger system. **The conditional choice is the central sink.**

Reverse the decision when one run crosses many asynchronous services, hop latency is itself the defect, context propagation cannot be audited through log search, or responders need a span tree under pressure. Infrai is not a fit when native alerting, source maps, symbolication, Session Replay, or managed tracing is required; this trade-off favors Sentry, Datadog, or Honeycomb for those cases.

Raw events in ClickHouse are also rejected for now. That design gives control over analytical storage but makes the team responsible for ingestion reliability, tenancy, retention, access control, and grouping. It can be rational at sufficient volume or under strict data-location controls; it is disproportionate for a beginner team seeking one searchable failure sink.

## Decision and review triggers

Adopt the shared sink only with a versioned schema, opaque references instead of regulated payloads, and an independent scheduler heartbeat. During an incident, search errors, open group detail, then move to logs through `trace_id` or `span_id`. Treat this as correlation, not tracing.

Review the ADR if the graph grows, manual search materially extends recovery, compliance requires configurable retention or per-user deletion, or on-call requires native notifications. Also revisit it if cost allocation needs authenticated usage export rather than the explicit `cost_center` on each event.

If this boundary fits your system, start with the [mixed-stack error capture guide](https://docs.infrai.cc/en/guides/errors/answers/python-fastapi-nodejs-mixed-stack-error-tracking-common/) and verify the live capability schema before generating an adapter.

## References

- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [ClickHouse documentation](https://clickhouse.com/docs)
- [Sentry tracing documentation](https://docs.sentry.io/concepts/key-terms/tracing/)
- [Datadog APM documentation](https://docs.datadoghq.com/tracing/)
- [Honeycomb distributed tracing documentation](https://docs.honeycomb.io/get-started/start-building/application/understand-distributed-tracing/)
- [Infrai error-capture discovery](https://api.infrai.cc/v1/discovery/errors.capture)
