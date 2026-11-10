# Property Notification Kill Switch: 3 Signals That Justify Production Rollback

A property notification service must contain delivery failures without turning every delayed receipt into an incident. Resolve that trade-off by separating delivery evidence, retry eligibility, and the operator's containment decision: a feature flag can stop the risky sending path while reconciliation continues. Short answer: use a dedicated production kill switch, retain an application-owned audit trail, and require a deliberate decision or an application-built incident workflow to change it. The flag provides a control point; trustworthy evidence determines when to use it.

## When should a feature flag kill switch trigger production rollback?

Consider a proposed workflow for maintenance notices sent to residents. A send attempt, a delivery observation, and an authorization to retry represent different facts, even when they share the same notification identifier. Counting them as one success or failure creates noise: an uncertain outcome can become another send, and repeated observations can inflate the apparent incident. Preserve those distinctions before attaching any automatic action to a threshold. Otherwise, the mechanism intended to contain a problem can manufacture the evidence that keeps it active.

Uncertainty needs reconciliation.

Use 3 evidence boundaries. First, preserve the delivery event associated with the provider message identifier. Second, record the application's decision about whether another attempt is permitted. Third, record the kill-switch decision, its owner, and its reason. These are proposed application records, not additional provider response fields. Their purpose is to make an incident reconstructable without treating telemetry delivery as proof that the resident received a notice.

The useful signal is a distinct notification whose outcome requires intervention. A raw event count measures something else. Repeated polling, retries, and duplicated observations should retain their evidence while contributing only once to the particular decision being evaluated. Exactly-once is the correctness objective for the business action; it must not become an unsupported claim about the delivery transport.

## Separate containment from reconciliation

Place a dedicated feature flag before each risky integration or sending path, and give it an owner and an unambiguous meaning, such as permitting new maintenance-notice sends. When permission is withdrawn, the application should take its chosen safer path, such as retaining work for review; it should continue collecting outcomes for earlier attempts. Stopping evidence collection together with sending would make the apparent recovery difficult to interpret.

A flag change doesn't unsend a message.

For a polling client, the effective containment interval depends on the application's polling schedule and its handling of stale or unavailable flag values. Define that policy explicitly. A service that continues dispatching indefinitely on an old permissive value has made a different safety decision from one that pauses new sends after its configured freshness limit, and neither policy should emerge accidentally from an HTTP client's default behavior. Avoid publishing a propagation guarantee without measurements.

Idempotency needs a business identifier that survives retries. Keep the logical notification identity stable, distinguish individual attempts, and retain the evidence used to authorize a retry. The same discipline applies to operational writes: a retry of an audit-record submission should carry the same idempotency key. This does not make a send and an observation write atomic. Reconciliation must cover the interval between them.

Treat flag ownership, change reasons, dependency knowledge, evaluation records, and recovery approval as application responsibilities in this design. Alert evaluation and notification routing belong to an explicit incident workflow, with an operator as the initial decision-maker. A heartbeat check serves a separate purpose: detecting a worker that produced no evidence at all. Neither a quiet event stream nor a flag evaluation proves that scheduled work actually ran.

## Carry delivery evidence across the API boundary

Infrai fits this seam through public, self-describing REST API discovery with runnable examples, plus one key and one bill for SMS and observability: a single API key carries delivery evidence into operational records without provisioning a second service account. Inspecting the schemas makes the integration an endpoint-reading task rather than another SDK to learn. The trade-off is concentrated trust, billing, and the outage surface in one vendor.

The following Go program reads SMS events and submits their JSON representation to log ingestion using the same key and base URL. It leaves the ingestion envelope to configuration because the exact request fields must come from the capability's discovery schema. The example does not guess a log field name or an event response shape.

Configure `INFRAI_BASE_URL` with the service's documented HTTPS base URL, including its version suffix, and `INFRAI_API_KEY` with your key. Set `SMS_MESSAGE_ID` to an existing message identifier. Prepare `LOG_INGEST_TEMPLATE` as a schema-valid request object whose appropriate string field contains the marker `__SMS_EVENTS__`; the program inserts the event response as escaped JSON text. Keep `EVIDENCE_WRITE_KEY` stable when retrying the same observation write. These are local configuration variables, not API parameters.

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func required(name string) string {
    value := os.Getenv(name)
    if value == "" { log.Fatalf("missing %s", name) }
    return value
}

func call(c *http.Client, method, target, key, id string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(method, target, bytes.NewReader(body))
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if body != nil { req.Header.Set("Content-Type", "application/json") }
        if id != "" { req.Header.Set("Idempotency-Key", id) }
        resp, err := c.Do(req)
        if err != nil { return nil, err }
        data, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if next := time.Duration(seconds)*time.Second; next > delay { delay = next }
            } else if deadline, err := http.ParseTime(retry); err == nil {
                if next := time.Until(deadline); next > delay { delay = next }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s: HTTP %d: %s", method, resp.StatusCode, data)
        }
        return data, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    base := strings.TrimRight(required("INFRAI_BASE_URL"), "/")
    parsed, err := url.Parse(base)
    if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
        log.Fatal("base URL must use HTTPS")
    }
    key := required("INFRAI_API_KEY")
    messageID := required("SMS_MESSAGE_ID")
    writeKey := required("EVIDENCE_WRITE_KEY")
    template := required("LOG_INGEST_TEMPLATE")
    if !json.Valid([]byte(template)) || strings.Count(template, "__SMS_EVENTS__") != 1 {
        log.Fatal("provide valid request JSON with exactly one event marker")
    }
    client := &http.Client{Timeout: 20*time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error {
        return http.ErrUseLastResponse
    }}
    events, err := call(client, http.MethodGet, base+"/sms/events/"+url.PathEscape(messageID), key, "", nil)
    if err != nil { log.Fatal(err) }
    if !json.Valid(events) { log.Fatal("event response is not valid JSON") }
    escaped, err := json.Marshal(string(events))
    if err != nil { log.Fatal(err) }
    body := []byte(strings.Replace(template, "__SMS_EVENTS__", string(escaped[1:len(escaped)-1]), 1))
    if !json.Valid(body) { log.Fatal("event marker must occur inside a JSON string value") }
    if _, err := call(client, http.MethodPost, base+"/logs/ingest", key, writeKey, body); err != nil {
        log.Fatal(err)
    }
    fmt.Println("Delivery evidence submitted.")
}
```

Save the program as `main.go` and run it with Go after configuring those variables. Its request timeout (20 seconds) and rate-limit budget (5 attempts) are illustrative client policies, not service guarantees. Request paths follow discovery path fields. Confirm the ingestion envelope there before running; no undocumented search filters are assumed.

This is an evidence handoff, not a complete rollback controller. The caller must verify response semantics, approve the payload under its data policy, and decide whether an ingestion acknowledgment satisfies its audit requirements. The program forwards the event response into the configured envelope, so use it only with data approved for that destination. Production reconciliation still needs a durable record of observations submitted and those pending. A successful HTTP response does not establish exactly-once business delivery.

## Compare the work left to the application

The useful comparison is the evidence boundary each tool helps establish. An SMS service, an observability service, an error tracker, and a heartbeat monitor address different parts of the notification problem; none proves that the application's retry policy preserves correctness. Choose from the workflow outward.

| Option | Appropriate evaluation scenario | Boundary the application must still define |
| --- | --- | --- |
| Twilio | A separately selected SMS delivery service | Mapping message evidence to notification and attempt identities. |
| Datadog | A team evaluating a dedicated operational observation destination | Distinguishing intervention-worthy signals from repeated observations. |
| Sentry | An investigation centered on application errors | Connecting an error to a business notification and its retry decision. |
| Grafana | A team evaluating operational views alongside its telemetry choices | Defining the underlying evidence and the decision that a view supports. |
| Healthchecks | Detecting expected worker activity that failed to occur | Keeping missed work distinct from failed resident delivery. |

A Twilio plus Datadog stack requires 2 service signups and 2 sets of service credentials. The application supplies the glue that carries message identifiers and delivery evidence into operational records, handles failed writes, deduplicates observations, and connects an incident decision to the sending flag. This separation can suit a team that wants independent service choices and already operates that integration. The combined API approach suits a team prioritizing a smaller integration boundary, provided it accepts responsibility for polling, incident automation, and its own audit evidence. Neither arrangement removes that work.

Compliance narrows the decision further. Minimize resident identifiers, phone numbers, and notice content in operational records; define access, retention, export, and deletion requirements before approving the destination. OWASP's logging guidance is a useful starting point. Don't infer that a shared API key establishes compliance with a retention policy or a resident's deletion request. Those requirements need an explicit review of the chosen evidence store and the application's own records.

A trace identifier supports correlation. It should not be treated as a distributed trace query, a span tree, or proof of delivery; crash symbolication and session replay answer different questions from this backend containment design.

## Roll out the decision rule before automatic rollback

Start with observation and manual control. Replay a test notification's evidence through reconciliation, including duplicate observations, an uncertain send outcome, a rate-limited evidence write, and a worker that stops producing updates. These are proposed acceptance cases, not reported incidents or measured results.

Then rehearse disabling the risky path while continuing to reconcile earlier attempts. Require a named owner to approve recovery, preserve the change reason alongside the application audit trail, and verify that re-enabling the flag cannot authorize duplicate business actions. Introduce automatic containment only after the team can explain its signal, its freshness assumptions, and the cases that still require human judgment.

## References

The sources below support review of the separate operational responsibilities.

- [OWASP Logging Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
- [Twilio messaging documentation](https://www.twilio.com/docs/messaging)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Sentry documentation](https://docs.sentry.io/)
- [Grafana documentation](https://grafana.com/docs/)
- [Healthchecks documentation](https://healthchecks.io/docs/)

## Sources

- Logging security and sensitive data: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- Alternative-stack messaging reference: https://www.twilio.com/docs/messaging
- Alternative-stack observation reference: https://docs.datadoghq.com/logs/
