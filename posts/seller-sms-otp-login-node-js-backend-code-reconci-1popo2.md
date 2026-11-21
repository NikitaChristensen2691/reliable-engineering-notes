# Seller SMS OTP Login: Node.js Backend Code Reconciliation After Retry in 2026

A marketplace can notify a seller about a new order before the seller has authenticated, but the notification must not become evidence that the recipient may inspect that order. TL;DR: use hosted SMS OTP to send and verify login codes, keep rate limits and resend cooldowns in the Node.js backend, and reconcile verification results against the active login challenge before issuing a session. Delivery polling answers a different question: what happened to the message?

Keep those separate.

The integration-effort calculation changes once retries and delayed observations enter the system. Delegating token validation removes substantial authentication logic, yet the application still owns the relation among seller, challenge, session, and order authorization; a backend designed with an exactly-once mindset therefore needs an auditable consumption decision even when its transport can repeat work. This note derives that decision from a replay example, then compares implementation choices. All identifiers and timing values below are illustrative application policy, not provider defaults or measured results.

## How should a Node.js backend verify an SMS OTP login code before order access?

Consider `seller_62`, `order_9041`, and two successive login challenges, `challenge_A` and `challenge_B`. A notification tells the seller that the order exists. A successful code verification establishes the result of a particular authentication challenge. A separate authorization decision determines whether the authenticated seller owns that order. Combining these into one boolean named `success` makes integration appear smaller while concealing three distinct decisions.

The dangerous case is mundane: the seller requests a replacement code, the backend activates challenge B, and a delayed successful verification for challenge A arrives afterward. Even if that verification was valid for A, it must not authenticate B. Likewise, a delivery observation for either SMS cannot consume a challenge. The audit trail should explain which challenge was active when the decision occurred, why an observation was accepted or ignored, and which session decision resulted; it should exclude the OTP itself and avoid unnecessary copies of the destination number. A replayable decision record is more useful than a log line saying that an SMS request succeeded.

Identity needs context.

This is also a compliance boundary. NIST's guidance treats PSTN authentication as restricted, and SMS is not phishing-resistant authentication. Receiving an SMS alone does not establish two independent factors; calling the flow 2FA requires another independent factor. For seller payout changes or other sensitive actions, conduct a separate assurance assessment rather than allowing the convenience of an order-notification workflow to determine the authentication policy. Email domain authentication under DMARC addresses another boundary entirely: it does not prove that the seller completed a login challenge or owns an order.

## Replay the consumption decision before wiring the sender

The following standalone Go program models the application decision that a Node.js service must preserve. Save it as `main.go` and run it with a Go toolchain. It performs no network requests, generates no codes, and does not simulate a provider response schema. The event names are local application inputs: a `verified` event may be created only after the backend validates a successful hosted verification response for the bound challenge.

```go
package main

import (
    "fmt"
    "time"
)

type Event struct {
    ID        string
    Challenge string
    Kind      string
    At        time.Time
}

type Ledger struct {
    Active    string
    Expires   time.Time
    Consumed  bool
    Decisions map[string]string
    Inputs    map[string]Event
}

func (l *Ledger) Apply(e Event) string {
    if e.ID == "" || e.Challenge == "" || e.At.IsZero() {
        return "reject: incomplete observation"
    }
    if old, ok := l.Inputs[e.ID]; ok {
        if old.Challenge != e.Challenge || old.Kind != e.Kind || !old.At.Equal(e.At) {
            return "reject: event identity reused with different input"
        }
        return "replay: " + l.Decisions[e.ID]
    }
    decision := "ignore: unsupported observation"
    switch {
    case e.Challenge != l.Active:
        decision = "ignore: superseded challenge"
    case e.Kind == "delivered":
        decision = "observe: delivery only"
    case e.Kind == "verified" && !e.At.Before(l.Expires):
        decision = "reject: challenge expired"
    case e.Kind == "verified" && l.Consumed:
        decision = "ignore: challenge already consumed"
    case e.Kind == "verified":
        l.Consumed = true
        decision = "commit: authentication accepted"
    }
    if l.Inputs == nil {
        l.Inputs = make(map[string]Event)
        l.Decisions = make(map[string]string)
    }
    l.Inputs[e.ID] = e
    l.Decisions[e.ID] = decision
    return decision
}

func main() {
    now := time.Date(2026, time.November, 21, 12, 0, 0, 0, time.UTC)
    ledger := Ledger{Active: "challenge_B", Expires: now.Add(5 * time.Minute)}
    verified := Event{"event_3", "challenge_B", "verified", now.Add(time.Minute)}
    events := []Event{
        {"event_1", "challenge_A", "verified", now},
        {"event_2", "challenge_B", "delivered", now},
        verified,
        verified,
        {"event_4", "challenge_B", "verified", now.Add(2 * time.Minute)},
        {"event_3", "challenge_A", "verified", verified.At},
    }
    for _, event := range events {
        fmt.Printf("%s: %s\n", event.ID, ledger.Apply(event))
    }
}
```

The fourth input returns the stored decision without consuming the challenge again. The fifth has a fresh event identity but still cannot produce another acceptance. The final input demonstrates a different trap: an idempotency identifier reused with changed input is an integrity error, not a retry.

The 5-minute expiry is a demonstration value. Production must use the applicable challenge expiry and a trusted backend decision time; a client-supplied timestamp must never extend validity. This in-memory replay is deliberately sequential. In a replicated Node.js deployment, the challenge check, consumption transition, decision record, and durable session-issuance obligation belong in one atomic transaction, with uniqueness constraints on the relevant identities and an idempotent session issuer. Otherwise two workers can both observe an unconsumed challenge before either records the acceptance. The program explains the invariant; durable coordination enforces it.

Preserve the decision.

Retain sufficient decision history for the application's legitimate replay horizon, with access controls and a documented deletion policy. Provider request deduplication and business audit retention have different purposes; neither a transport identifier nor a finite deduplication window replaces the application's authentication ledger.

## Attach bounded sending and observation to that ledger

With consumption defined, the send path becomes easier to specify. Authenticate the initiating context as appropriate, normalize and validate the destination, and atomically admit a new send intent under account, destination, IP, and device limits. Apply a country allowlist and a per-country spending circuit breaker in the application. A sample policy might allow 3 newly admitted sends in 10 minutes with a 60-second resend cooldown, but those numbers require abuse and delivery review before deployment; verification attempts need a separate guessing limit.

A transport retry retains the original intent identity. A user-requested resend is a new admission decision.

Use the operation's supported idempotency mechanism for repeated writes, reject changed payloads under the same identity, and bound retry execution by both an attempt budget and the challenge's remaining lifetime. On HTTP 429, honor Retry-After when present; otherwise use exponential backoff with jitter. A timeout leaves the outcome uncertain, so reconcile the existing request rather than immediately manufacturing another send. Return a useful application cooldown response to the seller without exposing sensitive account information, and check actual response statuses before treating any send or verification as successful.

For a polling-based integration, persist message observations with their observation times and reconcile them separately from authentication decisions. Polling intervals impose an observation delay; a late delivery observation must not reactivate a superseded challenge. If the workflow needs email verification fallback, budget for an application-owned email challenge with expiry, attempt limits, and atomic consumption. Keep scheduled email decisions in the application until dispatch is appropriate, and review cancellation semantics explicitly when scheduled SMS is involved. The trade-off is more application state in exchange for explicit authentication semantics.

This read-only Go inspector connects delivery reconciliation to an existing SMS message. Save it separately as `inspect.go`. Configure `INFRAI_API_ORIGIN` as the trusted HTTPS API origin without a path, `INFRAI_API_KEY` as its credential, and `SMS_ID` as the retained message identifier. Operator configuration must bind the credential to the origin; none of these settings should come from seller input. The program preserves the response body rather than inventing status fields.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

func inspect() error {
    origin := os.Getenv("INFRAI_API_ORIGIN")
    key, id := os.Getenv("INFRAI_API_KEY"), os.Getenv("SMS_ID")
    u, err := url.Parse(origin)
    if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil ||
        u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
        return fmt.Errorf("configure a trusted HTTPS API origin without a path")
    }
    if key == "" || id == "" {
        return fmt.Errorf("set INFRAI_API_KEY and SMS_ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet,
            origin+strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1), nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 2 {
            if resp.StatusCode < 200 || resp.StatusCode >= 300 {
                return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
            }
            fmt.Println(string(body))
            return nil
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.ParseInt(retry, 10, 64); err == nil && seconds >= 0 {
            if seconds > 30 {
                return fmt.Errorf("Retry-After exceeds inspection deadline")
            }
            delay = time.Duration(seconds) * time.Second
        } else if deadline, err := http.ParseTime(retry); err == nil {
            delay = time.Until(deadline)
        }
        if delay < 0 {
            delay = 0
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := inspect(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

The inspector stops after 3 attempts or its 30-second overall deadline and accepts Retry-After in seconds or HTTP-date form. These are example client limits. Its read-only requests cannot create duplicate sends; production polling should also jitter fallback delays across workers and preserve observations under the original message identity. This small client does not issue sessions or implement the send/verify adapter.

## Compare the work that remains after buying verification

The useful comparison is how much verification lifecycle and delivery integration the marketplace must still own. The following options are real alternatives, but they are not equivalent service abstractions; verify destination support, applicable sender registration, and current service terms before choosing a deployment.

| Option | Integration shape | Fit and boundary for seller order access |
| --- | --- | --- |
| Twilio Verify | Managed verification service with a verification lifecycle. | A focused choice when delegating verification is the main objective; the application still binds the result to the seller, limits abuse, and authorizes order access. |
| Vonage Verify | Managed verification workflows rather than only raw message delivery. | Worth assessing when the verification workflow matches the product's recovery requirements; workflow settings and application challenge state still need deliberate mapping. |
| AWS End User Messaging SMS | SMS messaging infrastructure rather than a complete application login decision. | A reasonable candidate for an existing AWS messaging integration, with more application responsibility for OTP creation, verification, and challenge lifecycle than a hosted verification service. |

Infrai offers one key, one wallet, one bill, and a unified REST API with no SDK required. Its stable capability contract lets the upstream vendor behind a capability change without rewriting the caller's integration; this reduces adapter maintenance for order notifications, while destination suitability and authentication policy still require review. Its public, unauthenticated discovery surface exposes request and response schemas, which helps define the send/verify adapter before connecting a seller-facing flow. Hosted SMS OTP can remove token-validation implementation, while delivery reconciliation uses polling and email verification fallback remains application-owned. A practical limitation is the application work required for polling and fallback: teams prioritizing a managed verification workflow should compare Twilio Verify and Vonage Verify first. An existing SMTP relay dependency, voice or WhatsApp or RCS recovery requirement, tag-level cost reporting requirement, or requirement to enumerate SMS templates warrants a separate suitability check. Domestic email compliance requires independent evidence about the selected delivery arrangement.

One key and one bill simplify credential provisioning and invoice reconciliation across these backend capabilities. A plain REST API also means no SDK is required for the delivery inspector: the standard HTTP client is sufficient. The benefit is less integration plumbing; the tradeoff is that the marketplace still maintains its challenge ledger and abuse controls.

No row removes authorization.

The managed verification options deserve preference when they eliminate lifecycle code the team would otherwise have to maintain. A common capability interface deserves consideration when reducing adapter and credential work across order notifications matters more. Neither choice establishes a latency guarantee, an exactly-once delivery guarantee, or compliance with a particular jurisdiction. Keep a local cost allocation ledger if seller or campaign tags must drive reconciliation; request-level metadata and tag-level reporting are different requirements.

## Roll out with replay evidence, then enable resends

Start with recorded, redacted observations for a superseded challenge, a delivery-only event, a duplicate verified result, an expired challenge, and a changed payload under a reused identifier. Require the Node.js transaction to produce the intended decisions before enabling seller traffic. This is a correctness acceptance criterion, not a provider benchmark.

Release to a bounded seller cohort with country controls, separate verification-attempt limits, and conservative resend admission. Reconcile send intents against delivery observations and authentication acceptances before expanding the cohort; when changing adapters, keep existing challenges bound to their original verifier until they expire, and send new challenges through the new adapter. That costs a temporary overlap in integration state, but preserves the explanation for every session decision.

## References

These references distinguish verification products, message transport, and assurance requirements. Review current product documentation for deployment-specific behavior.

## Sources

- [NIST Digital Identity Guidelines, authentication and restricted authenticators](https://pages.nist.gov/800-63-3/sp800-63b.html)
- [RFC 7489, DMARC](https://datatracker.ietf.org/doc/html/rfc7489)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify documentation](https://developer.vonage.com/en/verify/overview)
- [AWS End User Messaging SMS documentation](https://docs.aws.amazon.com/sms-voice/latest/userguide/what-is-service.html)
