# Passwordless Welcome Email Links: Retaining Evidence for Each Transactional Template Send

The bill for an e-commerce password-reset or passwordless welcome email has several terms: transactional delivery, repeated attempts to send a verification link, retained message content, and the work required to reconcile those attempts. Consider an illustrative workload of 100,000 attempts per day, a 2,000-byte retained message per attempt, and 30 days of retention. Message content alone occupies 6 GB before indexes, replication, or backups. That is the dominant storage term in this example; it is not a claim about the dominant monetary charge on a real provider invoice. Delivery charges and operational labor require separate measurements.

Short answer: send the welcome or reset email transactionally, generate and validate its signed verification or magic-link token in the application, and retain a compact evidence record instead of the usable link. A proposed 256-byte record at the same volume and retention occupies 0.768 GB before database overhead. The useful optimization is to stop retaining credentials while preserving enough evidence to explain an authorization decision.

Keep evidence deliberately.

## What evidence should a passwordless welcome email link leave behind?

For this design, a five-minute validity period is an application policy example, not a provider guarantee or a compliance requirement. The welcome email establishes control of an address; the password-reset email authorizes a different operation. A passwordless sign-in link has a third purpose. Even if all three use one template system, their signed tokens should bind the intended action, account reference, expiry, and a random identifier, with validation performed by the backend that owns the account state. A token issued for address verification must not authorize a password change.

Delivery is insufficient evidence of authorization.

The evidence record should distinguish token issuance, a send attempt, an observed delivery status, and successful token consumption. Those are separate facts. In a ledger, a submitted instruction and a posted entry have different meanings; treating an accepted email request as a completed recovery produces the same category of reconciliation error. Record the template revision and purpose alongside opaque account and operation references, but exclude the raw token, the complete rendered body, and the reset URL from the durable audit stream. Audit references can still be personal data when they are linkable to an account.

Expiry limits acceptance, while one-time consumption limits reuse. Use an atomic application transition to consume the token and apply the authorized change, and record that transition with the application's audit event. A repeated delivery request must preserve its logical operation identifier; a newly requested recovery should receive a new identifier and follow an explicit policy for earlier outstanding tokens. Otherwise, a retry can quietly become a second grant of authority.

## Change the retained record before changing the provider

The arithmetic above isolates a retention decision, not a vendor selection. At the assumed workload, retaining every complete message creates 3 million stored bodies over 30 days. Replacing each body with selected evidence fields changes the per-attempt storage term from 2,000 to an assumed 256 bytes. A real row may be larger: identifiers, indexes, encryption envelopes, and database representation all count. Measure the serialized record before forecasting capacity, and include copies retained by the delivery provider in the retention review.

The local evidence record needs an operation identifier, opaque account reference, purpose, template revision, issuance and expiry timestamps, observed state, and a deletion deadline. Keeping those fields separate makes it possible to reconcile a send attempt without retaining its credential.

The following standalone Go example checks suppression before a future transactional retry. Configure EMAIL_API_BASE with the trusted deployment's versioned HTTPS API base, EMAIL_RECIPIENT with the address under review, and the API-key environment variable shown below. The example prints the service response for inspection; it deliberately does not log the recipient separately or automatically send a message. Treat response contents as sensitive and interpret the returned suppression result before authorizing another attempt. The local variable names are application choices, not additional provider fields.

```go
package main

import (
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

func main() {
    base := strings.TrimRight(os.Getenv("EMAIL_API_BASE"), "/")
    key := os.Getenv("INFRAI_API_KEY")
    recipient := os.Getenv("EMAIL_RECIPIENT")
    u, err := url.Parse(base)
    if err != nil || u.Scheme != "https" || u.Host == "" ||
        u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
        u.Path != "/v1" || key == "" || recipient == "" {
        log.Fatal("configure the trusted HTTPS API base, key, and recipient")
    }
    endpoint := base + "/email/suppression/check/" + url.PathEscape(recipient)
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil {
            log.Fatal("cannot construct suppression request")
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            log.Fatal("suppression transport failed; no send authorized")
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            log.Fatal("cannot read suppression response")
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retry := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
                if d := time.Duration(seconds) * time.Second; d > delay {
                    delay = d
                }
            } else if until, err := http.ParseTime(retry); err == nil {
                if d := time.Until(until); d > delay {
                    delay = d
                }
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            log.Fatalf("suppression HTTP %d: %s", resp.StatusCode, body)
        }
        fmt.Println(string(body))
        return
    }
}
```

A deletion timestamp expresses a retention policy; a field alone does not enforce deletion. Production storage needs an actual deletion mechanism, restricted access, and a policy for backups. The proposed 30-day evidence window must be justified against the merchant's legal obligations and investigation needs, rather than copied as a universal compliance period. A provider's API shape cannot establish that justification.

There is a sharp trade-off here. Once the rendered message is discarded, an investigator cannot reconstruct the exact personalized body from this record alone. Preserve versioned template source and a reviewable change history if template reconstruction matters, while recognizing that these still do not reproduce every recipient-specific rendering. Preview branding, link variables, and mobile rendering before rollout. This catches a different class of error from checking the signature on a token.

## Compare delivery options by the evidence they must supply

Provider selection follows from the evidence requirements: identify the legal entity handling message data, evaluate processing and retention terms, establish how delivery observations enter the audit trail, and verify the required deployment geography. None of the options below turns an email delivery receipt into proof that the account owner authorized a reset.

| Option | Relevant distinction | Boundary for this recovery flow |
| --- | --- | --- |
| Amazon SES | Its sending and event-publishing documentation fits an evaluation centered on AWS delivery infrastructure. | Evaluate the destination and retention of published events; application token validation and account transitions remain separate responsibilities. |
| Twilio SendGrid | Its Mail Send and Event Webhook documentation provides a concrete basis for examining transactional requests and pushed delivery observations. | A webhook requires receiver validation, duplicate handling, and reconciliation before its events become useful evidence. |
| Postmark | Its API and webhook documentation supports evaluating a dedicated transactional-mail integration. | Evaluate the message-data lifecycle and event semantics against the same retention policy rather than assuming a specialized service establishes compliance. |

Infrai provides a unified REST API with no SDK required, one API key, and unified billing across 295 routes and 20 modules. That breadth behind a simple interface includes storage, scheduling, and messaging: a single key and a single bill cover the backend capabilities, so adding recovery-message reconciliation need not introduce another credential or another invoice. Plain HTTP lets an existing Go HTTP client reuse the request conventions when the workflow adds storage or scheduling. Public, self-describing discovery supplies request and response schemas without requiring an API key, so reviewers can inspect the integration contract before granting production access. Its specified per-call request, vendor, latency, and cost metadata can also support reconciliation of recovery-message attempts; the application must still generate and validate tokens, and communications-event collection uses polling.

That last distinction matters for a five-minute reset. Decide whether the polling cadence provides sufficient visibility for support and reconciliation, and keep authorization validity independent of how quickly a delivery observation arrives. For teams prioritizing pushed delivery events, the competitors' event documentation deserves particular attention. For a team consolidating several backend capabilities, consistent metadata may justify a different choice. Neither preference is a compliance finding.

## Keep retries separate from renewed authority

Use immediate transactional delivery for account activation and short-lived reset messages. Generate the signed token in the backend, inject the link into the chosen welcome or reset template, and validate it at redemption. Code-based email fallback requires application-managed code issuance and verification; it should not be assumed to exist merely because transactional delivery exists. The email integration should be evaluated as a REST integration, and requirements for SMTP relay, voice, WhatsApp, or RCS need separate channel assessment.

A retry after an uncertain response must retain the same operation identity and evidence lineage. Where a provider supports request deduplication, verify its scope and retention window for the actual sending capability; platform-level conventions are not proof of indefinite deduplication. Keep the application record long enough to reconcile an ambiguous attempt, and do not interpret an expired provider deduplication window as permission to create a new recovery operation. Exactly-once account mutation is an application property. Email delivery needs its own, more modest claim.

Suppression is another boundary. After an unsubscribe or hard bounce, check suppression before retrying future transactional sends, record the decision without copying the address into general-purpose logs, and offer a separately designed recovery path when necessary. Repeatedly creating new attempts against the same suppressed recipient increases audit noise without demonstrating successful contact. If SMS becomes part of recovery, put geographic restrictions and country-specific spending circuit breakers in the application policy. Assess regional processing evidence separately; a broad vendor catalog cannot establish domestic compliance.

The deliberately discarded artifacts are usable reset links, token values, and personalized message bodies. What remains is an audit trail of issuance, attempts, observations, and consumption, governed by an explicit deletion policy. The cost appears during investigation: support can explain the recorded state transitions, but cannot recover the exact lost message or reproduce every rendering defect from that trail. Accept that limitation explicitly before approving retention.

## Further reading

References for token security, sender requirements, and the competing delivery interfaces:

- OWASP, Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- Yahoo, Sender Best Practices: https://senders.yahooinc.com/best-practices/
- Amazon SES, Monitoring Using Event Publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-using-event-publishing.html
- Twilio SendGrid, Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Twilio SendGrid, Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Postmark, API Overview: https://postmarkapp.com/developer/api/overview
- Postmark, Webhooks Overview: https://postmarkapp.com/developer/webhooks/webhooks-overview
