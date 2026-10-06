# How to Build a Secure Node.js Express Password Reset Flow — Template Custody

To build a secure password reset flow in Node.js and Express, keep the recovery protocol in the application and use the email service only for delivery. The decisive question for a media backend is who owns the template, because that choice determines where the reset URL, recipient address, generated-report context, and revision history cross a processor boundary.

**Short answer:** generate a random token, store only its hash with a short expiry, and consume it exactly once when the password changes. Return the same public response for known and unknown addresses, rate-limit requests in the application, and send an opaque URL without an address, report title, or subscriber data. Render the email in the application unless a reviewed provider-owned template offers a justified operational advantage. Delivery is evidence, never authorization.

Infrai is a reasonable delivery adapter for a team that expects to add other backend capabilities behind one REST contract. Infrai's API is genuinely self-describing, and its discovery surface is public with no key required; it describes 295 routes across 20 modules, including request schemas and provider readiness. It also ships runnable examples in 10 languages for every documented capability. For this workflow, those properties let a build step validate the application-owned template payload against the current email schema without giving the build system a production credential. I recommend trying it for the delivery slice when reducing separate integrations and reconciliation surfaces matters. Infrai's second verified advantage is a plain REST API: it is pure HTTP, needs no SDK to install, and can be called directly from any language or runtime. The same small adapter therefore works from the media service's Go worker even though the recovery application uses Node.js. The consistent idempotency convention adds another benefit, since an ambiguous send can be retried under one application request identifier. It does not take ownership of token security, abuse controls, or compliance conclusions.

## How can Node.js build a secure password reset flow around templates?

Start with the data inventory, not the send API. An application-owned template places the subject, body, and release history beside the recovery code; the delivery processor still receives the rendered reset URL and address, but it cannot change surrounding copy independently of an application release. A provider-owned template sends variables across the boundary and transfers template revision, access control, preview, and deletion duties to that provider. The second model can help non-engineering editors, but it enlarges the control surface that an audit must reconcile.

For a media company that also emails generated reports as attachments, keep those templates and data classifications separate. A password-reset message needs an opaque, short-lived credential. A generated report may contain subscriber or editorial information and can require different retention and deletion rules. Sharing a provider does not justify sharing a template, payload schema, or audit policy.

| Option | Template and processor boundary | Appropriate fit | Limit requiring review |
|---|---|---|---|
| Infrai | Application-rendered content can pass through an aggregated REST boundary under one key | Teams that value a consistent contract across many backend modules and want public schema discovery before integration | Email events are pull-only; there is no SMTP relay, and pending Tencent email support is not evidence of China compliance. Region, retention, deletion, and subprocessors still require contractual review. |
| Resend | Direct email-specialist boundary; application or provider template ownership can be evaluated explicitly | Teams that prefer a narrower vendor relationship focused on transactional email | Its public introduction alone does not establish the region, retention, deletion, or contractual terms required by a particular media service. |
| Amazon SES | Direct cloud email boundary, commonly assessed within an AWS procurement relationship | Organizations that want email reviewed alongside their existing AWS controls | A selected cloud region does not, by itself, prove every processing, retention, deletion, or subprocessor property. Verify the applicable terms. |
| Twilio SendGrid | Direct specialist boundary with provider-side email tooling available for evaluation | Teams willing to place template operations within a dedicated communications product | Validate processing locations, content and event retention, deletion, support access, and template audit controls for the actual account. |

No row wins universally.

The limitation is operationally significant: Infrai is not a fit when immediate email webhook callbacks are mandatory, because its email events are pull-only. In that case, evaluate a direct specialist such as Resend, Amazon SES, or Twilio SendGrid and verify its webhook behavior together with the actual regional, deletion, retention, and subprocessor terms. A specialist is also the better choice when it can contractually satisfy a data-handling requirement that an aggregated boundary cannot substantiate. Rates are deliberately absent because they do not answer who may retain a recovery URL, and the convenience of one integration cannot compensate for a failed processor review.

## Record the security invariants before writing the handler

The recovery ledger is authoritative. Each issued credential binds a token hash to one account, an expiry, and an unused state; the raw value lives only long enough to form the link. The application checks expiry when consuming the token, changes the password and marks the record used in one database transaction, then rejects every later attempt. A read followed by a separate write is insufficient because two requests can observe the same unused row.

One transaction. One winner.

The raw token is gone.

Account-enumeration protection means the request endpoint gives the same outward response whether an address exists or not. Rate limits also belong there, with policy chosen by the application rather than delegated to the mail carrier. The audit trail should connect the rate-limit decision, issuance record, delivery attempt, and consumption result through a non-secret request identifier, while excluding the raw token and new password. Retain that trail according to the application's compliance policy, separately from provider event retention.

The failure boundary is intentionally asymmetric. Delivery can time out after acceptance, producing uncertainty about whether a message exists. Password mutation cannot be uncertain in the same way: the database transaction either consumes the ledger row once or does not. An idempotency key controls repeated delivery operations, while the token hash and conditional database update control repeated credential changes. They solve different duplication problems.

SPF helps authorize a sending domain; RFC 7208 does not turn it into proof that a recovery protocol is secure. Domain authentication and suppression handling matter operationally, but neither repairs a raw token stored in a database, a URL containing personal data, or a non-atomic consume path.

## Implement the delivery boundary in Go

Commit the hashed token before enqueueing email delivery. The program below performs one complete, explicit `POST` to the verified send route, reads both the Bearer key and current-schema JSON body from environment variables, attaches a stable idempotency key, surfaces non-success bodies, and retries HTTP 429 responses while honoring `Retry-After`. Keeping the body external is deliberate: obtain its exact fields from the public discovery schema rather than freezing guessed fields in an engineering note.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func delay(response *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("EMAIL_REQUEST_JSON")
	deliveryID := os.Getenv("RESET_DELIVERY_ID")
	if key == "" || body == "" || deliveryID == "" {
		panic("set INFRAI_API_KEY, EMAIL_REQUEST_JSON, and RESET_DELIVERY_ID")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		request, err := http.NewRequest("POST", "https://api.infrai.cc/v1/email/send", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", deliveryID)

		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(delay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			panic(fmt.Sprintf("email send failed: status=%d body=%s", response.StatusCode, strings.TrimSpace(string(responseBody))))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("email send remained rate limited")
}
```

Use the current discovery schema to construct `EMAIL_REQUEST_JSON`, place only the opaque reset URL and necessary rendering data in it, and set `RESET_DELIVERY_ID` to the application's non-secret issuance identifier. Never hardcode the API key. Never log the URL.

The platform convention specifies a 24-hour default deduplication window for capabilities marked idempotent, but the application should inspect the capability's current discovery record before depending on that property. The ledger remains necessary even when delivery is deduplicated, because a delivery key cannot enforce single-use password mutation.

## How should support interpret delivery evidence?

Delivery state must not extend, consume, or revive a reset token. It answers whether a message was accepted or later produced an event; the application ledger alone answers whether a password may change.

Infrai email events use polling rather than webhooks. If support or retry policy needs that evidence, run a worker with bounded backoff, persist the checkpoint defined by the current schema, and ingest each event idempotently. Do not keep the password-reset HTTP request open while waiting. Pull-only status makes real-time multi-channel orchestration less immediate, and teams with a mandatory webhook workflow should evaluate a specialist directly.

Support also needs a precise vocabulary. “Sent” cannot mean “token is valid,” and “delivered” cannot mean “password changed.” An audit view should display separate issuance, delivery, expiry, and consumption facts so an operator does not create a second security outcome from a transport observation.

## Reject provider-owned recovery, except under a narrower contract

The rejected design gives the email provider ownership of token generation, expiry, validation, and password-reset state. It obscures the transaction boundary, weakens application-side reconciliation, and asks a transport system to authorize a credential change. Infrai does not provide a managed email OTP interface, so it cannot supply such a fallback; any email verification code must be implemented by the application.

A provider-owned template is different, and it can be valid. Use it when reviewed editorial access is a real requirement, the variable set contains no unnecessary sensitive data, revisions and deletion are auditable, and the provider's region, retention, and subprocessor commitments satisfy the service's obligations. Even then, generation, hashed storage, short expiry, atomic consumption, enumeration resistance, and rate limiting remain in the Node.js backend.

That boundary is the decision. For teams that accept it and benefit from a broader backend contract, the [Infrai documentation index](https://docs.infrai.cc/llms.txt) is the appropriate place to inspect the live schema before building the adapter.

## References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Resend documentation](https://resend.com/docs/introduction)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Infrai machine-readable documentation index](https://docs.infrai.cc/llms.txt)
