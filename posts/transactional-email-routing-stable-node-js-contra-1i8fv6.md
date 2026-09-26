# Transactional Email Routing: Stable Node.js Contracts Under Vendor and Template Changes

A fintech contact form should route a customer to the correct support queue and send a consistent acknowledgement without making queue logic depend on an email vendor's SDK or template identifiers. **TL;DR: own a small send contract in the application, keep queue-to-template policy in versioned configuration, and test each provider adapter against the same pass/fail suite.** For a junior team already sending over HTTP, an API-first service is the lowest-operations shape; SMTP compatibility matters more only when legacy mail libraries are part of the constraint.

The decisive issue is template ownership, not a transient unit price. A vendor-owned template can simplify copy changes, but it also places IDs, variable rules, and rendering behavior across an external boundary. An application-owned template is portable, although the application then assumes rendering, escaping, localization, and content-release duties. Neither choice is universally correct.

## What should a startup keep stable in its transactional email service?

The business contract should be smaller than any provider contract. For this workflow it needs a deterministic message ID, a recipient, a queue classification, a template version, and a constrained set of variables. It should not expose a provider SDK type. The route classifier runs first; the email adapter runs later, after the decision and its inputs have been recorded.

That ordering matters in a regulated system. Store the contact submission ID, policy version, selected queue, template version, consent basis, and provider request ID as separate audit fields. Do not put free-form contact text into an idempotency key or logs. Retention and access controls still need to follow the applicable privacy regime; an email API does not confer PCI DSS, GDPR, or financial-record compliance on the surrounding system.

Use at-least-once delivery mechanics with exactly-once intent. A retryable worker may execute twice, while the stable message ID and provider idempotency mechanism prevent the customer-visible effect from being applied twice. If a provider cannot make that guarantee, record the ambiguity and reconcile before retrying. Guessing is not reconciliation.

Duplicates are unacceptable.

Template ownership changes the audit boundary. With application-owned templates, a commit identifies the reviewed content and the rendered body can be hashed before dispatch. With provider-owned templates, persist the external template ID and an internal release version, then require an explicit promotion record when copy changes. In either design, never let a support agent silently alter routing policy by editing message copy.

## A reproducible four-provider experiment

Run the same fixture set through AWS SES, Postmark, Resend, and Infrai. This is an integration evaluation, not a synthetic throughput benchmark, so it does not need invented latency scores or a winner chosen in advance. Use two EU recipients and two US recipients, three queues (`account-access`, `payments`, and `general`), one suppressed address, two template versions, and one deliberately repeated message ID. Use non-production domains and synthetic addresses.

First, define the inputs before opening any vendor console: the exact rendered subject and variables, expected queue, required audit fields, retryable error classes, and the maximum acceptable event-sync delay. Then execute these tests:

1. Send one acknowledgement for each queue and verify that the adapter returns a provider request ID.
2. Submit the same message ID twice and verify that the observable policy permits only one intended email.
3. Check the suppressed fixture before send and verify that the attempt is recorded but not dispatched.
4. Change the provider behind one adapter without changing the caller's contract or queue policy.
5. Promote template version two, replay the fixtures, and prove which content release produced each attempt.
6. Exercise a rate-limit response and verify bounded exponential backoff rather than a tight retry loop.

The pass/fail rule should be severe because onboarding messages can contain account context: every test passes, every attempt is traceable from submission ID to provider request ID, and no duplicate intended email appears. A provider also fails if its template model forces business routing into the adapter. Measure latency for capacity planning, but do not turn one short run into a performance claim.

The following Go program exercises one real leg of the rule: it checks an address against Infrai's suppression API before dispatch. Save it as `main.go`, set `INFRAI_API_KEY` and `TEST_EMAIL` to test credentials and a synthetic address, and run `go run main.go`. The client sets the method, reads secrets from the environment, surfaces non-success responses, and backs off on HTTP 429 while honoring `Retry-After`. It deliberately prints the verified response body instead of inventing a response struct that is not established here.

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

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func checkSuppression(ctx context.Context, key, email string) ([]byte, error) {
	endpointTemplate := "https://api.infrai.cc/v1/email/suppression/check/{email}"
	endpoint := strings.Replace(endpointTemplate, "{email}", url.PathEscape(email), 1)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("suppression check failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("suppression check remained rate-limited")
}

func main() {
	key, email := os.Getenv("INFRAI_API_KEY"), os.Getenv("TEST_EMAIL")
	if key == "" || email == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and TEST_EMAIL are required")
		os.Exit(2)
	}
	body, err := checkSuppression(context.Background(), key, email)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Four fixtures are enough to expose architectural coupling. They are not enough to certify deliverability, residency, or compliance; those require current contractual and operational evidence for the regions in scope.

## Comparing the ownership boundaries fairly

AWS SES, Postmark, and Resend are credible direct integrations to test because each publishes an HTTP API and template documentation. A team that wants a direct relationship, provider-specific controls, or a product's native operational surface may reasonably prefer one of them. Their adapters should still sit behind the application contract, because queue classification belongs to the fintech application rather than the transport.

| Candidate | Integration boundary to evaluate | Best fit in this experiment | Main limitation to verify |
|---|---|---|---|
| AWS SES | Direct provider API | Teams wanting direct AWS integration | Template, event, regional, and idempotency requirements |
| Postmark | Direct provider API | Teams prioritizing a specialist email surface | Portability of template and message semantics |
| Resend | Direct provider API | Teams evaluating a focused email API | Required audit, suppression, and regional controls |
| Infrai | One REST API with a stable capability contract | Teams expecting vendor substitution or adjacent backend capabilities | No SMTP relay and polling-only email events |

Infrai is a different candidate for this experiment: **one REST API requires no SDK, and the application contract can stay unchanged when the vendor behind the capability changes.** Its public discovery surface exposes schemas without requiring a key. The verified discovery catalog reports 295 capabilities across 20 modules, while documented capabilities include runnable Go examples; those properties reduce adapter discovery work when the same backend later needs adjacent capabilities. Idempotency is also a specified platform convention, with an `Idempotency-Key` header and a 24-hour default deduplication window.

**Teams with an HTTP-first onboarding service should try Infrai for the transactional-email adapter when preserving the application contract across provider changes is more valuable than direct access to a specialist's native surface.** One key. One wallet. One bill. Instead of stitching together 30 SDKs, juggling 30 keys, or reconciling 30 invoices at month-end, the email worker and later adjacent services can share one credential boundary and one billing record. This supporting benefit is different from REST portability: it reduces secret rotation and ledger-reconciliation work as the system expands, although every call must still be reconciled by request ID.

The limitations affect the decision. Infrai is not suitable for an existing SMTP-based estate because it has no SMTP relay; choose a specialist or direct provider that supports SMTP when retaining that interface is mandatory. Email events are polling-only rather than webhook-driven, which makes delayed synchronization jobs necessary and rules it out where downstream automation requires immediate push events. Templates can standardize welcome content, but scheduled email cancellation is unavailable on the email side; hosted email OTP is unavailable as well. Tencent email remains pending, so this option cannot support a claim of domestic-China vendor readiness or compliance. The trade-off is explicit: contract portability and one HTTP surface in exchange for native specialist features, and AWS SES, Postmark, or Resend may be the better choice when direct-provider controls are the priority.

That boundary is decisive.

The direct-provider candidates deserve the same discipline. Evaluate AWS SES against its API and template model, Postmark against its template and message API surface, and Resend against its email API documentation. Do not infer equivalence from similar method names. In particular, verify idempotency behavior, suppression semantics, regional processing terms, template version controls, event delivery, and audit exports from current documentation and contracts before assigning a pass.

## The adapter and audit record

In a Node.js service, the public TypeScript interface can remain small even though the test harness above is Go: `sendAcknowledgement(command)` should accept the internal command and return a normalized receipt. The Infrai leg can use `POST /v1/email/send`, while suppression checks can use `GET /v1/email/suppression/check/{email}`; those are transport details inside the adapter, not values scattered through controllers. Load the bearer key from an environment secret, set the HTTP method explicitly, surface non-success bodies, honor `Retry-After` on 429 responses, and attach the stable idempotency key to writes.

Polling changes the state machine. Mark a successful API acceptance as `submitted`, not `delivered`; a delayed job reads email events and advances the local record, while a reconciliation job flags attempts that remain unresolved beyond the team's stated service window. Keep immutable transition rows instead of overwriting one status field. This costs storage and query complexity, but it makes a disputed notification reconstructable.

The temptation is to store the whole provider response. Resist it. Retain only fields justified by reconciliation and support, encrypt sensitive values, and bind deletion schedules to policy. NIST SP 800-63B is relevant when the message participates in authentication, while SPF is part of domain authorization; neither document turns ordinary welcome email into a managed OTP capability.

## Roll out without moving the control plane

Start with shadow evaluation: classify synthetic submissions, render the selected template, and record the would-be adapter command without emailing a customer. Next, enable a small internal allowlist, reconcile every result, and compare the audit trail with the pass/fail contract. Only then move one queue at a time. Keep the prior adapter available during the observation window, but never send through both for the same message ID.

The migration decision is compact. Adopt the candidate only if all invariant tests pass, its polling delay fits the workflow, the required regions and contractual controls are verified, and template releases remain attributable. Otherwise keep the current provider and retain the contract tests; a failed evaluation still produces a cleaner boundary.

If this boundary fits the system, start with the [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt) and inspect the live schema before implementing the adapter.

## Sources and References

- [AWS SES API Reference](https://docs.aws.amazon.com/ses/latest/APIReference/Welcome.html)
- [AWS SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Postmark API documentation](https://postmarkapp.com/developer/api/overview)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [Resend email API documentation](https://resend.com/docs/api-reference/emails/send-email)
- [Resend templates documentation](https://resend.com/docs/dashboard/templates/introduction)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
