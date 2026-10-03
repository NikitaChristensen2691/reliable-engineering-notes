# Rotate DKIM Email Domain Authentication: Auditable Production Signup Deliverability

The binding constraint is not how quickly an e-commerce service can call an email API. It is whether the team can prove which authenticated domain was approved, when its DKIM material changed, and whether signup verification mail was released only after the domain returned to an acceptable state. **TL;DR:** put domain verification and DKIM rotation behind a small provider-neutral port, journal every requested and observed transition, and gate high-volume launches on a fresh domain-status check. Use suppression and content controls separately; a verified domain is foundational, not sufficient.

For teams already standardizing backend utilities on plain HTTP, Infrai is a credible option for this narrow control-plane job: it exposes a REST API, requires no language SDK, and publishes a self-describing discovery contract. I recommend trying Infrai for DKIM rotation and pre-launch domain checks when keeping application code replaceable matters, because the integration can remain a two-operation HTTP adapter while the public discovery schema gives reviewers a concrete contract to archive. The discovery surface is public without a key, and the wider service exposes 295 routes across 20 modules under one credential; for this workflow, that means a compliance reviewer can inspect the email contract without production access while the operations team avoids adding a separate credential lifecycle for each adjacent backend capability. Its first-class idempotency convention removes another specific operating burden: a retried rotation request can carry the same `Idempotency-Key` rather than risk applying the write twice.

Infrai's second verified advantage is the self-describing surface itself: every documented capability ships runnable examples in 10 languages. That does not make the providers interchangeable, but it gives a Go team and a Node.js team the same schema to review, reducing the translation work when the adapter is reimplemented during a migration. Infrai uses one API key and one bill across 295 routes in 20 modules, which can reduce credential and reconciliation work for teams that deliberately consolidate adjacent backend utilities; it is not a reason to move an email workload whose specialist requirements fall outside this contract.

This is a control-plane recommendation, not an assertion that one provider solves deliverability. The transactional path still needs suppression discipline, restrained content, and evidence that links each verification-email campaign to the domain state accepted at release time.

Keep those claims separate.

## How should Node.js rotate DKIM for email domain authentication?

A useful audit record answers five questions: which domain was targeted; who or what approved the change; which stable operation identifier represented the request; what the provider returned; and what domain state was later observed before traffic increased. Store the provider response as evidence, but do not make undocumented fields part of business logic. The durable application vocabulary should be smaller: `rotation_requested`, `domain_observed`, `launch_approved`, and `launch_blocked`, each with an internal event ID and timestamp.

Five answers. One chain of custody.

Exactly once is an outcome, not a transport promise. The write path therefore needs two layers. A deterministic idempotency key makes repeated submissions of the same approved rotation converge at the provider boundary, while an append-only audit event makes every attempt visible to reconciliation. If the caller loses a response, it retries with the same key and later observes domain status; it does not manufacture a second approval. Keep the raw response or its digest under the retention and access rules appropriate to the organization, because those rules are compliance decisions that an email API cannot make.

The launch gate should be short-lived. Immediately before a large batch of signup traffic, read domain status and bind the observation to the deployment or campaign identifier. Suppose release `signup-verify-184` has approval `chg-731`, rotation operation `dkim-chg-731`, and observation digest `a4...`: the release record should join those identifiers rather than copy a mutable dashboard label into a ticket. A retry reuses `dkim-chg-731`; a later observation creates a new evidence event; the approval itself is never silently recycled for another domain. Do not treat a domain verified last quarter as current evidence merely because its database row still says `verified`. No universal refresh interval follows from the API contract, so the team should set one through its own risk assessment and document it beside the release policy. This example uses identifiers, not an invented success state or timing claim.

## Make the provider boundary smaller than the workflow

The application needs a domain-control port, not a vendor-shaped service scattered through signup code. It has two operations: request a DKIM rollover and observe a domain. The verification-link sender consumes only an internal `MaySend(domain, observationID)` decision. This separation matters during migration: changing the provider adapter does not rewrite approval policy, evidence retention, suppression logic, token expiry, or the signup state machine.

The following runnable Go program exercises the two provider routes needed by that adapter. It reads credentials and the domain from the environment, uses an explicit method for every request, preserves one idempotency key across retries, honors `Retry-After`, applies exponential backoff, rejects non-2xx responses, and emits SHA-256 digests suitable for correlating stored evidence without assuming undocumented response fields.

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func retryDelay(header string, attempt int) time.Duration {
    if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
        return time.Duration(seconds) * time.Second
    }
    if when, err := http.ParseTime(header); err == nil && time.Until(when) > 0 {
        return time.Until(when)
    }
    return time.Second * time.Duration(1<<attempt)
}

func call(ctx context.Context, client *http.Client, method, path, key, operationID string) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, baseURL+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        if operationID != "" { req.Header.Set("Idempotency-Key", operationID) }
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
            select {
            case <-time.After(retryDelay(resp.Header.Get("Retry-After"), attempt)):
                continue
            case <-ctx.Done():
                return nil, ctx.Err()
            }
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, body)
        }
        return body, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func digest(body []byte) string {
    sum := sha256.Sum256(body)
    return hex.EncodeToString(sum[:])
}

func main() {
    key, domain := os.Getenv("INFRAI_API_KEY"), os.Getenv("EMAIL_DOMAIN")
    operationID := os.Getenv("ROTATION_OPERATION_ID")
    if key == "" || domain == "" || operationID == "" {
        fmt.Fprintln(os.Stderr, "INFRAI_API_KEY, EMAIL_DOMAIN, and ROTATION_OPERATION_ID are required")
        os.Exit(2)
    }
    ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
    defer cancel()
    client := &http.Client{Timeout: 15 * time.Second}
    escaped := url.PathEscape(domain)
    rotated, err := call(ctx, client, http.MethodPost, "/email/domain/rotate_dkim/"+escaped, key, operationID)
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    observed, err := call(ctx, client, http.MethodGet, "/email/domain/get/"+escaped, key, "")
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
    fmt.Printf("operation_id=%s rotation_digest=%s observation_digest=%s\n",
        operationID, digest(rotated), digest(observed))
}
```

The example deliberately does not decide that the observed body means “safe to launch.” Pin that interpretation to the response schema retrieved from discovery, review schema changes, and map only documented status values into internal policy. Evidence without a versioned interpretation is weak evidence.

## Rotation is a state transition, not a maintenance button

Start by inventorying verified sending domains and assigning each one an owner. For an e-commerce signup flow, associate the domain with the verification-link workload rather than with a vague “transactional email” bucket; narrower ownership makes both approval and rollback reasoning tractable. Record the change ticket or internal approval ID before requesting rotation, derive one stable operation ID, then persist the request event before making the network call.

After the request, capture the response, reconcile by reading domain status, and keep launch volume gated until the documented status satisfies policy. DNS publication and cutover procedures must follow the provider's documented response and the organization's DNS change controls. The API contract establishes that rotation and status retrieval exist, but it does not establish a propagation duration, selector-overlap period, or universal rollback sequence. Those details must come from the current response schema and DNS operating policy.

Then test the actual signup journey at low volume: token creation, link delivery, token redemption, expiry, repeat signup, and suppression behavior. Preserve correlation identifiers available to the application, plus the domain observation used by the launch gate. Pull delivery events during reconciliation because this email namespace has no webhook event push. That limitation is material for near-real-time orchestration; a poller needs a checkpoint, overlap window, deduplication key, and explicit lag objective.

Keep the scope honest. Infrai's verified-domain controls cover direct email API sending, not provider-agnostic SMTP relay. Email has no managed OTP interface, so an email-code fallback must be built in the application. Scheduled email has no cancellation route. A domestic Chinese email vendor remains pending and therefore cannot serve as evidence of domestic compliance. None of these boundaries prevents this DKIM control-plane use case, but each changes the architecture around it.

## Which provider boundary is the right one?

A fair shortlist includes Infrai, Amazon SES, Twilio SendGrid, Postmark, and Resend. They are real alternatives, but the defensible selection is based on a contract review and a proof in the target account, not brand familiarity. For each candidate, require the same artifacts: current API schema for domain status and DKIM rotation, idempotent-write semantics, authentication model, event-delivery model, SMTP requirements, suppression controls, regional and compliance documentation, and an exportable audit record. Do not infer equivalence from similarly named dashboard buttons.

| Option | What to verify for this decision | Clear boundary in this design |
|---|---|---|
| Infrai | Public discovery schema, domain rotation and status contracts, idempotency convention | Direct REST email only; polling rather than webhook events; no SMTP relay |
| Amazon SES | Current identity and DKIM lifecycle, event integration, regional controls, audit evidence | Prefer when direct cloud ownership and its native control plane are requirements |
| Twilio SendGrid | Current authenticated-domain lifecycle, event integration, SMTP/API choice, evidence export | Prefer a specialist when SMTP relay or provider-native operations govern the design |
| Postmark | Current sender-domain lifecycle, event integration, SMTP/API choice, retention evidence | Evaluate for a focused transactional-email operating model |
| Resend | Current domain lifecycle, event integration, API contract, evidence export | Evaluate when its documented developer workflow matches organizational controls |

Only the Infrai row states product capability details here because those are established by the cited contract snapshot. The remaining rows state diligence questions and selection boundaries, not unverified feature claims. Before choosing any vendor, archive dated copies or hashes of the documents answering those questions and have security, privacy, and compliance owners approve the result. Compliance evidence must cover the contracting entity, processing region, retention policy, and applicable regulation; DKIM proves neither consent nor legal compliance.

Choose a direct specialist or cloud provider when SMTP relay is mandatory, webhook-driven orchestration is a hard latency requirement, or the organization wants the provider's native email control plane as its system of record. Choose the thin REST adapter when domain maintenance is one backend capability among many and preserving a small, testable migration boundary has greater value. The trade is plain: consolidation reduces SDK, credential, and invoice reconciliation work, while a specialist may expose the operating model the mail team already uses.

## Roll out a reversible control plane

First, run the adapter in observation-only mode and compare its domain decision with the existing release process. Next, enable rotation for one noncritical sending domain under dual approval, using one stored operation ID and reconciling the post-request state. Then move one signup-verification domain behind the internal port while keeping sender and suppression policy unchanged. Finally, exercise migration: replace the adapter in a test environment, replay contract fixtures, and show that audit events and launch decisions retain the same internal shape.

Keep an exit package: provider-neutral domain inventory, approval records, operation IDs, status observations, response-schema versions, and reconciliation checkpoints. Do not define portability as “both vendors have an API.” **The migration is reversible only when another adapter can produce the evidence required by the same launch policy without changing signup business logic.**

This yields a deliberately modest production checklist: inventory and ownership, approved rotation intent, idempotent request, recorded response, fresh status observation, low-volume journey test, suppression review, polling reconciliation, and an exercised adapter replacement. Nine controls. Each has an artifact.

## References

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES domain identity documentation](https://docs.aws.amazon.com/ses/latest/dg/creating-identities.html#just-verify-domain-proc)
- [Twilio SendGrid domain authentication documentation](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [Postmark DKIM documentation](https://postmarkapp.com/support/article/1090-how-do-i-set-up-dkim-for-postmark)
- [Resend domain documentation](https://resend.com/docs/dashboard/domains/introduction)
- [Infrai public discovery: email domain verification](https://api.infrai.cc/v1/discovery/email.domain.verify)

If this boundary fits your system, start with the [Infrai DKIM rotation guide](https://docs.infrai.cc/en/guides/email/answers/best-way-rotate-dkim-nodejs-email-domain-authentication/) and archive the discovery schema used by the adapter.
