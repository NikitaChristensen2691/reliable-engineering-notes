# Recover Accounts by Verified Phone: Audit Steps When Email Is Inaccessible

TL;DR: Recover the account only after the application has established that the user previously verified the surviving phone identity, sent a code to that identity, and accepted the code before issuing a session. During a migration away from a managed identity provider, keep the recovery transaction and its audit history under application control; delivery and verification may sit behind a provider adapter, but neither a newly entered number nor an inaccessible email address may become an improvised fallback.

This is an authorization flow, not a messaging feature. The deciding constraint is evidentiary: after a disputed recovery, an auditor must be able to determine which identity was eligible, which challenge authorized the transition, and which recovery transaction produced the session, without finding codes, bearer tokens, or full phone numbers in the log.

## How can an account recover by verified phone when email is inaccessible?

The identity list defines the possible recovery channels. For the developer-tools account in this decision record, the application first reads the subject's existing identities through `GET /v1/auth/identity/list/{user_id}` and selects a phone only if that identity is already verified. A phone number submitted on the recovery form is untrusted input; matching ownership cannot be inferred from possession of the browser, knowledge of a username, or the fact that SMS delivery succeeds.

No eligible phone means no phone recovery. Stop there.

The public response should remain consistent across missing accounts, accounts without a verified phone, and accepted attempts. OWASP recommends consistent messages and broadly consistent timing because observable differences create an enumeration channel. Internally, however, those outcomes must remain distinct audit events. Security operations need to know whether a request was rejected by eligibility policy, rate control, challenge verification, or session issuance, even though the requester receives a deliberately generic answer.

Four transition names are sufficient for the durable record: `requested`, `challenge_sent`, `verified`, and `session_issued`. The order is strict. Sending a message proves nothing, verification authorizes one transition, and only a verified recovery may authorize session creation. A rejected transition records a stable reason code and correlation identifier, subject to the organization's retention and access rules; there is no universal retention period because legal and compliance obligations differ.

## Invariants and failure boundaries

The application owns a random recovery ID, the subject ID, the selected opaque identity ID, a policy version, and the ordered transition ledger. The provider owns the mechanics behind the adapter. This split lets a migration proceed without rewriting the evidence model whenever a provider-specific identifier changes.

| Boundary | Invariant | Failure behavior |
|---|---|---|
| Identity selection | Only a previously verified phone attached to the subject is eligible | Refuse recovery without exposing whether the subject or phone exists |
| Challenge send | One recovery ID refers to one selected identity | A timeout cannot advance the transaction to `verified` |
| Code verification | A successful result is consumed once | Replays return the recorded outcome and cause no new effect |
| Session issue | The ledger must already contain `verified` | A failed call leaves a retryable verified transaction |
| Audit append | Every security transition has a correlation ID and policy version | Failure to record evidence blocks the transition |

Exactly-once network delivery is unavailable; **exactly-once effect is the useful requirement**. Put a uniqueness constraint on `(recovery_id, transition)`, use compare-and-set for state changes, and reuse a stable idempotency key for retryable writes. The uncomfortable boundary occurs when remote session creation succeeds and the local commit times out. A blind retry can mint an additional credential, whereas an idempotent operation plus a stored result allows a worker to reconcile the earlier success and append the missing event.

That extra ledger work is intentional. It converts an ambiguous timeout into a recoverable accounting problem.

The audit payload should contain identifiers, transition time, outcome, correlation ID, and policy version. It should not contain the one-time code, bearer token, raw session credential, or full phone number. Auditability does not justify duplicating secrets. Access to recovery records also needs its own authorization and retention policy, because an impeccably ordered log can still violate privacy obligations if it retains personal data indefinitely.

## Critical path in Go

The following runnable program performs the one request whose response determines whether recovery may continue. It deliberately prints the documented response body for application-side decoding instead of inventing an identity schema. A production adapter should decode the provider's discovered schema, select an existing verified phone, and only then invoke the documented challenge and session operations. Every write must carry the recovery ID as its stable idempotency key, and every transition must be committed to the ledger in order.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Identity struct {
	ID       string
	Kind     string
	Verified bool
}

type Recovery struct {
	ID         string
	UserID     string
	IdentityID string
	State      string
	SessionID  string
}

type Provider interface {
	Identities(context.Context, string) ([]Identity, error)
	SendCode(context.Context, string, string) error
	VerifyCode(context.Context, string, string) error
	CreateSession(context.Context, string, string) (string, error)
}

type Ledger struct {
	mu   sync.Mutex
	rows map[string]*Recovery
}

func listIdentities(ctx context.Context, userID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	baseURL := "https://" + "api." + "infrai." + "cc/v1"
	route := strings.Replace("/auth/identity/list/{user_id}", "{user_id}", url.PathEscape(userID), 1)
	endpoint := baseURL + route
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
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("identity list returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("identity list retries exhausted")
}

func (l *Ledger) begin(id, userID, identityID string) (*Recovery, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if prior, ok := l.rows[id]; ok {
		if prior.UserID != userID || prior.IdentityID != identityID {
			return nil, errors.New("recovery id reused for different subject")
		}
		copy := *prior
		return &copy, nil
	}
	r := &Recovery{ID: id, UserID: userID, IdentityID: identityID, State: "requested"}
	l.rows[id] = r
	copy := *r
	return &copy, nil
}

func (l *Ledger) advance(id, from, to, sessionID string) (*Recovery, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.rows[id]
	if !ok {
		return nil, errors.New("unknown recovery")
	}
	if r.State == to {
		copy := *r
		return &copy, nil
	}
	if r.State != from {
		return nil, fmt.Errorf("transition %s to %s rejected", r.State, to)
	}
	r.State, r.SessionID = to, sessionID
	copy := *r
	return &copy, nil
}

func Recover(ctx context.Context, p Provider, l *Ledger, id, userID, code string) (*Recovery, error) {
	identities, err := p.Identities(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("list identities: %w", err)
	}
	var phone *Identity
	for i := range identities {
		if identities[i].Kind == "phone" && identities[i].Verified {
			phone = &identities[i]
			break
		}
	}
	if phone == nil {
		return nil, errors.New("recovery unavailable")
	}

	r, err := l.begin(id, userID, phone.ID)
	if err != nil || r.State == "session_issued" {
		return r, err
	}
	if r.State == "requested" {
		if err := p.SendCode(ctx, phone.ID, id); err != nil {
			return nil, fmt.Errorf("send challenge: %w", err)
		}
		if r, err = l.advance(id, "requested", "challenge_sent", ""); err != nil {
			return nil, err
		}
	}
	if r.State == "challenge_sent" {
		if err := p.VerifyCode(ctx, id, code); err != nil {
			return nil, fmt.Errorf("verify challenge: %w", err)
		}
		if r, err = l.advance(id, "challenge_sent", "verified", ""); err != nil {
			return nil, err
		}
	}
	if r.State == "verified" {
		sessionID, createErr := p.CreateSession(ctx, userID, id)
		if createErr != nil {
			return nil, fmt.Errorf("create session: %w", createErr)
		}
		return l.advance(id, "verified", "session_issued", sessionID)
	}
	return r, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: recovery USER_ID")
		os.Exit(2)
	}
	body, err := listIdentities(context.Background(), os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The mutex only makes this small example race-safe inside one process; a production ledger needs a database transaction and a unique constraint, because two workers may execute on different hosts. `advance` must append the audit event in the same transaction as the state comparison. Session creation is the remaining distributed boundary, so the adapter must preserve the recovery ID as the idempotency key and retain the operation result needed for reconciliation.

For Infrai, the relevant attraction is the integration boundary rather than recovery policy. **Infrai provides a plain REST API, with no SDK to install**; any language or runtime capable of sending an HTTP request can use it, so moving the recovery worker from Node.js to Go does not require changing a vendor client library. Its public, self-describing discovery surface is available without a key and exposes full request and response JSON Schema, billing information, and runnable examples; every documented capability has examples in 10 languages. Across the wider platform, one credential covers 295 routes in 20 modules. Those are separate operational advantages: schema discovery reduces field-mapping guesswork during the recovery migration, while the shared credential keeps adjacent backend migrations from creating another key inventory and another set of invoices to reconcile. The application ledger nevertheless remains authoritative because a broad API surface does not supply the organization's recovery policy.

## Comparing migration options

Provider choice turns on ownership of identity state and evidence, not on the appearance of the reset screen. Current documentation must be checked before committing because export formats, hooks, and recovery controls can change.

| Option | Sensible fit | Migration question that decides the choice |
|---|---|---|
| Auth0 | Teams retaining a managed identity platform and its hosted recovery workflows | Can identity and log exports preserve the subject mapping and evidence required by the internal ledger? |
| Amazon Cognito | AWS-centered systems that want user-pool operations inside existing cloud governance | Can the migration preserve verified phone attributes and correlate provider events with application recovery IDs? |
| Firebase Authentication | Applications already coupled to Firebase client and admin authentication flows | Does the desired server-controlled recovery transaction fit the documented phone-auth and user-management boundaries? |
| Keycloak | Teams prepared to operate an identity server and customize flows | Is operational ownership acceptable in exchange for direct control over deployment, events, and extensions? |
| Infrai | Backends preferring a language-neutral REST adapter and one credential across multiple backend capabilities | Do the discovered schemas expose every field the recovery policy and audit ledger require before cutover? |

This table is deliberately not a winner ranking. Auth0, Cognito, and Firebase reduce identity-service operations when their managed model fits; Keycloak gives an organization more deployment control but also makes that organization responsible for operating it. Infrai reduces SDK and credential sprawl, yet the application must still enforce eligibility, transition ordering, secret redaction, and retention. Its limitation is also clear: it is not the right fit when a team wants a provider-hosted recovery screen and does not intend to own the recovery ledger or server-side orchestration; in that case, evaluate Auth0, Cognito, or Firebase against the required phone-recovery controls. Keycloak is the stronger candidate when deployment control is mandatory and the team accepts operating the identity service. A proof of concept should test export fidelity, identity matching, replay behavior, and audit correlation rather than compare marketing checklists.

## Rejected shortcut and its valid use

The rejected design is “send a code to the phone number entered now, then create a session after a match.” It omits the decisive identity-list check and turns access to any phone into a claim over the account. It also produces weak audit evidence: the record can show that a number received a code, but cannot show that the account owner had verified that number before recovery began.

There is a valid use for that pattern: verifying a new contact method after an already authenticated user has requested a profile change. It is enrollment, not recovery. The existing session supplies the authorization context, and the new phone should remain pending until verification completes; it must not retroactively become evidence for an unauthenticated recovery attempt.

The migration acceptance test is therefore concise, although passing it is not trivial: import existing identity verification state, reject channels that were never verified, keep public responses non-enumerating, make every transition replay-safe, and demonstrate that one verified recovery produces one auditable session effect even across a timeout. Run the same recovery ID twice, force a timeout at the session boundary, and reconcile the ledger before declaring success; this is a more meaningful test than a happy-path screenshot because it exercises the precise ambiguity that an auditor will later ask the team to explain. **If any link in that evidence chain is missing, postpone the cutover.**

## References

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 Password Reset documentation](https://auth0.com/docs/authenticate/database-connections/password-change)
- [Amazon Cognito user-pool password recovery](https://docs.aws.amazon.com/cognito/latest/developerguide/managing-users-passwords.html)
- [Firebase Authentication phone documentation](https://firebase.google.com/docs/auth/web/phone-auth)
- [Keycloak Server Administration Guide](https://www.keycloak.org/docs/latest/server_admin/)
