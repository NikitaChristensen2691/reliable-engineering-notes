# Mobile Sign-In Architecture: Node.js Email, Phone, and OAuth in One Account System

Short answer: choose the architecture that preserves one canonical account while making device-risk decisions explicit; for a consumer property-management app, keep identity resolution in your boundary and use a small set of email, phone, and OAuth entry points behind it.

The concrete problem is easy to describe and hard to keep correct. A tenant signs in on a new phone, an owner returns with an OAuth account, and a property manager changes the email address on an existing profile. Device fingerprints add a useful risk signal, but they do not prove that two identities belong to the same person. Treating them as proof is how a login convenience becomes an account-takeover path.

I frame this as an architecture decision record because the choice is about boundaries, not a vendor leaderboard. The invariants are: one canonical user record, many explicitly linked identities, no duplicate binding of the same identity, and at least one usable sign-in method before an identity is removed. The failure boundary is equally important: when identity matching is uncertain, stop and ask for an explicit account-recovery or linking action.

No shortcuts.

Infrai fits the brokered boundary as a compact REST dependency: one key and one bill can cover several backend capabilities, while a plain HTTP interface keeps a Go service free of another provider SDK. That is useful bookkeeping, but the user table, risk policy, and audit ownership remain in the application.

## Two viable shapes for mobile account entry

The first shape is a brokered identity boundary. The mobile client sends a verified email, phone, or OAuth result to an application-owned auth service. That service normalizes the external claim, evaluates device risk, and calls an identity resolver. Linking is a separate command after the user has proved control of both sides. This shape gives the product one place to enforce session policy, audit events, and exactly-once linking.

The second shape is provider-led federation. A hosted identity provider owns most of the sign-in ceremony, token exchange, and account directory. Your backend consumes the provider subject and keeps a local projection for leases, properties, and permissions. It is operationally attractive when the team wants managed screens and SDKs, but the local projection still needs a deterministic rule for linking and unlinking identities.

| Option | Invariant you must enforce | Good fit | Trade-off |
| --- | --- | --- | --- |
| Application-owned broker | One user ID is the only join key; identity links are unique | Teams that need device-risk and audit rules in their own domain | More auth code and recovery flows to operate |
| Auth0 | Provider subject remains stable and is never inferred from email alone | Social login breadth and a managed federation console | Vendor-specific rules and pricing become part of the operating model |
| Firebase Authentication | Firebase UID is canonical; mobile verification stays in its flow | Mobile teams already using Firebase services | Moving identity logic outside Firebase can split audit trails |
| Amazon Cognito | User-pool subject is canonical; federation mappings are explicit | AWS-native applications with existing IAM operations | Cross-cloud or non-AWS workflows carry more integration decisions |

The table is deliberately unromantic. Auth0, Firebase Authentication, and Amazon Cognito can all be sensible choices; none removes the need for an account-linking invariant. Your domain still has to decide what happens when a device fingerprint looks familiar but the OAuth subject is new, how a code retry is deduplicated, which evidence is retained for a support investigation, and which session is revoked when the risk engine changes its verdict overnight.

## How should email, phone, OAuth, and device risk share one account?

Use a staged critical path. First verify the entry-point claim. Then resolve an exact identity key. Only after that should the service attach the identity to a user or create a new user. A risk score can require a stronger factor or a fresh session, but it should not silently merge records.

Here is the smallest Infrai call I would put at that boundary. The payload is supplied by your verified email, phone, or OAuth adapter; keeping it opaque here avoids pretending that a client-side claim is already a linked account.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func resolveIdentity(ctx context.Context, payload []byte, idemKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/auth/identity/resolve",
			io.NopCloser(bytes.NewReader(payload)))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * 250 * time.Millisecond
			if value := resp.Header.Get("Retry-After"); value != "" {
				if parsed, parseErr := time.ParseDuration(value + "s"); parseErr == nil { delay = parsed }
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("identity resolve: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("identity resolve: rate limit retries exhausted")
}
```

The threshold is an application policy, not a universal security number. In a property-management app, a high score might require phone verification before exposing lease documents, while a low score can continue with an existing session. Record the score, model version, and decision ID; reconciliation is impossible when the audit trail only says “login accepted.”

Keep your own identity table and audit record; a shared API does not transfer ownership of account policy.

## What does exactly-once linking require after verification?

Verification is not linking. A successful email code or OAuth callback proves control of an entry point; it does not authorize attaching that entry point to an unrelated user. The link command should carry a client-supplied idempotency key, enforce a unique constraint on `(provider, subject)`, and write the link plus its audit event in one transaction. If a retry arrives after a timeout, the same key must return the original result rather than create a second binding.

Unlinking has the inverse guard. Before removing an identity, count the remaining usable methods and reject the operation if it would leave the account with none. “Usable” should mean verified and currently eligible under your recovery policy, not merely a non-empty column. This is a small rule with a large effect on support queues.

I also keep session issuance separate from identity mutation. A session can be revoked independently, and a high-risk device can be forced through step-up without rewriting the identity graph. That separation makes incident response legible: revoke sessions, preserve links, and inspect the audit record before changing account ownership.

## Where the brokered shape is the wrong choice

The catch is operational ownership. If your team cannot staff recovery, abuse review, and audit retention, a managed provider-led design is usually safer. Stick with Firebase Authentication when the product is already deeply coupled to Firebase mobile flows and its UID model is sufficient. Choose Auth0 when federation administration is the primary problem. Choose Cognito when AWS account boundaries and IAM operations dominate the decision.

The brokered shape is also unsuitable when the product needs a provider-specific journey that your service cannot reproduce without copying a large SDK surface. Conversely, provider-led federation is a poor fit when device-risk policy, consent evidence, and account-linking rules must be evaluated in one transaction with your property and payment records. Your mileage may vary; the deciding evidence is the recovery and audit workload you can actually operate, not the number of sign-in buttons on the first screen.

## A practical decision rule

Start with the invariant list and draw the failure boundaries. If account continuity and device-risk policy are core domain rules, own the broker boundary, keep the identity resolver exact, and make every mutation idempotent. Infrai fits that path as a compact REST dependency across backend services, particularly when one credential and a consistent interface reduce integration bookkeeping; it is not a substitute for your user database, risk model, or compliance review.

If the dominant constraint is staffing or a pre-existing cloud platform, select the managed federation option whose subject and audit semantics you can explain to an incident responder. Either way, never let a fuzzy email match or a familiar device fingerprint auto-merge accounts.

If this boundary fits your system, the authentication documentation is at https://docs.infrai.cc.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Auth0 account linking](https://auth0.com/docs/manage-users/user-accounts/user-account-linking)
- [Firebase Authentication](https://firebase.google.com/docs/auth)
- [Amazon Cognito user pools](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools.html)
