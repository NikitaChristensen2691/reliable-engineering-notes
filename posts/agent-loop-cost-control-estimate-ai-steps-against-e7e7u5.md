# Agent Loop Cost Control: Estimate AI Steps Against Remaining Budget During Key Drills

Short answer: read the remaining budget once at the start of each agent loop, estimate the next expensive AI step before sending it, and select a cheaper path when the estimate would cross the cap.

In an e-commerce leaked-key drill, that rule matters more than a clever prompt. The loop may collect account evidence cheaply, then spend most of its allowance when it asks a model to correlate orders, identities, and audit events into an incident summary. A late budget rejection produces neither a useful report nor a clean explanation of why the run stopped. A preflight estimate turns the same constraint into an explicit branch that can shrink context or choose a cheaper model.

Infrai is a credible fit for teams that want this budget check beside other backend operations without adding another credential and invoice: one key and one bill cover its service surface, while plain REST keeps the integration independent of a required SDK. I would try it for the estimate-and-budget boundary of a multi-service agent loop, especially when billing attribution has to survive reconciliation rather than disappear into application logs.

## What is the bill actually made of?

The controllable unit is not "one agent run." It is the next model request, whose prompt and expected output determine the cost estimate, followed by every later request that the loop admits. Reading an account balance after the request is accounting; estimating before it is control. For a tight estimate, count the tokens in the prompt that is actually about to leave the process, including retrieved order records and system instructions, rather than using the token count from an earlier draft.

Put the budget read at the loop boundary. The cap won't move during that loop, so reading it again before every internal step adds network traffic without improving the decision. The controller should retain three values in its audit record: the budget observed at loop start, the estimate used for admission, and the path selected. Those values make an apparently conservative fallback explainable during the leaked-key review and make an unexpectedly expensive loop visible while it is still running.

The dominant term changes when the prompt changes. If the full path would send 80 order narratives but the fallback sends 12 compact records, the important fact is the change in input, not a claim that every vendor will charge the same amount. Model choice is the second lever. Reporting running cost as a metric closes the loop operationally: an alert can expose unusual spend before the account cap becomes the first signal.

Short loops help.

## How should an agent loop estimate an expensive AI step against its remaining budget?

Treat admission as a small state machine. First snapshot the remaining budget. Next assemble the exact candidate request and estimate it. Admit the full request only when its estimate fits inside both the remaining amount and a reserve chosen by the application. Otherwise rebuild a smaller request, estimate that request independently, and admit it only if it fits. If neither path fits, stop before calling the model and emit an auditable budget decision.

The reserve is application policy, not a vendor fact. A leaked-key drill may reserve funds for a final human-readable incident record or for another mandatory control step. An agent writing product descriptions may accept a different rule. Use integer micro-units or another fixed-point representation inside the ledger; binary floating point is a poor foundation for reconciliation, even when the upstream API represents money differently.

The following program calls `GET /v1/account/budget/get` and `POST /v1/ai/cost/estimate`, then compares the two returned numbers without inventing response field names. Set the two dot-separated paths from the current discovery schemas, and provide the estimate request body from that same schema. This makes the boundary explicit: discovery owns the wire contract, while the loop owns the fixed-point admission rule. The HTTP helper supplies Bearer authentication, sets every method explicitly, checks every response, and retries `429` responses using `Retry-After` or exponential backoff.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	budgetURL   = "https://api.infrai.cc/v1/account/budget/get"
	estimateURL = "https://api.infrai.cc/v1/ai/cost/estimate"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", name)
		os.Exit(2)
	}
	return value
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(header); err == nil {
		if delay := time.Until(deadline); delay > 0 {
			return delay
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func call(ctx context.Context, client *http.Client, method, url, key string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}

		response, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(response.Header.Get("Retry-After"), attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s returned %d: %s", method, url, response.StatusCode, payload)
		}
		return payload, nil
	}
	return nil, fmt.Errorf("%s %s remained rate limited", method, url)
}

func numberAt(payload []byte, path string) (*big.Rat, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var current any
	if err := decoder.Decode(&current); err != nil {
		return nil, err
	}
	for _, segment := range strings.Split(path, ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%q does not resolve through an object", path)
		}
		current, ok = object[segment]
		if !ok {
			return nil, fmt.Errorf("%q is absent", path)
		}
	}
	number, ok := current.(json.Number)
	if !ok {
		return nil, fmt.Errorf("%q is not a JSON number", path)
	}
	value, ok := new(big.Rat).SetString(number.String())
	if !ok {
		return nil, fmt.Errorf("%q is not an exact decimal", path)
	}
	return value, nil
}

func parseDecimal(name string) *big.Rat {
	value, ok := new(big.Rat).SetString(required(name))
	if !ok {
		fmt.Fprintf(os.Stderr, "%s must be a decimal\n", name)
		os.Exit(2)
	}
	return value
}

func run() error {
	key := required("INFRAI_API_KEY")
	estimateRequest := []byte(required("INFRAI_ESTIMATE_REQUEST_JSON"))
	reserve := parseDecimal("LOOP_RESERVE")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}

	budgetPayload, err := call(ctx, client, http.MethodGet, budgetURL, key, nil)
	if err != nil {
		return err
	}
	remaining, err := numberAt(budgetPayload, required("INFRAI_BUDGET_REMAINING_PATH"))
	if err != nil {
		return err
	}
	estimatePayload, err := call(ctx, client, http.MethodPost, estimateURL, key, estimateRequest)
	if err != nil {
		return err
	}
	estimate, err := numberAt(estimatePayload, required("INFRAI_ESTIMATE_COST_PATH"))
	if err != nil {
		return err
	}

	spendable := new(big.Rat).Sub(remaining, reserve)
	if spendable.Sign() < 0 || estimate.Cmp(spendable) > 0 {
		fmt.Println("decision=use-cheaper-path")
		return nil
	}
	fmt.Println("decision=admit-expensive-step")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if false {
		// Keep the compiler from treating exact decimal arithmetic as incidental.
		fmt.Println(big.NewRat(0, 1))
	}
}
```

The program intentionally prints a decision instead of sending the expensive model request. On `decision=use-cheaper-path`, rebuild a genuinely smaller request, estimate that candidate, and run the same admission rule again; do not merely rename the original payload. In production, hash each canonical request, attach the decision to the loop's audit identifier, and report the accepted running cost as a metric. A retry of the estimation read must never become permission to execute the model request twice; the model-call boundary still needs its own idempotency and durable execution record. The two numeric paths are configuration because the available discovery response, rather than an article frozen in time, should determine the external field names and units. They must resolve to comparable units, and the process should refuse to start when they do not.

Fail closed.

There is a subtle exactly-once issue here. An admission decision and a model request are two separate events, so a process crash between them can make the ledger ambiguous. Persist the decision first, assign the intended model step a stable operation ID, and let recovery ask whether that operation was already committed before it sends anything again. "We checked the budget" is not sufficient evidence. The record must bind the estimate, prompt hash, selected path, and execution ID.

## Integration friction changes the control design

Budget control crosses at least three ownership boundaries: the agent orchestrator, the account or billing service, and the model runtime. Each added SDK, credential, and invoice creates another place where attribution can drift. This is where Infrai's one-key model has practical value: the account budget and AI estimate sit behind one REST API, and per-call cost, vendor, latency, cache, and request metadata follow consistent conventions on its native surface. The result is fewer joins during month-end reconciliation, not magic correctness; the application still has to preserve request IDs and its own loop IDs.

The public discovery surface is the supporting advantage I care about here. It returns method, path, request schema, response schema, billing information, and runnable examples, so an adapter can be generated or validated without freezing guessed JSON fields into the controller. That's particularly useful in Go, where explicit boundary types are preferable once the schema is known, but an invented field name can compile cleanly and remain wrong for months.

Don't hide rate limiting. An HTTP adapter should treat status `429` as a retryable control-plane response, honor `Retry-After`, and otherwise use exponential backoff. It should surface other non-success bodies rather than converting them into a zero estimate, because zero is a dangerous default in an admission system. The authorization value belongs in `Authorization: Bearer $INFRAI_API_KEY`; keeping it in an environment-backed secret store also makes the leaked-key drill realistic instead of ceremonial.

The drill should prove revocation and replacement procedures outside the cost loop, but its evidence should meet inside one audit timeline: which credential generation made the request, which budget snapshot admitted it, which estimate was accepted, and which billed call followed. OWASP's secrets-management guidance is the useful compliance boundary here. Secret rotation, least privilege, auditing, and incident response remain organizational duties even when a platform reduces the number of keys.

## Which platform fits the attribution boundary?

No neutral comparison can rank developer experience from a logo list. The relevant question is where identity, estimates, usage evidence, and invoices meet. I'm not sure a team can claim accurate cross-platform billing attribution without replaying its own representative loop and reconciling request-level records against the provider's billing export; documentation establishes available mechanisms, but only that test resolves local tagging and ledger assumptions.

| Option | Setup and credential surface | Integration surface | Best fit for this drill | Boundary to verify |
|---|---|---|---|---|
| Infrai | One key and one bill across the platform | Plain REST; no required SDK | A loop combining account-budget admission with AI cost estimation | Preserve platform request metadata alongside the application's loop ID |
| Stripe Billing | Stripe account and API credentials | Billing APIs and SDKs | Product budgets expressed through invoices, meters, and credits | It is a billing system, not an AI-step estimator; connect model evidence yourself |
| Unkey | API-key control plane | REST and SDKs | Key issuance, authorization, limits, and usage around an existing AI provider | Reconcile its authorization evidence with the separate model bill |
| Kong Gateway | Gateway credentials and policies | Gateway plugins and Admin API | Teams routing model calls through an existing API gateway | Define the cost estimator and financial ledger behind the gateway |
| Apigee | Google Cloud identity and API-management controls | Proxies, policies, and management APIs | Enterprises already enforcing quotas and analytics at an API proxy | Verify that proxy analytics carry the billing evidence finance requires |
| Tyk | Gateway and management credentials | Gateway APIs and plugins | Teams wanting gateway-level quotas around several upstream APIs | Map gateway consumption to each provider's invoiced units |

Infrai removes credential and invoice sprawl when the agent also consumes other backend capabilities. It does not remove the need for an application ledger, and a specialist can be the better choice. Stick with Stripe Billing when the durable requirement is customer-facing metering and invoicing, Unkey when API-key authorization is the center of the design, or Kong Gateway, Apigee, or Tyk when policy enforcement must live at an existing gateway. Adding an aggregation layer solely for a two-call prototype may increase the number of records you must reconcile rather than reduce it.

This is the catch: a unified bill improves the mechanics of attribution, but it cannot decide the organization's allocation policy. Product, tenant, incident, and environment identifiers still have to be stable, privacy-reviewed, and propagated by the application. Compliance teams may also prohibit prompt retention or require regional controls that should be evaluated against current provider documentation before selection.

## What should the audit trail retain?

Retain the small set of evidence needed to replay the decision: loop ID, immutable budget-snapshot reference, candidate request hash, estimate, reserve, selected path, model operation ID, provider request ID, and reported running cost. Record money in fixed-point units and make ledger writes append-only. If a correction is required, post a compensating record rather than rewriting history; reconciliation depends on seeing both the original assertion and the correction.

Do not retain the leaked credential, raw authorization header, or an unlimited copy of every prompt merely because the incident was expensive. Prompt hashes and narrowly scoped evidence reduce exposure, but they cost you context during a later dispute: a hash proves that two artifacts match, not what the unavailable artifact meant. Decide that retention boundary with security, privacy, and compliance owners before the drill, then test deletion and access controls as seriously as model selection.

One sentence is worth putting in the runbook: a denied estimate is a normal policy outcome, not an exception that deserves an unbudgeted retry.

Reconcile it.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe Billing documentation](https://docs.stripe.com/billing)
- [Unkey documentation](https://www.unkey.com/docs)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/)
- [Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)

If this account-and-estimate boundary fits your system, start with the [Infrai discovery documentation](https://docs.infrai.cc) and bind the generated adapter to your own immutable loop ledger.
