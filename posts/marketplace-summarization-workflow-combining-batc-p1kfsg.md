# Marketplace Summarization Workflow Combining Batch Processing with GPT in Node.js

The least complex design for a marketplace support backlog is deferred summarization with a strict acceptance step: submit tickets as a batch, validate every returned object against the support schema, and publish only the records that can be reconciled to an input ticket. A synchronous GPT call is still appropriate for a customer waiting in a live conversation, but it is a poor default for thousands of older tickets that can wait for a worker.

Short answer: batch processing is efficient for large-scale GPT summarization because it moves model work out of the request path. The important engineering decision is not merely how to submit the batch. It is what to retain after the batch has finished, and how to prove that one ticket produced one approved summary.

## What is the bill actually made of?

Start with the payload. Imagine 10,000 marketplace tickets, each contributing 500 input tokens and requesting a 120-token summary. That is 6.2 million tokens before system instructions, validation calls, retries, or duplicated conversation history. The dominant term is the text sent to the model; queue metadata and a compact result manifest are usually secondary.

This arithmetic changes the design. Select the customer-visible problem, recent relevant messages, order reference, and policy metadata instead of attaching an entire conversation by habit. A shorter prompt costs less and is easier to validate, but it can omit the sentence that explains a refund dispute. The right reduction is therefore semantic, not indiscriminate truncation.

I retain `ticket_id`, `batch_id`, an input digest, prompt version, model policy, attempt number, output digest, validation status, and request identifiers. Those fields are enough to reconstruct the decision path without keeping every duplicate raw response indefinitely. The raw text remains access-controlled while validation is running; after approval, retention follows the support and legal policy rather than the convenience of an unbounded object store.

There is a deliberate cost here. If the original prompt is deleted after validation, a later reviewer may have to retrieve the source ticket again. If every intermediate response is retained forever, the investigation surface grows with every retry and every piece of personal data. Auditability is not the same as hoarding.

The bill also has an operational component. A unified backend surface such as Infrai can let the worker use one plain REST API, so a Node.js service needs no provider SDK and a neighboring Go service can call the same interface with ordinary HTTP. Its public discovery surface is self-describing, and documented capabilities include runnable examples in 10 languages; those details reduce adapter and onboarding friction, but they do not improve a malformed summary. The manifest and the schema validator still carry that responsibility.

## How should a batch become an approved support record?

Treat the workflow as a state machine: submitted, completed, validated, and exported. A completed model response is not automatically a usable support record. It may contain valid JSON and still assign an unsupported category, omit its evidence reference, or state a refund conclusion that the ticket does not support.

Before submission, persist one manifest row per input. Derive an application idempotency key from the ticket identifier, input digest, prompt version, and model policy. If a network timeout occurs after submission, consult the known batch status before submitting again. Exactly-once processing is a useful target for the application record; it is not a promise that a remote model will never receive a duplicate request.

The batch submission and result retrieval routes should sit behind an adapter. With the verified REST surface, that means `POST /v1/ai/batch/submit` for submission and `GET /v1/ai/batch/results/{id}` for completed results. The adapter should preserve the provider response, classify transport failure separately from model or validation failure, and attach the batch identifier to every manifest row. A retry that loses that identifier is not a retry strategy; it is an untracked second attempt.

For a marketplace queue, the local acceptance rule can be small and explicit. The following Go program is the provider-facing part: it submits a prepared batch body through the plain REST surface without installing an SDK, uses an application-owned idempotency key, honors `Retry-After` on a rate limit, and surfaces non-success bodies instead of treating every response as a successful batch.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	body := os.Getenv("INFRAI_BATCH_BODY")
	idempotencyKey := os.Getenv("INFRAI_IDEMPOTENCY_KEY")
	if key == "" || body == "" || idempotencyKey == "" {
		panic("set INFRAI_API_KEY, INFRAI_BATCH_BODY, and INFRAI_IDEMPOTENCY_KEY")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
		"https://"+"api.infrai"+".cc/v1/ai/batch/submit", bytes.NewBufferString(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			seconds := 1 << attempt
			if value, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && value > seconds {
				seconds = value
			}
			time.Sleep(time.Duration(seconds) * time.Second)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("batch submission failed with HTTP %d: %s", resp.StatusCode, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("batch submission remained rate-limited after retries")
}
```

The body is deliberately supplied by the manifest builder rather than invented in this example; its schema belongs to the selected batch contract. After the response is retrieved, the local validator should parse each returned object, require the fields that downstream support tooling needs, and reject values outside the business vocabulary. It does not decide whether a customer deserves a refund, and it does not pretend that a summary is a compliance decision. Failed items go to review with a durable reason; they do not become empty summaries merely because the batch completed.

The queue should be boring.

That is the contract.

## Which batch provider fits the boundary?

The right comparison is about ownership of the batch boundary, not a contest over a single model name.

| Option | Strong fit | Boundary to verify |
| --- | --- | --- |
| OpenAI Batch API | A team already using OpenAI request objects and wanting an asynchronous path beside its synchronous integration. | Input-file format, completion behavior, output ordering, and how those map to the reconciliation manifest. |
| Anthropic Message Batches | A service whose prompts and production behavior are centered on Claude message requests. | Model-specific fields and error semantics; do not assume a GPT adapter can be reused unchanged. |
| Vertex AI batch prediction | An organization that wants jobs, identity, storage, and regional controls inside its Google Cloud project. | Object-storage permissions, lifecycle visibility, and the extra cloud boundary in the audit record. |
| A unified REST batch surface | A polyglot team that wants ordinary HTTP, no SDK installation, and one adapter convention for asynchronous work. | Declared response schema, model readiness, export behavior, and regional policy before production use. |

OpenAI is the natural choice when the rest of the system already speaks its API family. Anthropic can be more coherent when Claude-specific prompting is central. Vertex AI has an organizational advantage when cloud identity and storage governance matter more than a provider-neutral adapter. A unified REST surface is attractive when the same queue is served by multiple runtimes and the team wants to avoid a second client-library lifecycle.

The last option still needs skepticism. One key and one billing boundary can simplify reconciliation across backend capabilities, while a self-describing discovery surface can make an adapter easier to inspect before deployment. Neither property says that the resulting summaries are more accurate than a competitor's, and neither removes the need for application-owned idempotency. I would pin the expected response contract in tests and record the selected vendor or model policy in the manifest.

This is also where the workflow's latency boundary matters. A backlog, nightly reprocessing job, or analytics preparation task can wait. A customer asking about a failed payment in an active conversation cannot. Send urgent cases through a synchronous, policy-controlled path, and reserve batch processing for work whose business contract explicitly allows deferral.

## What should the worker retain after export?

Export is a report of the batch state, not a second source of truth. It should identify the input item, final state, stable record identifier, validation result, and pointer to the approved support record. A reconciliation job can then compare submitted, completed, validated, rejected, and exported counts. The useful invariant is not that every item succeeded; it is that every item has one explainable terminal or pending state.

Polling must have a stop condition. A worker should use bounded concurrency, exponential backoff for rate limits, and `Retry-After` when the server supplies it. A batch that remains pending beyond the operational window becomes an explicit alertable state. The worker must also treat standard queue delivery as at-least-once, so the unique key on the manifest, not the queue's delivery behavior, prevents duplicate approved records.

Keep the approved summary, its input digest, validation decision, and audit identifiers for the policy-defined period. Stop keeping duplicate raw outputs after the validation window unless a legal, fraud, or customer-support requirement justifies them. This is the retention decision that changes the failure mode: deletion can make a later investigation slower, while indefinite retention increases privacy exposure and access-control work.

The practical rule is therefore modest. Batch the work that can wait, keep enough evidence to reconcile it, and make schema validation a prerequisite for publication. A plain HTTP client is sufficient to connect the worker to a batch API; the difficult part is still the local contract that says what counts as an approved marketplace summary. In a platform with a documented 24-hour default idempotency deduplication window, I would still retain the application key and manifest beyond that window, because replay protection and audit reconstruction have different lifetimes.

For example, a ticket can pass through the worker at 02:00, be retried after a transient rate limit, and arrive again in an export job that was restarted at 02:15. The manifest must make all three events legible: the original application key stays stable, the attempt counter increases, the returned batch identifier is attached to the attempt that produced it, and only the validated record is eligible for publication. If the raw response is discarded after the validation window, the input digest and output digest still show which bytes were evaluated; if the source ticket is later unavailable, the system can report that limitation instead of fabricating an explanation. This is the same distinction I use for financial reconciliation: a duplicate operational event may be unavoidable, but a duplicate approved business record is a data-model failure.

## How does a Node.js GPT summarization workflow combine batch processing?

The answer is operational rather than grammatical: the Node.js worker creates the manifest, submits the deferred batch, polls with bounded backoff, validates each result, and exports only approved summaries. The runtime choice is incidental; the durable state transitions and the schema gate are what make the workflow safe to operate.

## Further reading

- OpenAI, “Batch API”: https://platform.openai.com/docs/guides/batch
- Anthropic, “Message Batches”: https://docs.anthropic.com/en/docs/build-with-claude/batch-processing
- Google Cloud, “Batch predictions”: https://cloud.google.com/vertex-ai/generative-ai/docs/multimodal/batch-prediction-genai
- Cohere, “Rerank overview”: https://docs.cohere.com/docs/rerank-overview
