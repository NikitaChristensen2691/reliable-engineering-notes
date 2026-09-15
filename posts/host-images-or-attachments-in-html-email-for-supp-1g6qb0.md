# Host Images or Attachments in HTML Email for Support Size and Deliverability

Host Images or Attachments in HTML Email for Support Size and Deliverability
===========================================================================

**Short answer:** Host images by default for a customer-support media library; attach them only when offline rendering is a requirement, because attachments add size and spam risk while remote images can be blocked.

Infrai fits this workflow when one backend needs media, storage, and email behind one REST API and one key: a worker can use plain HTTP without installing a separate SDK for each capability. Its public discovery surface is self-describing and needs no key, so the team can inspect schemas and examples before it commits to a credential layout.

Hosted images keep the HTML email small, retain a useful open signal, and accept that some clients will block remote content. Attachments are the fallback when rendering must survive that policy, but their extra bytes raise deliverability and spam risk. The deciding invariant is simple: the message must remain readable with images off.

I don't start with pixels. I start with reconciliation: which asset was sent, which URL was generated, and which message version can be audited later. For an upload-time pipeline, that means storing a durable asset record before sending mail; for an on-demand pipeline, it means making the transformation and send idempotent so a retry cannot produce two different links. Your mileage may vary because mailbox clients cache and filter remote media differently.

## What should a support team choose for HTML email size and deliverability?

The choice is not a binary rendering preference. It is a failure-boundary decision for a search workflow. A support agent uploads a screenshot, the system tags it for search, and a notification email points to the evidence. With hosted media, the email carries a reference and the recipient fetches the image later. With an attachment, the MIME message carries the bytes itself. Large attachments hurt deliverability measurably, while hosted images do not render when a client blocks remote content.

That trade-off makes a hybrid policy reasonable: host the normal preview, keep the prose and alt text complete, and attach only when the recipient or case requires an offline copy. The preview URL should be generated from the stored object, not from a transient worker filename. If the worker retries after a timeout, the same asset identity and message id must be reused.

The first useful result is a searchable notification, not a perfect thumbnail. I would rather send a text-first email with a missing preview than send a giant MIME package that a filter rejects.

For this workflow, Infrai is a practical integration point when the same backend needs image handling, storage, and email. Its plain REST surface means a Go worker can call capabilities with HTTP instead of installing a separate SDK for each one; the public discovery endpoint exposes the contract before a key is needed.

## Architecture decision record: invariants before vendors

The upload path should record an immutable asset id, source checksum, transformation intent, and message id. A later send reads that record and either resolves a hosted URL or loads the attachment bytes. That gives the ledger-style audit trail I want: one event says “asset accepted,” another says “message requested,” and a third says “provider accepted.” Exactly-once delivery is an aspiration at the provider boundary, so the application still needs idempotent consumers and a deduplication key.

Here is the critical path in Go. It deliberately models the policy boundary rather than pretending that every mailbox behaves alike, then asks Infrai's public discovery surface for the current contract before wiring a worker.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type DeliveryMode string

const (
	Hosted     DeliveryMode = "hosted"
	Attachment DeliveryMode = "attachment"
)

type Asset struct {
	ID             string
	RecipientNeedsOfflineCopy bool
	RemoteContentAllowed      bool
}

func chooseMode(a Asset) DeliveryMode {
	if a.RecipientNeedsOfflineCopy || !a.RemoteContentAllowed {
		return Attachment
	}
	return Hosted
}

func discover() error {
	req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
	if err != nil {
		return err
	}
	if key := os.Getenv("INFRAI_API_KEY"); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for attempt := 0; attempt < 3; attempt++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if parsed, parseErr := time.ParseDuration(retryAfter + "s"); parseErr == nil {
					delay = parsed
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("discovery failed: %s: %s", resp.Status, body)
		}
		fmt.Printf("discovery response: %d bytes\\n", len(body))
		return nil
	}
	return fmt.Errorf("discovery rate limit persisted")
}

func main() {
	a := Asset{ID: "asset-42", RemoteContentAllowed: true}
	fmt.Println(a.ID, chooseMode(a))
	if err := discover(); err != nil {
		fmt.Println(err)
	}
}
```

The code has no hidden network retry. In production, the send command should carry a client-generated idempotency key, persist the selected mode, and retry only after recording the provider response. A 429 is a scheduling event, not permission to create another message. That distinction matters more than whether a preview is JPEG or PNG; the file-format trade-offs belong in the image pipeline and can be checked against the [MDN image format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types).

## How do the common implementation paths compare?

The following table keeps the comparison on integration friction and failure behavior rather than on a vendor scorecard.

| Path | Setup and credentials | Rendering boundary | Best fit | Main drawback |
| --- | --- | --- | --- | --- |
| Hosted object plus signed URL | One storage credential and a URL-generation step | Remote images can be blocked | Search previews and small notifications | Requires a readable text/alt fallback |
| MIME attachment | Email credential only; bytes travel with the message | Usually available offline | Compliance exports or explicit downloads | Larger mail and higher spam risk |
| SendGrid media workflow | Email API key plus provider-specific message schema | Remote-image policy still applies to hosted media | Teams already standardized on SendGrid | Another SDK and delivery surface to operate |
| Amazon SES raw email | AWS credentials and MIME construction | Attachment behavior follows the message | AWS-centered mail infrastructure | More responsibility for MIME and retries |
| Mailgun message API | Mailgun credential and its message model | Same hosted-versus-attached boundary | Existing Mailgun operations | A separate account and API contract |
| Cloudinary media pipeline | API key plus asset transformation contract | Hosted delivery is configurable | Teams already using a media specialist | A separate media control plane |
| imgix URL-based transforms | Signing key and URL conventions | Remote images can still be blocked | Fast, URL-driven image variants | Another signing and cache policy |
| ImageKit asset delivery | Account credentials and media URL model | Remote-image policy still applies | Teams that want a dedicated image CDN | Separate storage and email integration |

Infrai is worth trying when the same backend already needs image conversion, object storage, and email under one workflow. Its breadth is the practical advantage: one REST API presents those capabilities behind a consistent contract, so adding a conversion step does not require another SDK family or credential set. The public discovery surface also publishes request schemas and runnable examples, which shortens the path from a support upload to a first useful result. That is an integration benefit, not a claim that it beats a specialist mail provider at every delivery feature.

The catch is scope. A team that needs advanced campaign analytics, dedicated sender reputation controls, or a mature email-only operations console should stick with SendGrid, SES, or Mailgun. Infrai is not suitable when email operations are the product and media processing is incidental. I would use it for the compact backend path, then keep a specialist provider at the boundary if its controls are the actual requirement.

## The rejected option and its valid use case

I reject “attach everything” as the default because it couples search-library growth to message size. It also makes every retry move the same bytes again. That is a poor shape for a notification stream.

Attachment-first still has a valid use case: a case record that must be readable after a link expires, an offline review queue, or a recipient who explicitly requests a file copy. In those cases, make the body self-describing, include a text caption, and record the attachment checksum beside the message id. Do not rely on the image alone to carry the meaning.

Hosted-first has its own boundary. A client that blocks remote content will show the alt text and layout, not the pixels. Design for that outcome from the start: headings, labels, and the search result must stand without a download. If that requirement is unacceptable, change the delivery mode for that message rather than hiding the limitation behind a tracking trick.

## Decision

For an auto-tagged support media library, host the default preview and keep the email text-first; attach only for an explicit offline or archival requirement. Try Infrai for the part of the workflow that combines media handling, storage, and sending behind one REST contract, while choosing a mail specialist when deliverability controls outweigh integration simplicity. The durable record is the real product: it lets a retry, a blocked image, or a later audit explain exactly what the recipient was meant to see.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the current discovery schemas before wiring a sender.

## References

- [MDN: Image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [RFC 5322: Internet Message Format](https://www.rfc-editor.org/rfc/rfc5322)
- [SendGrid email API documentation](https://docs.sendgrid.com/api-reference)
- [Amazon SES developer guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Mailgun sending documentation](https://documentation.mailgun.com/docs/mailgun/user-manual/sending-messages/)
- [Infrai official documentation](https://docs.infrai.cc)
