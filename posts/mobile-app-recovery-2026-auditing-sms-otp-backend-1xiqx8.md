# Mobile App Recovery 2026: Auditing SMS OTP Backend API Notice Evidence

Require the audit record to identify exactly which claim each event supports before enabling administrator recovery. TL;DR: For a media SaaS mobile app, keep SMS OTP acceptance, privileged recovery authorization, and compliance-notice delivery as separate evidence categories; autofill and resend belong to the challenge workflow, while a delivery record must retain its own attributable evidence. A successful login response cannot stand in for that record.

This decision record addresses the evidence review for a publisher whose administrators recover access through a mobile app. The job is to send a compliance notice with an auditable delivery record. The decision axis is compliance evidence: what can a reviewer establish after the request, the phone screen, and the original operator are gone? The identifiers, thresholds, and event names below are proposed application policy, not observed performance or legal requirements.

## How can a mobile app audit SMS OTP login without overstating recovery evidence?

Start with the reviewer's question. A record that a challenge response was accepted supports a claim about verification under a specified policy. A separately authorized recovery decision supports a claim about access restoration. A notice attempt supports a claim that dispatch was attempted; the meaning of any subsequent transport status depends on the channel's documented contract. Combining those events into one success flag removes the distinctions that an investigation needs.

Keep the claims narrow.

For the proposed example, case `recovery-204` belongs to tenant `publisher-17`, with notice `notice-204` and content revision `notice-v3`. An evidence export should explain which administrator recovery produced the obligation, which approved destination reference was used, which content revision was dispatched, and which transport evidence supports the displayed outcome. Retain historical destination and content references; looking up today's profile or template during an audit could otherwise describe a different notice from the one attempted. This is an application invariant to implement, rather than a property supplied by an SMS API.

The compliance limit matters here: neither OTP acceptance nor a transport status establishes that a person read, understood, or legally received a notice. The responsible organization must approve the acceptable channel, notice content, evidence requirement, retention rule, and escalation deadline. The sender guidelines and SMS documentation in References address channel operations; they do not decide those obligations for this publisher.

## Evidence invariants and disclosure boundaries

Adopt three invariants for this design: each recovery decision has a stable case identifier, each required notice has a stable obligation identifier, and every displayed claim can be traced back to retained evidence. Repeated requests must reuse the original business decision after authorization; repeated dispatch attempts remain separately attributable. This is an exactly-once mindset for business records, with explicit uncertainty at network boundaries. It does not promise exactly-once external delivery.

A proposed challenge policy uses a code of 6 digits, a lifetime of 300 seconds, a resend cooldown of 30 seconds, and a maximum of 5 failed submissions per recovery case. These numbers make the example reviewable. They are not compliance thresholds. The backend must own expiry, challenge purpose, attempt accounting, and recovery authorization; a mobile client can request an action but cannot supply authoritative policy state. For a React Native interface, autofill and typed input should submit through the same backend verification path. A filled input proves nothing by itself.

Resend needs an evidence rule too.

If a replacement challenge resets the case-level failure counter, an attacker can renew the attempt budget by requesting another message. Bind challenges to the same recovery case and purpose, record replacement as a distinct event, and define which challenge remains eligible for verification. Apply issuance controls across account, destination, session, and network source: an IP-only restriction creates an awkward boundary for a shared newsroom, while a destination-only restriction leaves distributed issuance unaddressed. Treat these as dimensions of the proposed abuse policy, with thresholds approved separately.

Keep plaintext codes out of the audit trail. Store access-controlled decision reasons internally while returning a consistent public response that does not reveal account membership. This sacrifices convenient client diagnostics to reduce disclosure. A support view can explain a rejected resend without exposing the code, full destination, or unrestricted recovery history.

The other boundary is administrative: access to a recovery record must itself require authorization. Possession of `recovery-204` is not permission to export it. Retention should be purpose-specific, and an evidence export should state when its underlying records expire; retaining personal data indefinitely is not a substitute for an approved compliance policy.

## Compare what a reviewer receives

Choose an evidence representation before choosing a delivery adapter. The following alternatives compare application designs, rather than products, against the same review question.

| Representation | Reviewer's evidence | Important boundary | Decision for this notice |
| --- | --- | --- | --- |
| One success flag for recovery and messaging | A final application label | Cannot distinguish authorization from delivery | Reject |
| Mutable notice status with a transport identifier | Latest status and an external reference | Earlier observations and their interpretation require separate retention | Useful as a dashboard projection |
| Retained event history with a versioned interpretation | Decision, obligation, attempts, and attributable observations | Requires access policy, retention policy, and reconciliation | Select as the audit basis |

The selected design costs more operational attention: someone must own event interpretation, investigate unresolved evidence, and keep exports understandable after a template or adapter changes. The benefit is a reviewable explanation of the decision. A dashboard can still show a short status; it should link that projection to the records supporting it.

A transport timeout is the decisive example. If remote acceptance occurred but the response was lost, the application knows that it attempted dispatch and lacks a confirmed outcome. Retrying may create another notice unless the chosen transport supplies a suitable documented deduplication contract. A local idempotency key alone cannot settle the remote result. Preserve the uncertainty, reconcile against available evidence, and make the duplicate-notice trade-off explicit in the retry policy.

Unknown stays unknown.

## Project evidence without inventing delivery

The critical path below turns already-retained evidence into a review view. It deliberately has no field for human receipt or legal sufficiency. These Go types are proposed internal contracts, not transport event names. The authenticated ingestion layer must map documented channel events into these records, retain the original evidence separately, and check tenant and notice ownership before invoking the projection.

```go
package noticeaudit

import (
    "errors"
    "time"
)

type Observation struct {
    EventID     string
    NoticeID    string
    EvidenceRef string
    Kind        string
    RecordedAt  time.Time
}

type Review struct {
    NoticeID         string
    DispatchObserved bool
    DeliveryReported bool
    EvidenceRefs     []string
}

func Project(noticeID string, events []Observation) (Review, error) {
    view := Review{NoticeID: noticeID}
    if noticeID == "" {
        return Review{}, errors.New("missing notice identifier")
    }
    seen := make(map[string]Observation)
    for _, event := range events {
        if event.NoticeID != noticeID || event.EventID == "" ||
            event.EvidenceRef == "" || event.RecordedAt.IsZero() {
            return Review{}, errors.New("incomplete or mismatched evidence")
        }
        if previous, exists := seen[event.EventID]; exists {
            if previous != event {
                return Review{}, errors.New("conflicting event identity")
            }
            continue
        }
        switch event.Kind {
        case "dispatch-attempt-recorded":
            view.DispatchObserved = true
        case "transport-delivery-reported":
            view.DeliveryReported = true
        default:
            return Review{}, errors.New("unsupported evidence kind")
        }
        seen[event.EventID] = event
        view.EvidenceRefs = append(view.EvidenceRefs, event.EvidenceRef)
    }
    return view, nil
}
```

`DeliveryReported` means that the retained history contains an authenticated transport delivery report under the adapter's approved mapping. It is an observation, not a synthesized current delivery verdict. The projection does not infer dispatch from a later report, infer delivery from a successful OTP, or erase an earlier observation because another event arrived later. Consumers must treat `EvidenceRefs` as references to authorized records, not public download locations.

The example fails closed on unknown event kinds so a mapping change requires review. That is a deliberate maintainability trade-off: deploy compatible readers before enabling a new event type, and keep the interpretation version with exported evidence. In a full implementation, preserve both the reported event time and backend receipt time, use a durable uniqueness constraint for event identity, and retain unresolved or conflicting observations for investigation. The in-memory map is only projection-level deduplication.

Recovery approval and its required notice obligation still need an atomic durable boundary. Enforce uniqueness on the case and obligation, authorize replays, and reconcile approved cases against their notice records. A correct projection cannot repair a notice obligation that the application never recorded.

## Review release evidence and reject the shortcut

Before release, exercise duplicate observations, conflicting event identities, reordered callbacks, an unknown event kind, and a delivery report with no locally observed dispatch attempt. The last case should retain the report without manufacturing the missing attempt. Separately test concurrent challenge submissions, resend racing with verification, and approval retried after a lost response; the expected business outcome is one authorized recovery decision and one required notice obligation, with attributable attempts. These are proposed acceptance tests, not claims that the example implements the whole workflow.

Operational review should ask for the oldest unresolved obligation, the number of approved recoveries missing an obligation, and the age of unresolved dispatch evidence. A healthy worker process does not answer those questions. Assign ownership for reconciliation and escalation before rollout, keep pending records interpretable through rollback, and include retained evidence, support access, and review time in the operating-cost discussion. Price is secondary to the ability to explain an overdue notice.

Reject the single success flag as the audit basis. Its valid use case is a transient user-interface acknowledgement whose underlying evidence remains separately available; it is inexpensive to display and easy to understand. For this media administrator recovery workflow, the durable record must preserve the difference between challenge acceptance, authorized access restoration, and the delivery evidence actually obtained. That distinction is the decision.

## References

- Google, Email sender guidelines: https://support.google.com/a/answer/81126
- Twilio, SMS documentation: https://www.twilio.com/docs/sms
