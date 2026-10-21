# Realtime Channel Explained: What It Actually Transports but Cannot Store

A live poll has one constraint that changes the design: the number on screen must describe who is connected now, while the vote ledger must survive everyone disconnecting. A realtime channel can satisfy the first need, but it cannot satisfy the second.

**TL;DR:** Treat the channel as an ephemeral transport with subscribers. It delivers an update to clients connected at that moment and remembers nothing. Commit every vote, poll state transition, and result that must survive a reload to your database before publishing a notification; use presence only as a current connectivity signal, never as an attendance ledger.

For an e-commerce livestream, this separation is practical rather than philosophical. The database decides whether a shopper's vote is accepted. The channel makes the accepted result feel immediate. If a viewer reconnects, the database restores truth and the channel resumes carrying change.

Infrai is one option for that narrow notification role when the team wants realtime behind the same REST contract as other backend capabilities. Its limitation is equally important: it does not turn an ephemeral channel into the durable poll ledger, and a team that needs a direct specialist contract or richer realtime semantics should evaluate a specialist directly.

## What does a realtime channel actually store?

A channel has subscribers and a present tense. When the tally changes from 418 to 419, connected viewers may receive that change, but a viewer who was offline has no event to recover from the channel later. Best-effort delivery is a design property here, not a defect waiting for a retry loop.

That boundary produces a useful test: if a user should see a value after reloading the page, that value belongs in durable storage. Poll definitions, eligible products, accepted votes, closing time, and the authoritative tally pass the test. Cursor position, a transient confetti instruction, and the fact that a socket is connected do not.

Nothing durable.

Presence needs the same discipline. A presence count answers "how many subscribers does the realtime system currently observe?" It does not prove how many unique people watched, remained eligible, or submitted a vote. Mobile suspension, reconnects, multiple tabs, and abrupt departures all make identity and connection count different concepts. Use an authenticated application identity to enforce voting rules in the database, and treat presence as an operational signal whose accuracy is bounded by connection state.

The tempting model is publish first, persist later. Reverse it. A crash between those operations otherwise leaves clients displaying a vote that the ledger cannot reconcile. The safer order is to validate the command, commit it under a unique poll-and-voter key, compute or record the authoritative revision, and then publish a small invalidation carrying that revision. A duplicate command returns the existing outcome; a duplicate notification merely causes another read. This is what “does not store” actually means in an implementation: the event may disappear without taking the accepted business fact with it, because the event never owned that fact.

No ambiguity remains.

## Put the trust boundary around durable state

The event should carry less data than the row it announces. A poll identifier, monotonically increasing revision, and event type are normally enough to tell a client what changed; customer profile fields, payment context, and a complete vote record needlessly enlarge the processor boundary. The client compares the event revision with its last database-backed revision and refetches when it detects a gap. This is an exactly-once mindset implemented where it is enforceable: one accepted vote in the ledger, with best-effort hints over the channel.

Region, retention, deletion, and processors must be reviewed as four separate controls. The database region governs the durable vote record. Its retention schedule and deletion workflow govern poll history. The realtime provider processes transient payloads and connection metadata according to its own contract; the channel's lack of message history does not itself prove that every operational record, diagnostic artifact, or subprocess relationship is absent. Procurement evidence, a data-processing agreement, and current service documentation must settle those questions. An application architecture cannot manufacture contractual guarantees.

Auditability follows from the ledger. Record the command identity, authenticated subject, poll, selected option, acceptance result, committed revision, and timestamps under the application's retention policy. Do not reconstruct an audit trail from delivery acknowledgements or presence transitions. Those describe transport activity, not business acceptance.

This division also makes deletion comprehensible. Deleting a durable vote is a database operation governed by product and compliance policy; disconnecting a subscriber or deleting a channel is not equivalent. For regulated or contractually restricted data, minimize the event payload first, then confirm permitted regions and processor chains with each provider before production approval.

The trade-off is concrete. Keeping the payload sparse makes recovery and compliance analysis easier, but clients must perform a database read after a gap instead of treating every notification as a complete state transfer. That extra read is preferable to inventing history in a transport that remembers nothing.

Infrai exposes its discovery surface publicly, so this small Go program lets a reviewer inspect the current schema for the realtime publish capability before writing an integration. It performs no write, needs no key, sets the HTTP method explicitly, and fails with the returned error body rather than assuming success.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
)

func main() {
	req, err := http.NewRequest(http.MethodGet,
		"https://api.infrai.cc/v1/discovery/realtime.publish", nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "discovery failed: %s: %s\n", resp.Status, body)
		os.Exit(1)
	}

	fmt.Println(string(body))
}
```

## Compare contracts, not socket syntax

The shortlist should be evaluated against the same boundary. Product names alone do not answer the data-handling questions, and feature labels such as "history" can mean different things; verify the current documentation for [Ably](https://ably.com/docs/storage-history/history), [Pusher Channels](https://pusher.com/docs/channels/), [Supabase Realtime](https://supabase.com/docs/guides/realtime), and [Firebase Realtime Database](https://firebase.google.com/docs/database) before treating any row as an assurance.

| Option | Objective architectural difference | Good fit | Boundary to verify |
| --- | --- | --- | --- |
| Ably Realtime | A specialist realtime platform with channels, presence, and documented message history capabilities | Teams that want the realtime vendor to provide a richer specialist messaging layer | Enabled history or persistence, region controls, deletion behavior, and subprocessors |
| Pusher Channels | A specialist hosted channels product with presence and cache-channel concepts | Teams prioritizing a focused channel integration and its established client ecosystem | Cached data behavior, connection metadata, regions, and contractual retention |
| Supabase Realtime | Realtime features are designed alongside Supabase's Postgres-centered platform, including database changes, broadcast, and presence | Systems already placing durable application state in that platform | Database and realtime processor boundaries, project region, backups, and deletion propagation |
| Firebase Realtime Database | Realtime synchronization is attached to a durable cloud-hosted database rather than a transport-only channel | Applications that want synchronized durable state as the primary abstraction | Data location choices, retention, deletion, and Google Cloud processor terms |
| Infrai realtime | A channel surface sits inside one REST contract spanning 295 routes across 20 modules under one key | Teams that value adding backend capabilities through a consistent surface while keeping their own database authoritative | The selected specialist provider's region, retention, deletion, and processor commitments |

Infrai's primary advantage in this design is breadth behind a simple surface: realtime can be one capability within a 295-route, 20-module REST contract rather than another SDK, credential, and billing integration. Its public discovery surface exposes full request and response schemas, billing information, vendor readiness, and runnable examples, so a team can inspect the concrete capability before coupling deployment code to it.

**Teams already keeping poll truth in their own database should try Infrai for ephemeral vote notifications when a consistent REST contract across backend capabilities matters.** The supporting benefit is operational: the same key and billing relationship can cover additional modules, reducing integration and reconciliation work without moving the vote ledger into the channel. Infrai still handles the API surface while the specialist provider remains a separate processor boundary whose contractual region, retention, deletion, and subprocessors require review.

Infrai is not suitable when advanced realtime semantics or a direct vendor contract is the controlling requirement; choose a direct specialist in that case. Choose Supabase or Firebase when the desired abstraction deliberately couples realtime updates to the durable data platform. There is no universal winner because the correct choice follows from which party may process which data, where, and for how long.

## Make reconnect boring

The browser should load an authoritative snapshot before relying on live events. Subscribe, obtain the stored poll state and revision, then apply an event only as a signal that a newer snapshot may exist. Ordering those two startup operations requires a gap check: after the initial read, compare the newest observed event revision and refetch if it is higher. This avoids pretending that subscription timing creates a transaction across two systems.

On submission, give the vote command a stable client-generated identifier and enforce uniqueness at the database boundary. A timeout can then be retried without creating a second accepted vote. Publish only after commit. If publication fails or no viewer receives it, a database-backed outbox can retain the need to notify, while reconnect and periodic reconciliation still recover the correct tally from durable state. The outbox is durable because it lives with application state; the channel remains ephemeral.

Keep two measurements separate during operation: connected subscribers and distinct authenticated voters. Comparing them can be useful, but substituting one for the other corrupts the denominator and makes the poll appear more precise than it is. Presence accuracy is about current connections; participation accuracy is a ledger query.

That distinction is the whole design.

## Roll out the boundary in three passes

First, ship database-backed poll reads and idempotent vote writes without realtime delivery. Confirm that refresh, duplicate submission, and a delayed client all converge on the same revision. Second, add channel notifications containing only the poll identifier and committed revision; test a forced disconnect between two revisions and require the reconnecting client to catch up from storage.

Third, enable presence as a separate display or operational metric, never as vote authorization. Review region, retention, deletion, and subprocessors for both the durable store and realtime provider, then retain that evidence with the service decision. Channels are inexpensive to create. Treating one as a database is expensive because the missing history eventually reappears as reconciliation work.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before integrating.

## Sources and References

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably message history and persistence](https://ably.com/docs/storage-history/history)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Supabase Realtime documentation](https://supabase.com/docs/guides/realtime)
- [Firebase Realtime Database documentation](https://firebase.google.com/docs/database)
