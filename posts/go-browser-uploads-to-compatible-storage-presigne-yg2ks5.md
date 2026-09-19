# Go Browser Uploads to Compatible Storage: Presigned PUT or Multipart for Large Training Artifacts

Short answer: count retained byte-days before optimizing the transfer. For a fintech training run, the bill accumulates while exported inputs and checkpoints remain stored, not merely while a browser sends them. A hypothetical 40 GiB checkpoint retained for 90 days occupies 3,600 GiB-days; retaining it for 30 days occupies 1,200 GiB-days. That change affects the dominant storage term directly, assuming the checkpoint is eligible for earlier deletion. Use a short-lived, single-object presigned PUT when a full retry is tolerable; choose multipart for a large artifact whose interrupted transfer must resume by part. Neither choice authorizes retention. Record that decision separately, and never let delivery convenience widen a browser's access to other artifacts.

## Which bytes have a reason to survive?

An inventory of training outputs should separate source exports, final checkpoints, intermediate checkpoints, evaluation results, and incomplete transfers. Those categories have different evidentiary value. Size multiplied by retained days exposes the storage term; requests, retrieval, and unfinished multipart uploads belong in separate measurements, because their weight depends on workload and contract. The 40 GiB example is arithmetic, not a measured workload or a quoted rate. Before reducing any expiry, obtain the applicable records schedule and check for holds. Regulatory obligations cannot be inferred from a file extension.

Count days, not uploads.

Start with a decision record keyed by tenant, run, artifact role, and generation. Include the policy version, intended expiry, approval or hold state, and an integrity target. An uploaded filename cannot carry this authority: two retries might use the same name, while one run can produce several legitimate checkpoints. The retained inventory must account for both objects that should exist and objects that should have disappeared.

## Should a browser upload to compatible storage use presigned POST, PUT, or multipart?

The easiest delivery path is one signed PUT for one key. Its simplicity has a boundary: a failed large transfer may require retransmitting the whole object. A signed POST can constrain submitted form fields and size through policy conditions where the selected compatible implementation supports and enforces them. Multipart permits failed parts to be resent individually but introduces a second inventory: initiated uploads that were never completed. These are transfer properties, not access-control decisions.

Keep the browser's authority scoped to the exact tenant-owned generation that the server approved, with a bounded validity period. Do not issue bucket-list or delete credentials to the browser. A failed transfer can receive a new authorization only after the backend checks the same generation and current policy; otherwise a stale tab could overwrite the evidence for a newer run. Browser success is not proof of accepted evidence. For each mode, test cross-origin behavior, signed-field enforcement, expiration, and retry behavior against the actual compatible storage endpoint before enabling it for a tenant. In a disconnected-browser test, distinguish the object that exists with no accepted row from a row whose object is missing: the former may need verification or cleanup, whereas the latter must never be offered for download. Repeat the test after a policy revision, because an earlier browser tab should not be able to revive the previous retention decision merely by finishing its transfer late.

The stale tab is the trap.

The access-control trade-off matters more than saving an application-server hop. If policy requires inspecting bytes before an artifact is eligible for consumption, a direct upload needs a quarantine and verification boundary; a server-mediated upload places inspection in the data path but consumes server bandwidth. Keep downloads separate: an upload URL grants no read access, and Content-Disposition describes response presentation rather than authorization.

## Where does an accepted upload become a record?

An object and its database row cannot be committed as one ordinary transaction. Treat completion as an idempotent reconciliation request: verify the authorized key, expected size, and an independently checked integrity value before advancing a row from pending to accepted. A multipart ETag must not be assumed to equal a whole-object digest. The Go sketch makes the durable decision explicit while leaving storage-specific verification behind an interface.

```go
package artifacts

import (
	"context"
	"errors"
	"time"
)

type Authorization struct {
	Tenant, Run, Role, Generation string
	Key, SHA256, PolicyVersion    string
	Size                          int64
	ExpiresAt                     time.Time
}

type Verifier interface {
	VerifySHA256(context.Context, string, int64, string) error
}

type Records interface {
	AcceptOnce(context.Context, Authorization) error
}

func Accept(ctx context.Context, a Authorization, v Verifier, r Records) error {
	if a.Tenant == "" || a.Run == "" || a.Role == "" ||
		a.Generation == "" || a.Key == "" || a.SHA256 == "" ||
		a.PolicyVersion == "" || a.Size < 0 || a.ExpiresAt.IsZero() {
		return errors.New("incomplete authorization")
	}
	if err := v.VerifySHA256(ctx, a.Key, a.Size, a.SHA256); err != nil {
		return err
	}
	return r.AcceptOnce(ctx, a)
}
```

The records implementation needs a uniqueness constraint across tenant, run, role, and generation, plus a conditional transition that rejects a changed policy version. `VerifySHA256` denotes a real whole-object verification operation, not a claim that object metadata or an ETag proves the digest. A retry of `AcceptOnce` must return the existing accepted decision without creating a new expiry. If verification fails, keep the artifact unavailable for consumption and expose the failure to the uploader without silently extending its authorization.

No callback is a commit.

## What does the retention boundary cost later?

Deploy retention-policy changes as versioned decisions. Before applying deletion, compare the proposed inventory with holds and approved expiries; test the race between a new hold and a deletion worker, duplicate completion, stale generations, disconnected multipart transfers, and unauthorized tenant identifiers. Reconcile pending records, completed objects, incomplete multipart sessions, and deletion evidence on a schedule. Measure byte-days by role and age alongside verification failures and orphaned parts so a growing bill can be attributed to retained evidence or transfer debris.

Then stop keeping intermediate checkpoints once their approved period ends. This reduces accumulated bytes but sacrifices the ability to replay those intermediate states during a later investigation; source material, final artifacts, integrity evidence, and deletion records need their own approved schedules. Document that loss before changing the rule. A simpler PUT may increase retransmission after failure, and multipart may improve recovery while increasing abandoned-part cleanup. Neither trade-off should decide how long financial-model evidence survives.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition

## Further reading

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html
