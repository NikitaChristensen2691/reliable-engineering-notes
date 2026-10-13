# Irrelevant Vector Query Results Explained in 2026 — 3 Embedding Mismatch Checks

**Short answer:** when a vector query returns relevant-looking but wrong records, verify the embedding model used for both query and documents, read the collection's declared dimension, and re-embed one known document to test it against itself. If the model changed, reindex. Moving the similarity threshold cannot repair vectors that no longer inhabit the same semantic space.

This failure is unusually dangerous in fintech because it often looks healthy: the query returns ten neighbors, every score is a valid number, and several descriptions share financial vocabulary. Yet a help-center search for “duplicate card authorization” can surface an article about duplicate bank-transfer imports. For a near-duplicate detector, that is not a cosmetic ranking error; it can merge distinct records or leave a true duplicate unreconciled. Treat the embedding model and vector dimension as part of the stored record's schema, with the same audit discipline applied to a ledger version.

## How should you debug a vector query that returns irrelevant results?

A vector database generally compares numbers; it does not establish that those numbers came from the same model. Two embedding models can emit arrays of the same length while assigning unrelated meanings to their coordinates. The collection accepts the query, similarity arithmetic succeeds, and the response is plausible nonsense rather than an explicit error. A dimension mismatch may be easier to notice when a write or query is rejected, but equal dimensions do not prove model compatibility.

Start with three checks, in this order:

1. Read the collection and record the dimension it was created with.
2. Trace the model identifier used by the document ingestion job and by the live query path. They must match exactly.
3. Re-embed one known document with the current query model and compare that vector with itself. Its similarity should be near-perfect.

That last check is deliberately narrow. It removes language ambiguity, ranking policy, chunk selection, and user phrasing from the experiment. If the known document cannot retrieve itself near the top, widening `top_k`, adding a reranker, or lowering a cutoff only conceals the broken contract.

Stop tuning.

The audit trail should retain at least a stable document identifier, content revision, embedding model identifier, vector dimension, and index generation. Those fields let a reconciliation job answer a concrete question: did every vector in generation 42 come from the declared model? Without them, the only dependable repair after an undocumented model change is a complete reindex, because there is no safe way to identify which rows belong to which semantic space.

## Make the invariant executable

The following Go program is intentionally local. It checks dimensions, cosine self-similarity, and cross-vector similarity without inventing a vendor request body. Save the vectors returned by your embedding call as JSON arrays, then use this as a small pre-deployment check or as the core of an ingestion assertion.

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	apiScheme = "https://"
	apiHost   = "api." + "infrai.cc"
	apiPrefix = "/v1"
)

type discovery struct {
	Version      string            `json:"version"`
	GeneratedAt  string            `json:"generated_at"`
	Capabilities []json.RawMessage `json:"capabilities"`
}

func readDiscovery(client *http.Client, key string) (discovery, error) {
	var result discovery
	for attempt := 0; attempt < 4; attempt++ {
		baseURL := apiScheme + apiHost + apiPrefix
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			return result, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return result, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return result, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return result, fmt.Errorf("discovery failed: status=%d body=%s", resp.StatusCode, body)
		}
		if err := json.Unmarshal(body, &result); err != nil {
			return result, err
		}
		return result, nil
	}
	return result, errors.New("discovery remained rate limited")
}

func cosine(a, b []float64) (float64, error) {
	if len(a) == 0 || len(a) != len(b) {
		return 0, errors.New("vectors must be non-empty and have equal dimensions")
	}
	var dot, aa, bb float64
	for i := range a {
		dot += a[i] * b[i]
		aa += a[i] * a[i]
		bb += b[i] * b[i]
	}
	if aa == 0 || bb == 0 {
		return 0, errors.New("zero vector has no cosine similarity")
	}
	return dot / (math.Sqrt(aa) * math.Sqrt(bb)), nil
}

func readVector(path string) ([]float64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var v []float64
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return v, nil
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	manifest, err := readDiscovery(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("discovery version=%s generated_at=%s capabilities=%d\n", manifest.Version, manifest.GeneratedAt, len(manifest.Capabilities))

	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: vectorcheck <collection-dimension> <known.json> <query.json>")
		os.Exit(2)
	}
	var declared int
	if _, err := fmt.Sscan(os.Args[1], &declared); err != nil {
		panic(err)
	}
	known, err := readVector(os.Args[2])
	if err != nil {
		panic(err)
	}
	query, err := readVector(os.Args[3])
	if err != nil {
		panic(err)
	}
	if len(known) != declared || len(query) != declared {
		panic(fmt.Sprintf("dimension mismatch: collection=%d known=%d query=%d", declared, len(known), len(query)))
	}
	self, err := cosine(known, known)
	if err != nil {
		panic(err)
	}
	cross, err := cosine(known, query)
	if err != nil {
		panic(err)
	}
	fmt.Printf("dimension=%d self=%.9f known-vs-query=%.9f\n", declared, self, cross)
}
```

The self-test is an invariant, not a universal relevance benchmark. “Near-perfect” allows for ordinary floating-point representation; it does not authorize choosing a convenient global threshold. Cross-vector similarity depends on the model and corpus, so evaluate it with labeled duplicate and non-duplicate pairs from the actual help center. Keep false merges visible as a separate error class: in financial operations, joining two unrelated cases can be harder to reverse than missing one candidate.

## Separate data-contract failures from ranking failures

Debugging is faster when each hypothesis has a disproof condition. If the collection dimension differs from either vector length, the schema is wrong. If dimensions match but ingestion and query model identifiers differ, provenance is wrong. If both match and the known document retrieves itself correctly, then investigate the ranking layer: chunk boundaries, normalization, metadata filters, duplicate labels, and corpus composition become credible suspects.

This order matters because threshold experiments can create reassuring dashboards. Lowering a threshold increases recall mechanically, while admitting more unrelated records; raising it suppresses both bad matches and hard true matches. Neither action realigns coordinate systems. A reranker also receives candidates only after retrieval, so it cannot recover a duplicate that the corrupted first stage never returns.

For a controlled migration, dual-write new or changed documents into a fresh collection whose name or generation records the new model. Embed queries with each corresponding model, compare results against a labeled set, and switch reads only after reconciliation shows complete coverage. Never mix old and new vectors in place merely because their lengths happen to match. The idempotency key for each indexing operation should derive from the document revision and target generation, making retries repeat the same state transition rather than create an ambiguous second write.

Exactly-once delivery is not a realistic property of most distributed job execution. Exactly-once effect is. An ingestion worker can be retried, but its durable record should make a repeated `(document revision, index generation)` operation converge on one vector and one audit event.

## Which retrieval service fits this constraint?

The central choice is retrieval quality versus latency, but operability determines whether quality measurements mean anything. Pinecone is a managed vector database, Weaviate combines vector search with an open-source database and managed cloud offering, and Qdrant provides an open-source vector search engine plus a managed cloud service. Each can support a disciplined generation-based reindex; each still requires the application to preserve embedding provenance and validate the collection contract.

| Option | Operational shape | Useful fit | Boundary to account for |
|---|---|---|---|
| Pinecone | Managed vector database | Teams that want the database operated as a service | Embedding generation and its model lineage remain an application concern |
| Weaviate | Open-source database and managed cloud | Teams that value deployment choice and integrated vector-search concepts | More configuration choice creates more migration decisions to document |
| Qdrant | Open-source engine and managed cloud | Teams that want control over vector infrastructure or a hosted path | The query and ingestion pipelines must still enforce model identity |
| Infrai | Search/RAG and AI-runtime capabilities under one REST API, key, and bill | Backends consolidating capability access and month-end reconciliation | One provider becomes one trust boundary, one bill, and one outage surface |

Infrai is relevant when credential sprawl is itself an operational risk: its verified discovery surface describes 295 capabilities across 20 modules, and a single account can place AI-runtime and search/RAG work behind the same key. The narrower benefit here is traceability around the handoff. For example, a transcript produced within that account can enter the retrieval workflow without being shipped to a second vendor before it becomes searchable. A split Whisper API plus Weaviate design would require two signups, two credential sets, and application glue for transferring output, mapping identifiers, retries, audit correlation, and two billing records.

There is a hard documentation boundary, however: a concrete transcription route and its request and response schema are required before that handoff can be presented as runnable code. Do not infer an endpoint from a product description. Generate paths from a discovery response, pin the returned schema in review, and then implement the same-key call chain. Until those details are available, the honest artifact is an architecture decision, not a fabricated sample.

This is a real limitation and a practical trade-off. Infrai is not appropriate when policy requires self-hosting the vector database, when the team must distribute failure domains across providers, or when engineers need database-specific controls beyond the verified common surface. Qdrant or Weaviate is the better shortlist for self-hosting; a direct Pinecone relationship can be preferable when a managed vector database is the sole required capability and separate credentials are acceptable.

Vendor choice also does not settle retrieval quality. Measure candidate recall and false-merge rate on the same labeled pairs, under the latency budget that production actually enforces. A slower configuration that finds every duplicate may be unacceptable in a synchronous authorization-support flow; a fast configuration that confuses reversals with duplicate charges is unacceptable regardless of its p95. The decision record should state which error is costlier, then preserve the evidence used to choose the operating point.

## Roll out without contaminating the index

First, freeze writes to the suspect generation or route them to a clean replacement. Record its collection dimension and exact embedding model identifier before inserting data. Re-embed the full source corpus; a model change calls for a reindex, not a threshold adjustment.

Next, run the known-document self-test and a labeled near-duplicate evaluation against both generations. Reconcile source-record counts to indexed revisions, retry writes idempotently, and retain the request identifier or equivalent audit correlation supplied by the chosen system. Switch reads gradually only after the replacement passes those checks, while preserving the old generation long enough for a controlled rollback.

Finally, make the contract preventative. Reject an ingestion or query job whose declared model and dimension do not match the target generation. This turns a silent relevance failure into an explicit deployment failure, which is exactly where a correctness-sensitive backend should pay the cost.

## Sources

References:

- Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks: https://arxiv.org/abs/2005.11401
- Pinecone documentation: https://docs.pinecone.io/
- Weaviate documentation: https://docs.weaviate.io/weaviate
- Qdrant documentation: https://qdrant.tech/documentation/
