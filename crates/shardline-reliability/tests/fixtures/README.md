# Historical deployment evidence

`deployment-evidence-0.1.0.json` was generated from the unchanged Shardline
1.11.3 reliability crate at source commit
`98a4dadb865cca14178a27cb7480e1bb4e9eabe3` using published
StateChronicle/Penelope 0.1.0 crates.
These are frozen compatibility inputs, not values recomputed by the upgraded
library. Do not regenerate them when changing the current dependency versions.

The 30 chains cover upload intents, resumable sessions, Hub refs, OCI tags,
OCI visibility, S3 objects, provider observations, quarantine, retention holds,
webhook deliveries, and repair evidence: all eleven operation kinds. Uploads
and the eight snapshot families
include legacy JSON, canonical BCS V1, and canonical BCS V2. Every chain contains
a baseline and at least one subsequent event, with complete persisted Merkle
commitments and detached signature blocks. The separate
`mixed-encoding-upload-0.1.0.json` fixture continues legacy JSON events with
canonical V2 events in the same operation chain, protecting incremental writes
after an upgrade without rewriting the original history.

The signing seed is the public test value `[7; 32]`; the key identifier is
`deployment-compatibility-key`. Neither is a deployment credential.

The integration tests reconstruct commitments with the upgraded dependencies,
compare every stored field and signature, independently verify signatures with
the public version-2 Ed25519 implementation, exercise envelopes without schema
markers, and reject root tampering, missing commitments, and row-key sequence
rewrites. The fixture retains a hold on a future object key and absent mutable
pointers to protect those existing state representations.

For a new evidence format, add separately versioned fixtures. Keep these inputs
to preserve historical read and deterministic reconstruction coverage.

`maintenance-artifact-v1.json` freezes the separate native maintenance profile
introduced with StateChronicle/Penelope 0.2.0. It contains an immutable Custom
checkpoint state, event, and signed commit for the bytes `report`, dated
2026-10-11T00:00:00Z. Its public test seed is `[7; 32]` and its key identifier is
`maintenance-fixture-key`. This fixture must not replace the historical inputs.
