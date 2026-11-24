# Selective serialized-Xorb ranges

Native Xet serialized-Xorb range reads keep full-container verification by default. Enable the opt-in path with either:

```sh
SHARDLINE_SELECTIVE_SERIALIZED_XORB_RANGES=true
```

or:

```toml
[server]
selective_serialized_xorb_ranges = true
```

The setting accepts only `true` or `false`; an invalid environment value prevents startup. Omitted configuration means strict whole-container verification.

The selected-member path currently applies to closed serialized byte ranges on S3 objects with an authenticated version-1 Xorb footer and a stable provider snapshot identity. It checks the exact signed range and repository coverage before fetching selected payload members, validates the trusted Xet root, binds repository coverage to the authenticated member layout, fetches and verifies each complete intersecting packed member, then returns only the requested original serialized bytes. The suffix, footer, and payload reads are pinned to the captured ETag/version and length. The server delays the response until all members in this bounded path are verified. The selected response range is capped at 1 MiB, with 8 MiB packed and decoded work limits and at most 4096 selected members; ranges that exceed these limits and unsupported footer layouts use strict whole-object verification. Proof, source-identity, or selected-member errors fail the request.

Selective verification proves the bytes returned for this range. It does not check unrequested members during that request, so it does not provide whole-container health detection. Strict mode still detects corruption anywhere in the Xorb. Operators enabling selective mode should retain immutable object-store controls and periodic full-container scrub or `fsck` checks. This setting makes no claim about physical availability, retention, replication, or backup durability.

Logical-file range reads continue to use the existing authenticated member verifier and their existing eligibility rules. This option changes only native serialized-Xorb range selection.
