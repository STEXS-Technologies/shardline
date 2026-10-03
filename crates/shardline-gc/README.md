# shardline-gc

Garbage collection for orphaned chunks and unreferenced CAS objects.
Implements quarantine-based GC: chunks are first moved to a quarantine state,
then deleted after a configurable retention period if no new references appear.
GC also inventories serialized-xorb containers and retained shards, resolves
xorb containers to their constituent chunk hashes, and prunes xorb cache
sidecars when their parent xorb is swept.
Supports concurrent upload/GC safety and produces detailed GC reports.
Invoked via `shardline gc`.

See the [main Shardline README](../../README.md) for the project overview.

### Clock safety and scheduled runs

On Linux, a trusted GC run persists its wall clock together with the kernel boot
identity and boot uptime. Later runs on the same host and boot compare wall-clock
progress against independent elapsed uptime. Daily scheduling jitter, missed days,
and GC process restarts therefore do not appear to be clock jumps, even without
new lifecycle activity. A forward wall-clock step beyond the existing one-day
slack still defers retention mutation and leaves the trusted anchor unchanged.
Correcting the clock allows recovery without server writes; real elapsed time
catching up to a fixed suspect clock also clears the guard. A continually advancing
clock with a persistent large offset remains deferred until corrected.

`retention_deferred_clock` in the GC report/CLI summary makes this state visible.
Pure dry runs never update the anchor. The existing anchor stays numeric for older binaries. A separate boot-observation
sidecar is trusted only when its wall timestamp matches that numeric anchor;
partial writes and old-binary updates safely fall back to the conservative guard. After a kernel reboot, a move to a
different host, or when Linux procfs is unavailable, there is no independent proof
of elapsed time: the conservative lifecycle/anchor guard remains in effect. Long
inactivity in those cases can still defer GC until a trustworthy lifecycle event
refreshes the reference. Do not delete the anchor merely to bypass a suspect clock.
