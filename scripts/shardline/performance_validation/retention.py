"""Observe final retention without modifying quarantine clocks or policy."""

import json
import time

from .measurements import utc_now
from .workload import verify_all


def quarantine(deployment, name):
    return json.loads(deployment.sql(name, """
        SELECT json_build_object(
          'objects', count(*), 'observed_bytes', coalesce(sum(observed_length), 0),
          'latest_delete_after_unix_seconds', coalesce(max(delete_after_unix_seconds), 0))
        FROM shardline_quarantine_candidates
    """))


def expiry_deadline(snapshots):
    return max((row["latest_delete_after_unix_seconds"] for row in snapshots), default=0)


def qualify_retention_expiry(deployment, results, objects, size):
    for row in results:
        row["quarantine_before_retention_expiry"] = quarantine(deployment, row["label"])
    observation_started = int(time.time())
    deadline = max(observation_started + 3600,
                   expiry_deadline(row["quarantine_before_retention_expiry"] for row in results))
    checkpoint = deployment.output / "retention-expiry.json"
    checkpoint.write_text(json.dumps({"status": "waiting", "started": utc_now(),
                                     "observation_started_unix_seconds": observation_started,
                                     "minimum_observation_seconds": 3600,
                                     "delete_after_unix_seconds": deadline,
                                     "lanes": results}, indent=2) + "\n")
    # All lanes age together with API writers stopped. Use the actual database
    # deadlines and at least the CLI minimum even when quarantine is empty.
    # Never backdate rows or shorten the production retention policy.
    while time.time() <= deadline:
        remaining = deadline + 1 - time.time()
        print(f"RETENTION WAIT: {remaining:.0f} seconds", flush=True)
        time.sleep(min(30, max(0, remaining)))
    for row in results:
        name = row["label"]
        lane = deployment.lane(name, row["replicas"], resume=True)
        try:
            before = deployment.inventory(name)
            start = deployment.trace.barrier()
            durations = [deployment.gc(lane / f"retention-gc-{i}.log") for i in range(2)]
            backend = deployment.trace.snapshot(start, row["final_logical_bytes"])
            after = deployment.inventory(name)
            verified = verify_all(deployment, row["dataset"], objects, size)
            row["retention_expiry"] = {
                "completed": utc_now(), "gc_seconds": durations, "backend_s3": backend,
                "before_gc": before, "after_gc": after,
                "quarantine_after_gc": quarantine(deployment, name),
                "verified_responses": verified,
                "stored_to_logical_ratio": after["stored_payload_bytes"] / row["final_logical_bytes"],
                "trace_validation": deployment.trace.validate(),
            }
            (lane / "result.json").write_text(json.dumps(row, indent=2) + "\n")
        finally:
            deployment.stop_lane(lane)
    checkpoint.write_text(json.dumps({"status": "complete", "completed": utc_now(),
                                     "observation_started_unix_seconds": observation_started,
                                     "minimum_observation_seconds": 3600,
                                     "delete_after_unix_seconds": deadline,
                                     "lanes": results}, indent=2) + "\n")
