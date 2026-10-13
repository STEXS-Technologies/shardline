//! Preserve current upload evidence when an older connection attempts recovery.
use rusqlite::Connection;

pub(super) fn install(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_upload_writers_v2 (intent_id TEXT PRIMARY KEY NOT NULL);
         CREATE TRIGGER IF NOT EXISTS shardline_upload_recovery_guard_v2
         BEFORE UPDATE OF state ON shardline_upload_intents
         WHEN NEW.state <> OLD.state AND OLD.object_key LIKE 'record/%'
           AND NOT EXISTS (SELECT 1 FROM shardline_upload_writers_v2
                           WHERE intent_id=OLD.intent_id)
           AND (OLD.intent_id LIKE 'file-v2-%' OR (
               SELECT CASE WHEN json_valid(event_json)
                           THEN json_extract(event_json, '$.operation.repository') END
               FROM shardline_reliability_events
               WHERE operation_kind='Upload' AND operation_id=OLD.intent_id
               ORDER BY sequence DESC LIMIT 1
           ) LIKE 'scope-json-v1:%')
         BEGIN SELECT RAISE(IGNORE); END;
         CREATE TRIGGER IF NOT EXISTS shardline_native_upload_recovery_guard_v2
         BEFORE UPDATE OF state ON shardline_upload_intents
         WHEN NEW.state <> OLD.state AND OLD.intent_id LIKE 'shard-v2-%'
           AND NOT EXISTS (SELECT 1 FROM shardline_upload_writers_v2
                           WHERE intent_id=OLD.intent_id)
         BEGIN SELECT RAISE(IGNORE); END;
         CREATE TRIGGER IF NOT EXISTS shardline_xorb_upload_recovery_guard_v2
         BEFORE UPDATE OF state ON shardline_upload_intents
         WHEN NEW.state <> OLD.state AND OLD.intent_id LIKE 'xorb-v2-%'
           AND NOT EXISTS (SELECT 1 FROM shardline_upload_writers_v2
                           WHERE intent_id=OLD.intent_id)
         BEGIN SELECT RAISE(IGNORE); END;",
    )
}

pub(super) fn enter(
    transaction: &Connection,
    intent_id: &str,
    repository: &str,
) -> Result<bool, rusqlite::Error> {
    if !intent_id.starts_with("file-v2-")
        && !intent_id.starts_with("shard-v2-")
        && !intent_id.starts_with("xorb-v2-")
        && !repository.starts_with("scope-json-v1:")
    {
        return Ok(false);
    }
    // A marker exists only inside this atomic write transaction. Other SQLite
    // connections cannot observe it; crashes roll it back with the transition.
    // It grants no resource fence or storage rights.
    transaction.execute(
        "INSERT INTO shardline_upload_writers_v2(intent_id) VALUES (?1)",
        [intent_id],
    )?;
    Ok(true)
}

pub(super) fn leave(transaction: &Connection, intent_id: &str) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "DELETE FROM shardline_upload_writers_v2 WHERE intent_id=?1",
        [intent_id],
    )?;
    Ok(())
}
