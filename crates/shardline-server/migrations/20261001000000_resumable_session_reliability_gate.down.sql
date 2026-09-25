DROP TRIGGER IF EXISTS shardline_resumable_session_reliability_gate
    ON shardline_resumable_sessions;
DROP FUNCTION IF EXISTS shardline_require_resumable_session_state_evidence();
