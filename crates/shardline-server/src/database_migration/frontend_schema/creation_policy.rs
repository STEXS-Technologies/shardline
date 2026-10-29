//! Validate the owner-installed publication guard before accepting traffic.
use super::{DatabaseMigrationError, FRONTEND_MIGRATIONS, incompatible};
use sqlx::{PgConnection, query_scalar};

pub(super) async fn mirror_grants(
    connection: &mut PgConnection,
) -> Result<(), DatabaseMigrationError> {
    for (target, privileges) in [
        (
            "shardline_hub_creation_policies",
            &["SELECT", "INSERT (namespace)", "UPDATE (namespace)"][..],
        ),
        ("shardline_hub_creation_denials", &["SELECT"][..]),
        (
            "shardline_hub_creation_events",
            &["SELECT", "INSERT", "DELETE"][..],
        ),
    ] {
        super::mirror_grants_as(
            connection,
            "shardline_hub_repos",
            target,
            &["INSERT"],
            Some(privileges),
        )
        .await?;
        super::mirror_grants(connection, "shardline_hub_repos", target, &["SELECT"]).await?;
    }
    Ok(())
}

pub(super) async fn check(connection: &mut PgConnection) -> Result<(), DatabaseMigrationError> {
    let search_path: String =
        query_scalar("SELECT format('search_path=pg_catalog, %I',current_schema())")
            .fetch_one(&mut *connection)
            .await?;
    let migration = FRONTEND_MIGRATIONS
        .iter()
        .find(|m| m.name == "hub_creation_policy")
        .ok_or_else(|| incompatible("bundled Hub creation policy"))?;
    for (table, columns) in [
        (
            "shardline_hub_creation_policies",
            vec![
                ("namespace", "text", true),
                ("configured", "boolean", true),
                ("enabled", "boolean", true),
                ("max_repositories", "bigint", false),
                ("max_creations", "bigint", false),
                ("window_seconds", "bigint", true),
            ],
        ),
        (
            "shardline_hub_creation_denials",
            vec![
                ("namespace", "text", true),
                ("issuer", "text", true),
                ("subject", "text", true),
            ],
        ),
        (
            "shardline_hub_creation_events",
            vec![
                ("namespace", "text", true),
                ("created_at_unix_seconds", "bigint", true),
            ],
        ),
    ] {
        let actual: Vec<(String,String,bool)> = sqlx::query_as(
            "SELECT attname::text,format_type(atttypid,atttypmod),attnotnull FROM pg_attribute
             WHERE attrelid=to_regclass(format('%I.%I',current_schema(),$1)) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        ).bind(table).fetch_all(&mut *connection).await?;
        if actual.len() != columns.len()
            || actual
                .iter()
                .zip(columns)
                .any(|((name, kind, required), (n, k, r))| name != n || kind != k || *required != r)
        {
            return Err(incompatible(table));
        }
    }
    check_constraints_and_defaults(connection).await?;
    check_indexes(connection).await?;
    for (trigger, table, function, kind) in [
        (
            "shardline_hub_creation_guard",
            "shardline_hub_repos",
            "shardline_hub_creation_guard",
            7_i16,
        ),
        (
            "shardline_hub_creation_record",
            "shardline_hub_repos",
            "shardline_hub_creation_record",
            5,
        ),
        (
            "shardline_hub_creation_policy_lock",
            "shardline_hub_creation_policies",
            "shardline_hub_creation_policy_lock",
            31,
        ),
        (
            "shardline_hub_creation_denial_lock",
            "shardline_hub_creation_denials",
            "shardline_hub_creation_policy_lock",
            31,
        ),
    ] {
        let marker = format!("CREATE FUNCTION {function}()");
        let body = migration
            .up_sql
            .split_once(&marker)
            .and_then(|(_, tail)| tail.split_once("AS $$"))
            .and_then(|(_, tail)| tail.split_once("$$;"))
            .map(|(body, _)| body)
            .ok_or_else(|| incompatible("bundled Hub creation guard body"))?;
        let valid: bool = query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
             JOIN pg_namespace n ON n.oid=p.pronamespace
             WHERE t.tgname=$1 AND t.tgrelid=to_regclass(format('%I.%I',current_schema(),$2))
               AND t.tgenabled IN ('O','A') AND NOT t.tgisinternal AND t.tgtype=$3
               AND t.tgqual IS NULL AND t.tgnargs=0 AND cardinality(t.tgattr::smallint[])=0
               AND p.proname=$4 AND n.nspname=current_schema() AND NOT p.prosecdef
               AND p.prosrc=$5 AND p.proconfig=ARRAY[$6]::text[])",
        )
        .bind(trigger)
        .bind(table)
        .bind(kind)
        .bind(function)
        .bind(body)
        .bind(&search_path)
        .fetch_one(&mut *connection)
        .await?;
        if !valid {
            return Err(incompatible(trigger));
        }
    }
    Ok(())
}

async fn check_constraints_and_defaults(
    connection: &mut PgConnection,
) -> Result<(), DatabaseMigrationError> {
    for (table, expected) in [
        (
            "shardline_hub_creation_policies",
            &[
                "CHECK (length(namespace) > 0 AND POSITION(('/'::text) IN (namespace)) = 0)",
                "CHECK (max_creations IS NULL OR max_creations >= 0)",
                "CHECK (max_repositories IS NULL OR max_repositories >= 0)",
                "CHECK (window_seconds >= 1 AND window_seconds <= 86400)",
                "PRIMARY KEY (namespace)",
            ][..],
        ),
        (
            "shardline_hub_creation_denials",
            &[
                "FOREIGN KEY (namespace) REFERENCES shardline_hub_creation_policies(namespace) ON DELETE CASCADE",
                "PRIMARY KEY (namespace, issuer, subject)",
            ][..],
        ),
        ("shardline_hub_creation_events", &[][..]),
    ] {
        let constraints: Vec<String> = query_scalar(
            "SELECT pg_get_constraintdef(oid,true) FROM pg_constraint
             WHERE conrelid=to_regclass(format('%I.%I',current_schema(),$1))
             ORDER BY pg_get_constraintdef(oid,true)",
        )
        .bind(table)
        .fetch_all(&mut *connection)
        .await?;
        if constraints.iter().map(String::as_str).collect::<Vec<_>>() != expected {
            return Err(incompatible(table));
        }
    }
    for (table, column, kind, required, default) in [
        (
            "shardline_hub_repos",
            "creation_authorized",
            "boolean",
            true,
            Some("false"),
        ),
        (
            "shardline_hub_repos",
            "creation_issuer",
            "text",
            false,
            None,
        ),
        (
            "shardline_hub_repos",
            "creation_subject",
            "text",
            false,
            None,
        ),
        (
            "shardline_hub_creation_policies",
            "configured",
            "boolean",
            true,
            Some("false"),
        ),
        (
            "shardline_hub_creation_policies",
            "enabled",
            "boolean",
            true,
            Some("true"),
        ),
        (
            "shardline_hub_creation_policies",
            "window_seconds",
            "bigint",
            true,
            Some("3600"),
        ),
    ] {
        let actual: Option<(String, bool, Option<String>)> = sqlx::query_as(
            "SELECT format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid)
             FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
             WHERE a.attrelid=to_regclass(format('%I.%I',current_schema(),$1))
               AND a.attname=$2 AND a.attnum>0 AND NOT a.attisdropped",
        )
        .bind(table)
        .bind(column)
        .fetch_optional(&mut *connection)
        .await?;
        if actual
            .as_ref()
            .is_none_or(|(k, r, d)| k != kind || *r != required || d.as_deref() != default)
        {
            return Err(incompatible(column));
        }
    }
    Ok(())
}

async fn check_indexes(connection: &mut PgConnection) -> Result<(), DatabaseMigrationError> {
    for (name, table, keys) in [
        (
            "shardline_hub_creation_events_window",
            "shardline_hub_creation_events",
            &["namespace", "created_at_unix_seconds"][..],
        ),
        (
            "shardline_hub_repos_creation_namespace",
            "shardline_hub_repos",
            &["split_part(repo_id, '/'::text, 1)"][..],
        ),
    ] {
        let valid: bool = query_scalar(
            "SELECT COALESCE((SELECT i.indisvalid AND i.indisready AND NOT i.indisunique
             AND i.indnatts=i.indnkeyatts AND i.indpred IS NULL AND a.amname='btree'
             AND i.indrelid=to_regclass(format('%I.%I',current_schema(),$2))
             AND ARRAY(SELECT pg_get_indexdef(i.indexrelid,n,true)
                       FROM generate_series(1,i.indnkeyatts) AS n)=$3::text[]
             FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
             WHERE i.indexrelid=to_regclass(format('%I.%I',current_schema(),$1))),false)",
        )
        .bind(name)
        .bind(table)
        .bind(keys)
        .fetch_one(&mut *connection)
        .await?;
        if !valid {
            return Err(incompatible(name));
        }
    }
    Ok(())
}
