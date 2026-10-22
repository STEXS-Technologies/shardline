//! Derived subject mappings do not alter OCI visibility or publication evidence.
use sqlx::{Connection as _, Row as _, query, query_scalar};

use super::{PostgresIndexStore, PostgresMetadataStoreError};
use crate::OciManifestSubject;

fn conflict() -> PostgresMetadataStoreError {
    PostgresMetadataStoreError::ReliabilityEventConflict(
        "OCI manifest subject index mismatch".to_owned(),
    )
}

async fn exists(connection: &mut sqlx::PgConnection) -> Result<bool, PostgresMetadataStoreError> {
    Ok(query_scalar("SELECT to_regclass(format('%I.shardline_oci_manifest_subjects_v1', current_schema())) IS NOT NULL")
        .fetch_one(connection).await?)
}

async fn lock_mapping(
    connection: &mut sqlx::PgConnection,
    key: &crate::OciObjectKey,
) -> Result<(), PostgresMetadataStoreError> {
    // Separate from publication/resource fences: only serializes this optional
    // derived row with GC cleanup. Hash collisions only serialize extra work.
    query("SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text,$2::text,$3::text)::text, 73140519))")
        .bind(&key.scope_namespace).bind(&key.repository).bind(&key.digest_hex)
        .execute(connection).await?;
    Ok(())
}

pub(super) async fn forget_reclaimed_manifest(
    connection: &mut sqlx::PgConnection,
    key: &crate::OciObjectKey,
) -> Result<(), PostgresMetadataStoreError> {
    if key.kind != crate::OciObjectKind::Manifest {
        return Ok(());
    }
    // Cleanup is optional for legacy schemas and existing least-privilege GC
    // roles. Never install tables or require additional grants during serving.
    let schema: Option<String> = query_scalar(
        "SELECT CASE WHEN relation IS NOT NULL
         AND has_table_privilege(current_user, relation, 'SELECT')
         AND has_table_privilege(current_user, relation, 'DELETE')
         THEN current_schema() ELSE NULL END
         FROM (SELECT to_regclass(format('%I.shardline_oci_manifest_subjects_v1', current_schema())) AS relation) AS lookup"
    ).fetch_one(&mut *connection).await?;
    if let Some(schema) = schema {
        lock_mapping(connection, key).await?;
        let schema = schema.replace('"', "\"\"");
        query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM \"{schema}\".shardline_oci_manifest_subjects_v1
             WHERE scope_namespace=$1 AND repository=$2 AND manifest_digest=$3"
        )))
        .bind(&key.scope_namespace)
        .bind(&key.repository)
        .bind(&key.digest_hex)
        .execute(connection)
        .await?;
    }
    Ok(())
}

impl PostgresIndexStore {
    /// Reports whether this connection role can maintain the optional lookup
    /// index. Read-only roles/replicas and schemas not yet owner-upgraded retain
    /// authoritative manifest discovery without attempting metadata writes.
    ///
    /// # Errors
    /// Returns database/catalog access failures; errors are not availability hints.
    pub async fn oci_referrer_index_writable(&self) -> Result<bool, PostgresMetadataStoreError> {
        Ok(query_scalar("SELECT CASE WHEN relation IS NULL THEN FALSE ELSE
            has_table_privilege(current_user, relation, 'SELECT') AND
            has_table_privilege(current_user, relation, 'INSERT') AND
            current_setting('transaction_read_only')='off' END
            FROM (SELECT to_regclass(format('%I.shardline_oci_manifest_subjects_v1', current_schema())) AS relation) AS lookup")
            .fetch_one(&self.pool).await?)
    }

    /// Installs the derived subject index with migration-owner privileges.
    /// Mirrors existing OCI tag SELECT/INSERT grants, including inherited role
    /// grants, without requiring runtime DDL or changing released migrations.
    ///
    /// # Errors
    /// Returns insufficient owner permissions or other database errors.
    pub async fn initialize_oci_referrer_index(&self) -> Result<(), sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(transaction.as_mut())
            .await?;
        initialize(transaction.as_mut(), &schema).await?;
        transaction.commit().await
    }

    /// Retains one verified immutable manifest's subject mapping. Repeated
    /// identical values do not rewrite it; conflicting mappings fail closed.
    /// This optional additive index never changes publication or deletion state.
    ///
    /// # Errors
    /// Returns database, digest validation, corruption or immutable mapping errors.
    pub async fn retain_oci_manifest_subject(
        &self,
        entry: &OciManifestSubject,
    ) -> Result<(), PostgresMetadataStoreError> {
        self.retain_subject(entry, false).await
    }

    /// Backfills verified bytes without recreating mappings removed by physical
    /// reclamation. Uses the verified lifecycle head under the cleanup lock.
    ///
    /// # Errors
    /// Returns database, evidence verification or immutable mapping errors.
    pub async fn reconcile_oci_manifest_subject(
        &self,
        entry: &OciManifestSubject,
    ) -> Result<(), PostgresMetadataStoreError> {
        self.retain_subject(entry, true).await
    }

    async fn retain_subject(
        &self,
        entry: &OciManifestSubject,
        reconciliation: bool,
    ) -> Result<(), PostgresMetadataStoreError> {
        if !entry.valid() {
            return Err(conflict());
        }
        let mut connection = self.pool.acquire().await?;
        let mut transaction = connection.begin().await?;
        if !exists(transaction.as_mut()).await? {
            return Err(PostgresMetadataStoreError::Unsupported(
                "OCI subject indexing requires the existing db migrate up command".to_owned(),
            ));
        }
        let key = crate::OciObjectKey {
            scope_namespace: entry.scope_namespace.clone(),
            repository: entry.repository.clone(),
            kind: crate::OciObjectKind::Manifest,
            digest_hex: entry.digest_hex.clone(),
        };
        if reconciliation {
            lock_mapping(transaction.as_mut(), &key).await?;
        }
        if reconciliation
            && super::oci_objects::load_oci_evidence(transaction.as_mut(), &key)
                .await?
                .events()
                .last()
                .is_some_and(|head| {
                    head.after.state == shardline_reliability::OciObjectLifecycleState::Reclaimed
                })
        {
            return Ok(());
        }
        let expected = entry.commitment()?;
        query("INSERT INTO shardline_oci_manifest_subjects_v1 VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING")
            .bind(&entry.scope_namespace).bind(&entry.repository).bind(&entry.digest_hex)
            .bind(entry.subject_digest_hex.as_deref().unwrap_or("")).bind(&expected)
            .execute(transaction.as_mut()).await?;
        let row = query("SELECT CASE WHEN octet_length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END AS subject,
            CASE WHEN octet_length(commitment)<=128 THEN commitment ELSE '' END AS commitment
            FROM shardline_oci_manifest_subjects_v1 WHERE scope_namespace=$1 AND repository=$2 AND manifest_digest=$3")
            .bind(&entry.scope_namespace).bind(&entry.repository).bind(&entry.digest_hex)
            .fetch_one(transaction.as_mut()).await?;
        if row.try_get::<String, _>("subject")? != entry.subject_digest_hex.as_deref().unwrap_or("")
            || row.try_get::<String, _>("commitment")? != expected
        {
            return Err(conflict());
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Returns a bounded, corruption-checked page of derived subject mappings.
    /// `None` inventories mappings for legacy reconciliation; a subject selects
    /// candidates through its index. Missing tables are an empty, read-only result.
    /// Callers must still check authoritative publication/deletion visibility.
    ///
    /// # Errors
    /// Returns database, invalid page bound or corruption errors.
    pub async fn list_oci_manifest_subjects(
        &self,
        scope: &str,
        repository: &str,
        subject: Option<&str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciManifestSubject>, PostgresMetadataStoreError> {
        if limit == 0 || limit > 1000 {
            return Err(conflict());
        }
        let mut connection = self.pool.acquire().await?;
        if !exists(&mut connection).await? {
            return Ok(Vec::new());
        }
        let (sql, filter) = subject.map_or(
            ("SELECT CASE WHEN octet_length(manifest_digest)=64 THEN manifest_digest ELSE '' END AS manifest_digest,
                     CASE WHEN octet_length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END AS subject_digest,
                     CASE WHEN octet_length(commitment)<=128 THEN commitment ELSE '' END AS commitment FROM shardline_oci_manifest_subjects_v1
                WHERE scope_namespace=$1 AND repository=$2 AND $3::text='' AND manifest_digest>$4
                ORDER BY shardline_oci_manifest_subjects_v1.manifest_digest LIMIT $5", ""), |subject| {
            ("SELECT CASE WHEN octet_length(manifest_digest)=64 THEN manifest_digest ELSE '' END AS manifest_digest,
                     CASE WHEN octet_length(subject_digest)<=64 THEN subject_digest ELSE 'invalid' END AS subject_digest,
                     CASE WHEN octet_length(commitment)<=128 THEN commitment ELSE '' END AS commitment FROM shardline_oci_manifest_subjects_v1
                WHERE scope_namespace=$1 AND repository=$2 AND subject_digest=$3 AND manifest_digest>$4
                ORDER BY shardline_oci_manifest_subjects_v1.manifest_digest LIMIT $5", subject)
        });
        let rows = query(sql)
            .bind(scope)
            .bind(repository)
            .bind(filter)
            .bind(after.unwrap_or(""))
            .bind(limit as i64)
            .fetch_all(&mut *connection)
            .await?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let stored_subject: String = row.try_get("subject_digest")?;
            let entry = OciManifestSubject {
                scope_namespace: scope.to_owned(),
                repository: repository.to_owned(),
                digest_hex: row.try_get("manifest_digest")?,
                subject_digest_hex: if stored_subject.is_empty() {
                    None
                } else {
                    Some(stored_subject)
                },
            };
            if !entry.valid() || entry.commitment()? != row.try_get::<String, _>("commitment")? {
                return Err(conflict());
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}

pub(super) async fn initialize(
    connection: &mut sqlx::PgConnection,
    schema: &str,
) -> Result<(), sqlx::Error> {
    query("SELECT pg_advisory_xact_lock(1936220526, 1868786035)")
        .execute(&mut *connection)
        .await?;
    let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {quoted_schema}.shardline_oci_manifest_subjects_v1 (
            scope_namespace TEXT NOT NULL, repository TEXT NOT NULL,
            manifest_digest TEXT COLLATE \"C\" NOT NULL, subject_digest TEXT COLLATE \"C\" NOT NULL,
            commitment TEXT NOT NULL, PRIMARY KEY (scope_namespace, repository, manifest_digest));
         CREATE INDEX IF NOT EXISTS shardline_oci_manifest_subject_lookup_v1
         ON {quoted_schema}.shardline_oci_manifest_subjects_v1(scope_namespace, repository, subject_digest, manifest_digest);"
    ))).execute(&mut *connection).await?;
    let grants = query("SELECT CASE WHEN acl.grantee=0 THEN NULL ELSE pg_get_userbyid(acl.grantee) END AS grantee, acl.privilege_type
        FROM pg_class AS relation JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace,
        LATERAL aclexplode(COALESCE(relation.relacl, acldefault('r', relation.relowner))) AS acl
        WHERE namespace.nspname=$1 AND relation.relname='shardline_oci_tags' AND acl.privilege_type IN ('SELECT','INSERT')")
        .bind(schema).fetch_all(&mut *connection).await?;
    for grant in grants {
        let privilege: String = grant.try_get("privilege_type")?;
        let privilege = match privilege.as_str() {
            "SELECT" => "SELECT",
            "INSERT" => "INSERT",
            _ => continue,
        };
        let grantee: Option<String> = grant.try_get("grantee")?;
        let role = grantee.map_or_else(
            || "PUBLIC".to_owned(),
            |name| format!("\"{}\"", name.replace('"', "\"\"")),
        );
        query(sqlx::AssertSqlSafe(format!(
            "GRANT {privilege} ON {quoted_schema}.shardline_oci_manifest_subjects_v1 TO {role}"
        )))
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[tokio::test]
    #[ignore = "requires disposable DATABASE_URL"]
    async fn postgres_subject_index_backfill_cannot_resurrect_reclaimed_mapping() {
        use crate::OciObjectStore as _;
        let pool = super::super::connect_isolated_postgres().await.unwrap();
        let store = PostgresIndexStore::new(pool.clone());
        store.initialize_oci_referrer_index().await.unwrap();
        let entry = OciManifestSubject {
            scope_namespace: "scope".to_owned(),
            repository: "repo".to_owned(),
            digest_hex: "1".repeat(64),
            subject_digest_hex: Some("a".repeat(64)),
        };
        let key = crate::OciObjectKey {
            scope_namespace: entry.scope_namespace.clone(),
            repository: entry.repository.clone(),
            kind: crate::OciObjectKind::Manifest,
            digest_hex: entry.digest_hex.clone(),
        };
        for _ in 0..16 {
            store.publish_oci_object(&key, &[]).await.unwrap();
            store.reconcile_oci_manifest_subject(&entry).await.unwrap();
            store.delete_oci_object(&key).await.unwrap();
            let tombstone = store
                .list_oci_object_tombstones()
                .await
                .unwrap()
                .pop()
                .unwrap();
            let (reclaimed, backfilled) = tokio::join!(
                store.delete_oci_object_tombstone_if_unchanged(&tombstone),
                store.reconcile_oci_manifest_subject(&entry)
            );
            assert!(reclaimed.unwrap());
            backfilled.unwrap();
            store.reconcile_oci_manifest_subject(&entry).await.unwrap();
            assert!(
                store
                    .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
        store.publish_oci_object(&key, &[]).await.unwrap();
        store.reconcile_oci_manifest_subject(&entry).await.unwrap();
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .await
                .unwrap(),
            vec![entry]
        );
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL DATABASE_URL with role administration"]
    async fn postgres_subject_index_runtime_writer_needs_no_ddl_permissions() {
        let pool = super::super::connect_isolated_postgres()
            .await
            .expect("DATABASE_URL is required");
        let schema: String = query_scalar("SELECT current_schema()")
            .fetch_one(&pool)
            .await
            .unwrap();
        let quoted_schema = format!("\"{}\"", schema.replace('"', "\"\""));
        let role = format!(
            "oci_subject_writer_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let quoted_role = format!("\"{role}\"");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {quoted_role} NOLOGIN;
            GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_role};
            GRANT SELECT,INSERT ON {quoted_schema}.shardline_oci_tags TO {quoted_role};"
        )))
        .execute(&pool)
        .await
        .unwrap();
        PostgresIndexStore::new(pool.clone())
            .initialize_oci_referrer_index()
            .await
            .unwrap();
        let setup = format!("SET search_path TO {quoted_schema}; SET ROLE {quoted_role};");
        let runtime = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _metadata| {
                let setup = setup.clone();
                Box::pin(async move {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(setup))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*pool.connect_options()).clone())
            .await
            .unwrap();
        let can_create: bool =
            query_scalar("SELECT has_schema_privilege(current_user, current_schema(), 'CREATE')")
                .fetch_one(&runtime)
                .await
                .unwrap();
        assert!(!can_create);
        let store = PostgresIndexStore::new(runtime.clone());
        let entry = OciManifestSubject {
            scope_namespace: "scope".to_owned(),
            repository: "repo".to_owned(),
            digest_hex: "1".repeat(64),
            subject_digest_hex: Some("a".repeat(64)),
        };
        store.retain_oci_manifest_subject(&entry).await.unwrap();
        let key = crate::OciObjectKey {
            scope_namespace: entry.scope_namespace.clone(),
            repository: entry.repository.clone(),
            kind: crate::OciObjectKind::Manifest,
            digest_hex: entry.digest_hex.clone(),
        };
        {
            let mut connection = runtime.acquire().await.unwrap();
            forget_reclaimed_manifest(&mut connection, &key)
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .await
                .unwrap(),
            vec![entry.clone()],
            "missing DELETE grant must retain optional mapping without failing"
        );
        query(sqlx::AssertSqlSafe(format!(
            "GRANT DELETE ON {quoted_schema}.shardline_oci_manifest_subjects_v1 TO {quoted_role}"
        )))
        .execute(&pool)
        .await
        .unwrap();
        {
            let mut connection = runtime.acquire().await.unwrap();
            forget_reclaimed_manifest(&mut connection, &key)
                .await
                .unwrap();
        }
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
        store.retain_oci_manifest_subject(&entry).await.unwrap();
        assert_eq!(
            store
                .list_oci_manifest_subjects(
                    "scope",
                    "repo",
                    entry.subject_digest_hex.as_deref(),
                    None,
                    1
                )
                .await
                .unwrap(),
            vec![entry]
        );
        assert!(store.oci_referrer_index_writable().await.unwrap());
        query("SET default_transaction_read_only=on")
            .execute(&runtime)
            .await
            .unwrap();
        assert!(!store.oci_referrer_index_writable().await.unwrap());
        query("SET default_transaction_read_only=off")
            .execute(&runtime)
            .await
            .unwrap();
        query(sqlx::AssertSqlSafe(format!(
            "REVOKE INSERT ON {quoted_schema}.shardline_oci_manifest_subjects_v1 FROM {quoted_role}"
        )))
        .execute(&pool)
        .await
        .unwrap();
        assert!(!store.oci_referrer_index_writable().await.unwrap());
        runtime.close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {quoted_role}; DROP ROLE {quoted_role};"
        )))
        .execute(&pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires disposable PostgreSQL DATABASE_URL"]
    async fn postgres_subject_index_is_immutable_scoped_and_verified() {
        let pool = super::super::connect_isolated_postgres()
            .await
            .expect("DATABASE_URL is required");
        let store = PostgresIndexStore::new(pool.clone());
        // The fixture copies public tables, including an index created by an
        // earlier server run. Remove only our private copy to model upgrade
        // while a same-named public table may still exist on the search path.
        query("DO $$ BEGIN EXECUTE format('DROP TABLE IF EXISTS %I.shardline_oci_manifest_subjects_v1', current_schema()); END $$")
            .execute(&pool).await.unwrap();
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 1)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(!exists(&mut pool.acquire().await.unwrap()).await.unwrap());
        assert!(!store.oci_referrer_index_writable().await.unwrap());
        let (first_initialization, second_initialization) = tokio::join!(
            store.initialize_oci_referrer_index(),
            store.initialize_oci_referrer_index()
        );
        first_initialization.unwrap();
        second_initialization.unwrap();
        let entry = OciManifestSubject {
            scope_namespace: "scope".to_owned(),
            repository: "repo".to_owned(),
            digest_hex: "1".repeat(64),
            subject_digest_hex: Some("a".repeat(64)),
        };
        let (first, second) = tokio::join!(
            store.retain_oci_manifest_subject(&entry),
            store.retain_oci_manifest_subject(&entry)
        );
        first.unwrap();
        second.unwrap();
        let mut next = entry.clone();
        next.digest_hex = "2".repeat(64);
        next.subject_digest_hex = None;
        store.retain_oci_manifest_subject(&next).await.unwrap();
        assert_eq!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, Some(&entry.digest_hex), 1)
                .await
                .unwrap(),
            vec![next]
        );
        assert_eq!(
            store
                .list_oci_manifest_subjects(
                    "scope",
                    "repo",
                    entry.subject_digest_hex.as_deref(),
                    None,
                    1
                )
                .await
                .unwrap(),
            vec![entry.clone()]
        );
        assert!(
            store
                .list_oci_manifest_subjects("scope", "other", None, None, 1)
                .await
                .unwrap()
                .is_empty()
        );
        let mut conflicting = entry.clone();
        conflicting.subject_digest_hex = None;
        assert!(
            store
                .retain_oci_manifest_subject(&conflicting)
                .await
                .is_err()
        );
        query("UPDATE shardline_oci_manifest_subjects_v1 SET subject_digest=$1")
            .bind("b".repeat(64))
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            store
                .list_oci_manifest_subjects("scope", "repo", None, None, 1)
                .await
                .is_err()
        );
        assert!(store.retain_oci_manifest_subject(&entry).await.is_err());
    }
}
