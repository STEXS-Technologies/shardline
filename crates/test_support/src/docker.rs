use std::{
    io::{Error as IoError, ErrorKind},
    process::{Command, Output},
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use shardline_storage::S3ObjectStoreConfig;

const POSTGRES_IMAGE: &str = "postgres:16-alpine";
const MINIO_IMAGE: &str = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z";
const MINIO_MC_IMAGE: &str = "quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z";
const REDIS_IMAGE: &str = "redis:7-alpine";

const POSTGRES_USER: &str = "shardline";
const POSTGRES_PASSWORD: &str = "change-me";
const POSTGRES_DATABASE: &str = "shardline";
const MINIO_ROOT_USER: &str = "minio";
const MINIO_ROOT_PASSWORD: &str = "miniosecret";
const DEFAULT_S3_BUCKET: &str = "shardline-e2e";

/// Containerized service stack for self-contained end-to-end tests.
#[derive(Debug)]
pub struct DockerLocalStack {
    postgres: Option<PostgresService>,
    minio: Option<MinioService>,
    redis: Option<RedisService>,
}

/// Builder for [`DockerLocalStack`].
#[derive(Debug, Clone, Copy, Default)]
pub struct DockerLocalStackBuilder {
    postgres: bool,
    minio: bool,
    redis: bool,
}

#[derive(Debug)]
struct PostgresService {
    container_name: String,
    host_port: u16,
}

#[derive(Debug)]
struct MinioService {
    container_name: String,
    host_port: u16,
}

#[derive(Debug)]
struct RedisService {
    container_name: String,
    host_port: u16,
}

impl DockerLocalStack {
    /// Returns `true` when the Docker CLI is available to the test process.
    #[must_use]
    pub fn docker_available() -> bool {
        Command::new("docker")
            .arg("version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    /// Creates a builder for a disposable local service stack.
    #[must_use]
    pub fn builder() -> DockerLocalStackBuilder {
        DockerLocalStackBuilder::default()
    }

    /// Returns the Postgres metadata URL when the stack includes Postgres.
    #[must_use]
    pub fn postgres_url(&self) -> Option<String> {
        self.postgres.as_ref().map(|service| {
            format!(
                "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{}/{}",
                service.host_port, POSTGRES_DATABASE
            )
        })
    }

    /// Returns the Redis URL when the stack includes Redis.
    #[must_use]
    pub fn redis_url(&self) -> Option<String> {
        self.redis
            .as_ref()
            .map(|service| format!("redis://127.0.0.1:{}/", service.host_port))
    }

    /// Returns an S3 object-store config for the MinIO service.
    #[must_use]
    pub fn s3_config_with_prefix(&self, key_prefix: Option<&str>) -> Option<S3ObjectStoreConfig> {
        self.minio.as_ref().map(|service| {
            S3ObjectStoreConfig::new(DEFAULT_S3_BUCKET.to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(format!("http://127.0.0.1:{}", service.host_port)))
                .with_credentials(
                    Some(MINIO_ROOT_USER.to_owned()),
                    Some(MINIO_ROOT_PASSWORD.to_owned()),
                    None,
                )
                .with_key_prefix(key_prefix)
                .with_allow_http(true)
        })
    }

    /// Returns a unique test key prefix suitable for isolated object-store runs.
    #[must_use]
    pub fn unique_s3_key_prefix(&self, prefix: &str) -> String {
        let unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |duration| duration.as_nanos());
        format!("{prefix}/{unix_nanos}")
    }
}

impl Drop for DockerLocalStack {
    fn drop(&mut self) {
        if let Some(service) = self.postgres.take() {
            let _ignored = remove_container(&service.container_name);
        }
        if let Some(service) = self.minio.take() {
            let _ignored = remove_container(&service.container_name);
        }
        if let Some(service) = self.redis.take() {
            let _ignored = remove_container(&service.container_name);
        }
    }
}

impl DockerLocalStackBuilder {
    /// Enables Postgres for the stack.
    #[must_use]
    pub const fn with_postgres(mut self) -> Self {
        self.postgres = true;
        self
    }

    /// Enables MinIO for the stack.
    #[must_use]
    pub const fn with_minio(mut self) -> Self {
        self.minio = true;
        self
    }

    /// Enables Redis for the stack.
    #[must_use]
    pub const fn with_redis(mut self) -> Self {
        self.redis = true;
        self
    }

    /// Starts the configured stack.
    ///
    /// Returns `Ok(None)` when Docker is unavailable so tests can skip cleanly.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when container startup, readiness checks, or
    /// MinIO bucket creation fails.
    pub fn start(self) -> Result<Option<DockerLocalStack>, IoError> {
        if !DockerLocalStack::docker_available() {
            return Ok(None);
        }

        let run_id = unique_run_id();
        let postgres = if self.postgres {
            Some(start_postgres_service(&run_id)?)
        } else {
            None
        };
        let minio = if self.minio {
            Some(start_minio_service(&run_id)?)
        } else {
            None
        };
        let redis = if self.redis {
            Some(start_redis_service(&run_id)?)
        } else {
            None
        };

        Ok(Some(DockerLocalStack {
            postgres,
            minio,
            redis,
        }))
    }
}

fn start_postgres_service(run_id: &str) -> Result<PostgresService, IoError> {
    let container_name = format!("shardline-test-postgres-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
            .arg("--rm")
            .arg("--name")
            .arg(&container_name)
            .arg("-e")
            .arg(format!("POSTGRES_USER={POSTGRES_USER}"))
            .arg("-e")
            .arg(format!("POSTGRES_PASSWORD={POSTGRES_PASSWORD}"))
            .arg("-e")
            .arg(format!("POSTGRES_DB={POSTGRES_DATABASE}"))
            .arg("-p")
            .arg("127.0.0.1::5432")
            .arg(POSTGRES_IMAGE),
        "start postgres container",
    )?;
    let host_port = docker_published_port(&container_name, 5432)?;
    wait_for(
        || {
            run_command(
                Command::new("docker")
                    .arg("exec")
                    .arg(&container_name)
                    .arg("pg_isready")
                    .arg("-U")
                    .arg(POSTGRES_USER)
                    .arg("-d")
                    .arg(POSTGRES_DATABASE),
            )
            .is_ok_and(|output| output.status.success())
        },
        "postgres readiness",
    )?;

    Ok(PostgresService {
        container_name,
        host_port,
    })
}

fn start_minio_service(run_id: &str) -> Result<MinioService, IoError> {
    let container_name = format!("shardline-test-minio-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
            .arg("--rm")
            .arg("--name")
            .arg(&container_name)
            .arg("-e")
            .arg(format!("MINIO_ROOT_USER={MINIO_ROOT_USER}"))
            .arg("-e")
            .arg(format!("MINIO_ROOT_PASSWORD={MINIO_ROOT_PASSWORD}"))
            .arg("-p")
            .arg("127.0.0.1::9000")
            .arg("-p")
            .arg("127.0.0.1::9001")
            .arg(MINIO_IMAGE)
            .arg("server")
            .arg("/data")
            .arg("--console-address")
            .arg(":9001"),
        "start minio container",
    )?;
    let host_port = docker_published_port(&container_name, 9000)?;
    let mc_host = format!("http://{MINIO_ROOT_USER}:{MINIO_ROOT_PASSWORD}@127.0.0.1:{host_port}");
    wait_for(
        || {
            run_command(Command::new("docker").arg("logs").arg(&container_name)).is_ok_and(
                |output| {
                    let combined = format!(
                        "{}{}",
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    );
                    output.status.success() && combined.contains("API:")
                },
            )
        },
        "minio readiness",
    )?;
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("--rm")
            .arg("--network")
            .arg("host")
            .arg("-e")
            .arg(format!("MC_HOST_local={mc_host}"))
            .arg(MINIO_MC_IMAGE)
            .arg("mb")
            .arg("--ignore-existing")
            .arg(format!("local/{DEFAULT_S3_BUCKET}")),
        "create minio bucket",
    )?;

    Ok(MinioService {
        container_name,
        host_port,
    })
}

fn start_redis_service(run_id: &str) -> Result<RedisService, IoError> {
    let container_name = format!("shardline-test-redis-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
            .arg("--rm")
            .arg("--name")
            .arg(&container_name)
            .arg("-p")
            .arg("127.0.0.1::6379")
            .arg(REDIS_IMAGE),
        "start redis container",
    )?;
    let host_port = docker_published_port(&container_name, 6379)?;
    wait_for(
        || {
            run_command(
                Command::new("docker")
                    .arg("exec")
                    .arg(&container_name)
                    .arg("redis-cli")
                    .arg("ping"),
            )
            .is_ok_and(|output| output.status.success())
        },
        "redis readiness",
    )?;

    Ok(RedisService {
        container_name,
        host_port,
    })
}

fn unique_run_id() -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0_u128, |duration| duration.as_nanos());
    format!("{}-{unix_nanos}", std::process::id())
}

fn docker_published_port(container_name: &str, container_port: u16) -> Result<u16, IoError> {
    let output = run_command_checked(
        Command::new("docker")
            .arg("port")
            .arg(container_name)
            .arg(format!("{container_port}/tcp")),
        "inspect docker port mapping",
    )?;
    let value = String::from_utf8(output.stdout)
        .map_err(|error| IoError::new(ErrorKind::InvalidData, error))?;
    let raw_port = value
        .trim()
        .rsplit_once(':')
        .map(|(_host, port)| port)
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "docker port output was invalid"))?;
    raw_port
        .parse::<u16>()
        .map_err(|error| IoError::new(ErrorKind::InvalidData, error))
}

fn remove_container(container_name: &str) -> Result<(), IoError> {
    run_command_checked(
        Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(container_name),
        "remove container",
    )?;
    Ok(())
}

fn wait_for(mut check: impl FnMut() -> bool, description: &str) -> Result<(), IoError> {
    for _attempt in 0..60 {
        if check() {
            return Ok(());
        }
        sleep(Duration::from_secs(1));
    }

    Err(IoError::new(
        ErrorKind::TimedOut,
        format!("{description} timed out"),
    ))
}

fn run_command_checked(command: &mut Command, description: &str) -> Result<Output, IoError> {
    let output = run_command(command)?;
    if output.status.success() {
        return Ok(output);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(IoError::other(format!(
        "{description} failed with status {}: {stderr}",
        output.status
    )))
}

fn run_command(command: &mut Command) -> Result<Output, IoError> {
    command.output()
}
