#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{
    fs,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Mutex, Once, OnceLock},
    thread::sleep,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Raw S3 configuration returned by [`DockerLocalStack::s3_raw_config`].
pub struct S3RawConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub key_prefix: Option<String>,
    pub allow_http: bool,
}

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

static PROCESS_CLEANUP_CONTAINERS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static PROCESS_CLEANUP_REGISTERED: Once = Once::new();

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
    redis_tls: bool,
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
    tls_material: Option<RedisTlsMaterial>,
}

#[derive(Debug)]
struct RedisTlsMaterial {
    directory: tempfile::TempDir,
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

    /// Returns the TLS Redis URL when the stack includes an mTLS Redis service.
    #[must_use]
    pub fn redis_tls_url(&self) -> Option<String> {
        self.redis.as_ref().and_then(|service| {
            service
                .tls_material
                .as_ref()
                .map(|_material| format!("rediss://127.0.0.1:{}/", service.host_port))
        })
    }

    /// Returns the CA certificate for the mTLS Redis service.
    #[must_use]
    pub fn redis_tls_ca_cert_path(&self) -> Option<PathBuf> {
        self.redis
            .as_ref()?
            .tls_material
            .as_ref()
            .map(|material| material.directory.path().join("ca-cert.pem"))
    }

    /// Returns the client certificate accepted by the mTLS Redis service.
    #[must_use]
    pub fn redis_tls_client_cert_path(&self) -> Option<PathBuf> {
        self.redis
            .as_ref()?
            .tls_material
            .as_ref()
            .map(|material| material.directory.path().join("client-cert.pem"))
    }

    /// Returns the client key accepted by the mTLS Redis service.
    #[must_use]
    pub fn redis_tls_client_key_path(&self) -> Option<PathBuf> {
        self.redis
            .as_ref()?
            .tls_material
            .as_ref()
            .map(|material| material.directory.path().join("client-key.pem"))
    }

    /// Returns raw S3 config for the MinIO service.
    #[must_use]
    pub fn s3_raw_config(&self, key_prefix: Option<&str>) -> Option<S3RawConfig> {
        self.minio.as_ref().map(|service| S3RawConfig {
            bucket: DEFAULT_S3_BUCKET.to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: Some(format!("http://127.0.0.1:{}", service.host_port)),
            access_key: Some(MINIO_ROOT_USER.to_owned()),
            secret_key: Some(MINIO_ROOT_PASSWORD.to_owned()),
            session_token: None,
            key_prefix: key_prefix.map(str::to_owned),
            allow_http: true,
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

    /// Stops the Postgres service while retaining its container and data for a
    /// later [`Self::start_postgres`] call.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when Postgres is not configured or Docker cannot
    /// stop the service.
    pub fn stop_postgres(&self) -> Result<(), IoError> {
        let service = self
            .postgres
            .as_ref()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "postgres is not configured"))?;
        stop_container(&service.container_name)
    }

    /// Starts a previously stopped Postgres service and waits until it accepts
    /// database connections. Docker may assign a new host port, so callers must
    /// retrieve [`Self::postgres_url`] again after this returns.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when Postgres is not configured, Docker cannot
    /// start the service, or it does not become ready in time.
    pub fn start_postgres(&mut self) -> Result<(), IoError> {
        let service = self
            .postgres
            .as_mut()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "postgres is not configured"))?;
        start_container(&service.container_name)?;
        service.host_port = docker_published_port(&service.container_name, 5432)?;
        wait_for_postgres(&service.container_name, service.host_port)?;
        Ok(())
    }

    /// Stops the MinIO service while retaining its container and data for a
    /// later [`Self::start_minio`] call.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when MinIO is not configured or Docker cannot
    /// stop the service.
    pub fn stop_minio(&self) -> Result<(), IoError> {
        let service = self
            .minio
            .as_ref()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "minio is not configured"))?;
        stop_container(&service.container_name)
    }

    /// Starts a previously stopped MinIO service and waits until its S3 API is
    /// available again. Docker may assign a new host port, so callers must
    /// retrieve [`Self::s3_raw_config`] again after this returns.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when MinIO is not configured, Docker cannot start
    /// the service, or it does not become ready in time.
    pub fn start_minio(&mut self) -> Result<(), IoError> {
        let service = self
            .minio
            .as_mut()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "minio is not configured"))?;
        start_container(&service.container_name)?;
        wait_for_minio(&service.container_name)?;
        service.host_port = docker_published_port(&service.container_name, 9000)?;
        Ok(())
    }

    /// Stops the Redis service while retaining its container and data for a
    /// later [`Self::start_redis`] call.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when Redis is not configured or Docker cannot
    /// stop the service.
    pub fn stop_redis(&self) -> Result<(), IoError> {
        let service = self
            .redis
            .as_ref()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "redis is not configured"))?;
        stop_container(&service.container_name)
    }

    /// Starts a previously stopped Redis service and waits until it responds to
    /// a ping. Docker may assign a new host port, so callers must retrieve
    /// [`Self::redis_url`] again after this returns.
    ///
    /// # Errors
    ///
    /// Returns an [`IoError`] when Redis is not configured, Docker cannot start
    /// the service, or it does not become ready in time.
    pub fn start_redis(&mut self) -> Result<(), IoError> {
        let service = self
            .redis
            .as_mut()
            .ok_or_else(|| IoError::new(ErrorKind::NotFound, "redis is not configured"))?;
        start_container(&service.container_name)?;
        wait_for_redis_service(service)?;
        service.host_port = docker_published_port(&service.container_name, 6379)?;
        Ok(())
    }
}

impl Drop for DockerLocalStack {
    fn drop(&mut self) {
        if let Some(service) = self.postgres.take() {
            let _ignored = remove_container(&service.container_name);
            untrack_container_for_process_cleanup(&service.container_name);
        }
        if let Some(service) = self.minio.take() {
            let _ignored = remove_container(&service.container_name);
            untrack_container_for_process_cleanup(&service.container_name);
        }
        if let Some(service) = self.redis.take() {
            let _ignored = remove_container(&service.container_name);
            untrack_container_for_process_cleanup(&service.container_name);
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

    /// Enables a Redis service that requires TLS client authentication.
    #[must_use]
    pub const fn with_redis_tls(mut self) -> Self {
        self.redis_tls = true;
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
        let mut stack = DockerLocalStack {
            postgres: None,
            minio: None,
            redis: None,
        };
        if self.postgres {
            stack.postgres = Some(start_postgres_service(&run_id)?);
        }
        if self.minio {
            stack.minio = Some(start_minio_service(&run_id)?);
        }
        if self.redis_tls {
            stack.redis = Some(start_redis_tls_service(&run_id)?);
        } else if self.redis {
            stack.redis = Some(start_redis_service(&run_id)?);
        }

        if let Some(service) = stack.postgres.as_ref() {
            track_container_for_process_cleanup(&service.container_name);
        }
        if let Some(service) = stack.minio.as_ref() {
            track_container_for_process_cleanup(&service.container_name);
        }
        if let Some(service) = stack.redis.as_ref() {
            track_container_for_process_cleanup(&service.container_name);
        }

        Ok(Some(stack))
    }
}

fn start_postgres_service(run_id: &str) -> Result<PostgresService, IoError> {
    let container_name = format!("shardline-test-postgres-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
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
    let service = (|| {
        let host_port = docker_published_port(&container_name, 5432)?;
        wait_for_postgres(&container_name, host_port)?;

        Ok(PostgresService {
            container_name: container_name.clone(),
            host_port,
        })
    })();
    remove_container_after_start_failure(&container_name, &service);
    service
}

fn start_minio_service(run_id: &str) -> Result<MinioService, IoError> {
    let container_name = format!("shardline-test-minio-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
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
    let service = (|| {
        let host_port = docker_published_port(&container_name, 9000)?;
        let mc_host =
            format!("http://{MINIO_ROOT_USER}:{MINIO_ROOT_PASSWORD}@127.0.0.1:{host_port}");
        wait_for_minio(&container_name)?;
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
            container_name: container_name.clone(),
            host_port,
        })
    })();
    remove_container_after_start_failure(&container_name, &service);
    service
}

fn start_redis_service(run_id: &str) -> Result<RedisService, IoError> {
    let container_name = format!("shardline-test-redis-{run_id}");
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
            .arg("--name")
            .arg(&container_name)
            .arg("-p")
            .arg("127.0.0.1::6379")
            .arg(REDIS_IMAGE),
        "start redis container",
    )?;
    let service = (|| {
        let host_port = docker_published_port(&container_name, 6379)
            .map_err(|error| container_start_error(&container_name, &error))?;
        let service = RedisService {
            container_name: container_name.clone(),
            host_port,
            tls_material: None,
        };
        wait_for_redis_service(&service)?;
        Ok(service)
    })();
    remove_container_after_start_failure(&container_name, &service);
    service
}

fn start_redis_tls_service(run_id: &str) -> Result<RedisService, IoError> {
    let tls_material = generate_redis_tls_material()?;
    let container_name = format!("shardline-test-redis-tls-{run_id}");
    let mount = format!("{}:/tls:ro", tls_material.directory.path().display());
    run_command_checked(
        Command::new("docker")
            .arg("run")
            .arg("-d")
            .arg("--name")
            .arg(&container_name)
            .arg("--user")
            .arg("0:0")
            .arg("-v")
            .arg(mount)
            .arg("--expose")
            .arg("6379")
            .arg("-p")
            .arg("127.0.0.1::6379")
            .arg(REDIS_IMAGE)
            .arg("redis-server")
            .arg("--port")
            .arg("0")
            .arg("--tls-port")
            .arg("6379")
            .arg("--tls-cert-file")
            .arg("/tls/server-cert.pem")
            .arg("--tls-key-file")
            .arg("/tls/server-key.pem")
            .arg("--tls-ca-cert-file")
            .arg("/tls/ca-cert.pem")
            .arg("--tls-auth-clients")
            .arg("yes"),
        "start TLS redis container",
    )?;
    let service = (|| {
        let host_port = docker_published_port(&container_name, 6379)
            .map_err(|error| container_start_error(&container_name, &error))?;
        let service = RedisService {
            container_name: container_name.clone(),
            host_port,
            tls_material: Some(tls_material),
        };
        wait_for_redis_service(&service)?;
        Ok(service)
    })();
    remove_container_after_start_failure(&container_name, &service);
    service
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

fn stop_container(container_name: &str) -> Result<(), IoError> {
    run_command_checked(
        Command::new("docker").arg("stop").arg(container_name),
        "stop container",
    )?;
    Ok(())
}

fn start_container(container_name: &str) -> Result<(), IoError> {
    run_command_checked(
        Command::new("docker").arg("start").arg(container_name),
        "start container",
    )?;
    Ok(())
}

fn wait_for_postgres(container_name: &str, host_port: u16) -> Result<(), IoError> {
    wait_for(
        || {
            let container_ready = run_command(
                Command::new("docker")
                    .arg("exec")
                    .arg(container_name)
                    .arg("pg_isready")
                    .arg("-U")
                    .arg(POSTGRES_USER)
                    .arg("-d")
                    .arg(POSTGRES_DATABASE),
            )
            .is_ok_and(|output| output.status.success());
            let published_port_ready = run_command(
                Command::new("pg_isready")
                    .arg("-h")
                    .arg("127.0.0.1")
                    .arg("-p")
                    .arg(host_port.to_string())
                    .arg("-U")
                    .arg(POSTGRES_USER)
                    .arg("-d")
                    .arg(POSTGRES_DATABASE),
            )
            .is_ok_and(|output| output.status.success());
            container_ready && published_port_ready
        },
        "postgres readiness",
    )
}

fn wait_for_minio(container_name: &str) -> Result<(), IoError> {
    wait_for(
        || {
            run_command(
                Command::new("docker")
                    .arg("exec")
                    .arg(container_name)
                    .arg("curl")
                    .arg("--fail")
                    .arg("--silent")
                    .arg("http://127.0.0.1:9000/minio/health/live"),
            )
            .is_ok_and(|output| output.status.success())
        },
        "minio readiness",
    )
}

fn wait_for_redis_service(service: &RedisService) -> Result<(), IoError> {
    wait_for(
        || {
            let mut command = Command::new("docker");
            command
                .arg("exec")
                .arg(&service.container_name)
                .arg("redis-cli");
            if service.tls_material.is_some() {
                command
                    .arg("--tls")
                    .arg("--cacert")
                    .arg("/tls/ca-cert.pem")
                    .arg("--cert")
                    .arg("/tls/client-cert.pem")
                    .arg("--key")
                    .arg("/tls/client-key.pem");
            }
            command.arg("ping");
            run_command(&mut command).is_ok_and(|output| output.status.success())
        },
        "redis readiness",
    )
}

fn generate_redis_tls_material() -> Result<RedisTlsMaterial, IoError> {
    let directory = tempfile::tempdir()?;
    let ca_key = directory.path().join("ca-key.pem");
    let ca_cert = directory.path().join("ca-cert.pem");
    generate_redis_tls_ca(&ca_key, &ca_cert)?;
    generate_redis_tls_identity(directory.path(), "server", &ca_key, &ca_cert, true)?;
    generate_redis_tls_identity(directory.path(), "client", &ca_key, &ca_cert, false)?;
    make_redis_tls_material_readable(directory.path())?;
    Ok(RedisTlsMaterial { directory })
}

#[cfg(unix)]
fn make_redis_tls_material_readable(directory: &Path) -> Result<(), IoError> {
    fs::set_permissions(directory, fs::Permissions::from_mode(0o755))?;
    for file_name in [
        "ca-cert.pem",
        "server-cert.pem",
        "server-key.pem",
        "client-cert.pem",
        "client-key.pem",
    ] {
        fs::set_permissions(directory.join(file_name), fs::Permissions::from_mode(0o644))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn make_redis_tls_material_readable(_directory: &Path) -> Result<(), IoError> {
    Ok(())
}

fn generate_redis_tls_ca(ca_key: &Path, ca_cert: &Path) -> Result<(), IoError> {
    run_command_checked(
        Command::new("openssl")
            .arg("req")
            .arg("-x509")
            .arg("-newkey")
            .arg("rsa:2048")
            .arg("-nodes")
            .arg("-sha256")
            .arg("-days")
            .arg("1")
            .arg("-subj")
            .arg("/CN=shardline-redis-test-ca")
            .arg("-keyout")
            .arg(ca_key)
            .arg("-out")
            .arg(ca_cert),
        "generate redis TLS certificate authority",
    )?;
    Ok(())
}

fn generate_redis_tls_identity(
    directory: &Path,
    identity: &str,
    ca_key: &Path,
    ca_cert: &Path,
    is_server: bool,
) -> Result<(), IoError> {
    let key = directory.join(format!("{identity}-key.pem"));
    let request = directory.join(format!("{identity}.csr"));
    let cert = directory.join(format!("{identity}-cert.pem"));
    let subject = format!("/CN=shardline-redis-test-{identity}");
    let ext_file = directory.join(format!("{identity}.ext"));
    fs::write(
        &ext_file,
        "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n",
    )?;
    let mut request_command = Command::new("openssl");
    request_command
        .arg("req")
        .arg("-new")
        .arg("-newkey")
        .arg("rsa:2048")
        .arg("-nodes")
        .arg("-subj")
        .arg(subject)
        .arg("-keyout")
        .arg(&key)
        .arg("-out")
        .arg(&request);
    if is_server {
        request_command
            .arg("-addext")
            .arg("subjectAltName=DNS:localhost,IP:127.0.0.1");
    }
    run_command_checked(
        &mut request_command,
        "generate redis TLS certificate request",
    )?;
    run_command_checked(
        Command::new("openssl")
            .arg("x509")
            .arg("-req")
            .arg("-in")
            .arg(request)
            .arg("-CA")
            .arg(ca_cert)
            .arg("-CAkey")
            .arg(ca_key)
            .arg("-CAcreateserial")
            .arg("-days")
            .arg("1")
            .arg("-sha256")
            .arg("-copy_extensions")
            .arg("copy")
            .arg("-extfile")
            .arg(&ext_file)
            .arg("-out")
            .arg(cert),
        "sign redis TLS certificate",
    )?;
    Ok(())
}

fn remove_container_after_start_failure<T>(container_name: &str, result: &Result<T, IoError>) {
    if result.is_err() {
        let _ignored = remove_container(container_name);
    }
}

fn container_start_error(container_name: &str, error: &IoError) -> IoError {
    let logs = run_command(Command::new("docker").arg("logs").arg(container_name))
        .ok()
        .map(|output| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        })
        .filter(|logs| !logs.trim().is_empty());
    let message = logs.map_or_else(|| error.to_string(), |logs| format!("{error}: {logs}"));
    IoError::new(error.kind(), message)
}

fn track_container_for_process_cleanup(container_name: &str) {
    PROCESS_CLEANUP_REGISTERED.call_once(|| {
        // SAFETY: `cleanup_tracked_containers` has the C ABI, captures no state, and
        // is valid for the entire lifetime of this process.
        let result = unsafe { libc::atexit(cleanup_tracked_containers) };
        debug_assert_eq!(result, 0, "register container cleanup with atexit");
    });
    let containers = PROCESS_CLEANUP_CONTAINERS.get_or_init(|| Mutex::new(Vec::new()));
    let Ok(mut containers) = containers.lock() else {
        return;
    };
    containers.push(container_name.to_owned());
}

fn untrack_container_for_process_cleanup(container_name: &str) {
    let Some(containers) = PROCESS_CLEANUP_CONTAINERS.get() else {
        return;
    };
    let Ok(mut containers) = containers.lock() else {
        return;
    };
    containers.retain(|tracked_name| tracked_name != container_name);
}

extern "C" fn cleanup_tracked_containers() {
    let Some(containers) = PROCESS_CLEANUP_CONTAINERS.get() else {
        return;
    };
    let Ok(mut containers) = containers.lock() else {
        return;
    };
    while let Some(container_name) = containers.pop() {
        let _ignored = remove_container(&container_name);
    }
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
