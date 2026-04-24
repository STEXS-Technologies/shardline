use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use blake3::Hasher as Blake3Hasher;
use sha2::{Digest, Sha256};
use shardline_protocol::ShardlineHash;
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, S3ObjectStore, S3ObjectStoreConfig,
};
use tempfile::TempDir;

const FILE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_ITERATIONS: usize = 8;

struct Fixture {
    _root: TempDir,
    source_path: PathBuf,
    store: S3ObjectStore,
    run_prefix: String,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = std::env::var("S3_BENCH_ENDPOINT")
            .unwrap_or_else(|_error| "http://127.0.0.1:9010".to_owned());
        let bucket =
            std::env::var("S3_BENCH_BUCKET").unwrap_or_else(|_error| "shardline-bench".to_owned());
        let access_key =
            std::env::var("S3_BENCH_ACCESS_KEY").unwrap_or_else(|_error| "minio".to_owned());
        let secret_key =
            std::env::var("S3_BENCH_SECRET_KEY").unwrap_or_else(|_error| "minio123".to_owned());
        let root = tempfile::tempdir()?;
        let source_path = root.path().join("source.bin");
        fs::write(&source_path, vec![0x5a; FILE_BYTES])?;
        let store = S3ObjectStore::new(
            S3ObjectStoreConfig::new(bucket, "us-east-1".to_owned())
                .with_endpoint(Some(endpoint))
                .with_allow_http(true)
                .with_credentials(Some(access_key), Some(secret_key), None),
        )?;
        let run_prefix = format!(
            "bench-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0_u128, |duration| duration.as_nanos())
        );
        Ok(Self {
            _root: root,
            source_path,
            store,
            run_prefix,
        })
    }

    fn object_key(
        &self,
        mode: &str,
        iteration: usize,
    ) -> Result<ObjectKey, Box<dyn std::error::Error>> {
        Ok(ObjectKey::parse(&format!(
            "protocol-bench/s3/{}/{mode}/{iteration:08}",
            self.run_prefix
        ))?)
    }
}

fn main() -> ExitCode {
    let mut mode = "direct";
    let mut iterations = DEFAULT_ITERATIONS;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "old" | "generic" | "direct" => mode = Box::leak(argument.into_boxed_str()),
            "--bench" => {}
            "--iterations" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --iterations");
                    return ExitCode::FAILURE;
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => iterations = parsed,
                    _ => {
                        eprintln!("invalid --iterations value: {value}");
                        return ExitCode::FAILURE;
                    }
                }
            }
            _ => {
                eprintln!(
                    "usage: cargo bench --bench s3_put_file_fixed -- [old|generic|direct] [--iterations N]"
                );
                return ExitCode::FAILURE;
            }
        }
    }

    let fixture = match Fixture::new() {
        Ok(fixture) => fixture,
        Err(error) => {
            eprintln!("fixture setup failed: {error}");
            return ExitCode::FAILURE;
        }
    };

    let start = Instant::now();
    for iteration in 0..iterations {
        let key = match fixture.object_key(mode, iteration) {
            Ok(key) => key,
            Err(error) => {
                eprintln!("object key failed: {error}");
                return ExitCode::FAILURE;
            }
        };
        let result = match mode {
            "old" => old_finalize_s3(&fixture.store, &fixture.source_path, &key),
            "generic" => generic_finalize_s3(&fixture.store, &fixture.source_path, &key),
            "direct" => direct_finalize_s3(&fixture.store, &fixture.source_path, &key),
            invalid => {
                eprintln!("unsupported mode: {invalid}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(error) = result {
            eprintln!("{mode} finalize failed: {error}");
            return ExitCode::FAILURE;
        }
    }
    let elapsed = start.elapsed();
    let elapsed_ms = elapsed.as_millis();
    let elapsed_micros_remainder = elapsed.subsec_micros() % 1000;
    println!(
        "mode={mode} iterations={iterations} file_bytes={FILE_BYTES} elapsed_ms={elapsed_ms}.{elapsed_micros_remainder:03}"
    );
    ExitCode::SUCCESS
}

fn old_finalize_s3(
    store: &S3ObjectStore,
    path: &Path,
    key: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = fs::read(path)?;
    let _sha256_hex = hex::encode(Sha256::digest(&bytes));
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
        u64::try_from(bytes.len())?,
    );
    let _stored = store.put_if_absent(key, ObjectBody::from_vec(bytes), &integrity)?;
    Ok(())
}

fn generic_finalize_s3(
    store: &S3ObjectStore,
    path: &Path,
    key: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_sha256_hex, integrity) = stream_file_integrity(path)?;
    let _stored = store.put_file_if_absent(key, path, &integrity)?;
    Ok(())
}

fn direct_finalize_s3(
    store: &S3ObjectStore,
    path: &Path,
    key: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_sha256_hex, integrity) = stream_file_integrity(path)?;
    let _stored = store.put_content_addressed_file(key, path, &integrity)?;
    Ok(())
}

fn stream_file_integrity(
    path: &Path,
) -> Result<(String, ObjectIntegrity), Box<dyn std::error::Error>> {
    let mut file = fs::File::open(path)?;
    let mut sha256 = Sha256::new();
    let mut blake3 = Blake3Hasher::new();
    let mut buffer = [0_u8; 256 * 1024];
    let mut total_length = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let slice = buffer
            .get(..read)
            .ok_or("integrity buffer read exceeded allocated capacity")?;
        sha256.update(slice);
        blake3.update(slice);
        total_length = total_length
            .checked_add(u64::try_from(read)?)
            .ok_or("overflow while summing file bytes")?;
    }
    let sha256_hex = hex::encode(sha256.finalize());
    let blake3_hash = ShardlineHash::from_bytes(*blake3.finalize().as_bytes());
    Ok((sha256_hex, ObjectIntegrity::new(blake3_hash, total_length)))
}
