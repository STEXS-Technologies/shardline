use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Instant,
};

use blake3::Hasher as Blake3Hasher;
use sha2::{Digest, Sha256};
use shardline_protocol::ShardlineHash;
use shardline_storage::{LocalObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};
use tempfile::TempDir;

const FILE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_ITERATIONS: usize = 8;

struct Fixture {
    _root: TempDir,
    source_file: PathBuf,
    stage_dir: PathBuf,
    object_store: LocalObjectStore,
}

impl Fixture {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let source_file = root.path().join("source.bin");
        let stage_dir = root.path().join("oci-uploads");
        let store_root = root.path().join("chunks");
        fs::create_dir_all(&stage_dir)?;
        let payload = vec![0x5a; FILE_BYTES];
        fs::write(&source_file, payload)?;
        let object_store = LocalObjectStore::new(store_root)?;
        Ok(Self {
            _root: root,
            source_file,
            stage_dir,
            object_store,
        })
    }

    fn stage_path(&self, iteration: usize) -> PathBuf {
        self.stage_dir.join(format!("upload-{iteration}.bin"))
    }

    fn object_key(&self, iteration: usize) -> Result<ObjectKey, Box<dyn std::error::Error>> {
        Ok(ObjectKey::parse(&format!(
            "protocols/oci/global/repos/bench/blobs/{iteration:08x}"
        ))?)
    }

    fn reset_stage_file(&self, iteration: usize) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let stage = self.stage_path(iteration);
        let _ignored = fs::remove_file(&stage);
        fs::copy(&self.source_file, &stage)?;
        Ok(stage)
    }
}

fn main() -> ExitCode {
    let mut mode = "new";
    let mut iterations = DEFAULT_ITERATIONS;
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "old" | "new" => mode = Box::leak(argument.into_boxed_str()),
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
                    "usage: cargo bench --bench oci_finalize_fixed -- [old|new] [--iterations N]"
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
        let stage = match fixture.reset_stage_file(iteration) {
            Ok(stage) => stage,
            Err(error) => {
                eprintln!("stage reset failed: {error}");
                return ExitCode::FAILURE;
            }
        };
        let key = match fixture.object_key(iteration) {
            Ok(key) => key,
            Err(error) => {
                eprintln!("object key failed: {error}");
                return ExitCode::FAILURE;
            }
        };
        let result = match mode {
            "old" => old_finalize(&fixture.object_store, &stage, &key),
            "new" => new_finalize(&fixture.object_store, &stage, &key),
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

fn old_finalize(
    store: &LocalObjectStore,
    stage: &Path,
    key: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = fs::read(stage)?;
    let _sha256 = hex::encode(Sha256::digest(&bytes));
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
        u64::try_from(bytes.len())?,
    );
    let _stored = store.put_if_absent(key, ObjectBody::from_vec(bytes), &integrity)?;
    Ok(())
}

fn new_finalize(
    store: &LocalObjectStore,
    stage: &Path,
    key: &ObjectKey,
) -> Result<(), Box<dyn std::error::Error>> {
    let (_sha256, integrity) = stream_file_integrity(stage)?;
    let _stored = store.put_temporary_file_if_absent(key, stage, &integrity)?;
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
