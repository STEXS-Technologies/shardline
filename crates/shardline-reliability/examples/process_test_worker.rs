//! Child-process fixture for the runtime contract suite, not a maintenance worker.
#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use penelope::core::task::{TaskGeneration, TaskScope};
    use shardline_reliability::ProcessWorker;
    use std::{env, path::PathBuf, thread, time::Duration};

    let mut args = env::args().skip(1);
    if args.next().as_deref() != Some("maintenance-worker") {
        return Err("invalid entrypoint".into());
    }
    if args.next().as_deref() != Some("--journal") {
        return Err("missing journal".into());
    }
    let path = PathBuf::from(args.next().ok_or("missing journal path")?);
    if args.next().as_deref() != Some("--scope") {
        return Err("missing scope".into());
    }
    let scope: TaskScope =
        serde_json::from_slice(&hex::decode(args.next().ok_or("missing scope value")?)?)?;
    if args.next().as_deref() != Some("--generation") {
        return Err("missing generation".into());
    }
    let generation = TaskGeneration::new(args.next().ok_or("missing generation value")?.parse()?);
    let mut worker = ProcessWorker::claim(&path, scope, generation)?;
    let mode = worker.input().to_vec();
    match mode.as_slice() {
        b"complete" => worker.complete(b"output", 1)?,
        b"checkpoint" => {
            worker.checkpoint(b"checkpoint_1", 1)?;
            worker.complete(b"output", 1)?;
        }
        b"hold" => loop {
            thread::sleep(Duration::from_millis(10));
        },
        b"cpu" => loop {
            std::hint::black_box(std::hint::black_box(7_u64).wrapping_mul(11));
        },
        b"limit" => {
            let limit = rustix::process::getrlimit(rustix::process::Resource::Cpu);
            worker.complete(&serde_json::to_vec(&(limit.current, limit.maximum))?, 1)?;
        }
        _ => return Err("unknown fixture mode".into()),
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn main() {}
