use penelope_core::task::{TaskTick, runtime::TaskPortError};
use rustix::{
    process::{Signal, getpid, kill_process},
    time::{
        Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec, timerfd_create,
        timerfd_settime,
    },
};

/// The absolute kernel timer and dedicated thread live in the worker process,
/// independently of controller lifetime. No checkpoint or application poll is
/// needed to terminate blocked or non-cooperative work at its deadline.
pub(super) fn arm(deadline: TaskTick) -> Result<(), TaskPortError> {
    // Compatibility-mode work retains the host's existing limits. Its managed
    // worker is tied to controller lifetime rather than an added wall deadline.
    if deadline.get() == u64::MAX {
        return Ok(());
    }
    let timer = timerfd_create(TimerfdClockId::Boottime, TimerfdFlags::CLOEXEC)
        .map_err(|_error| TaskPortError::Runtime)?;
    let stamp = Timespec {
        tv_sec: (deadline.get() / 1000)
            .try_into()
            .map_err(|_error| TaskPortError::Runtime)?,
        tv_nsec: (deadline.get() % 1000)
            .checked_mul(1_000_000)
            .ok_or(TaskPortError::Runtime)?
            .try_into()
            .map_err(|_error| TaskPortError::Runtime)?,
    };
    timerfd_settime(
        &timer,
        TimerfdTimerFlags::ABSTIME,
        &Itimerspec {
            it_interval: Timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            it_value: stamp,
        },
    )
    .map_err(|_error| TaskPortError::Runtime)?;
    std::thread::Builder::new()
        .name("task-deadline".to_owned())
        .spawn(move || {
            let mut count = [0; 8];
            loop {
                match rustix::io::read(&timer, &mut count) {
                    Err(rustix::io::Errno::INTR) => continue,
                    Ok(_) | Err(_) => {
                        let _sent = kill_process(getpid(), Signal::KILL);
                        break;
                    }
                }
            }
        })
        .map_err(super::identity::io_error)?;
    Ok(())
}
