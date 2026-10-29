"""Bound owned fixture commands and keep credentials out of public errors."""

import os
import signal
import subprocess
import tempfile


def command(*args, **kwargs):
    """Capture output privately and stop owned descendants on every exit."""
    timeout = kwargs.pop("timeout", 120)
    # Nested commands can share an outer command's owned group so terminating
    # the outer worker also terminates these children. The outer owner remains
    # responsible for group cleanup, including when this inner wait is killed.
    new_session = kwargs.pop("start_new_session", os.name == "posix")
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        output = kwargs.pop("stdout", stdout)
        error_output = kwargs.pop("stderr", stderr)
        process = subprocess.Popen(args, stdout=output, stderr=error_output,
                                   start_new_session=new_session, **kwargs)
        try:
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise TimeoutError("fixture command exceeded deadline") from None
        finally:
            if os.name == "posix" and new_session:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif process.poll() is None:
                process.kill()
            process.wait()
        if status:
            raise subprocess.CalledProcessError(status, "fixture command")
        if output is not stdout:
            return ""
        stdout.seek(0)
        return stdout.read().decode().strip()
