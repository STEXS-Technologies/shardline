"""Bound owned fixture commands and keep credentials out of public errors."""

import os
import signal
import subprocess
import tempfile


def command(*args, **kwargs):
    """Capture output privately and stop owned descendants on every exit."""
    timeout = kwargs.pop("timeout", 120)
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        output = kwargs.pop("stdout", stdout)
        error_output = kwargs.pop("stderr", stderr)
        process = subprocess.Popen(args, stdout=output, stderr=error_output,
                                   start_new_session=os.name == "posix", **kwargs)
        try:
            try:
                status = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                raise TimeoutError("fixture command exceeded deadline") from None
        finally:
            if os.name == "posix":
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
