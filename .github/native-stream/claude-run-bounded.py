"""Bound native simulator preparation without changing measured latency budgets."""

import os
import signal
import subprocess
import sys
import time


timeout = float(sys.argv[1])
label = sys.argv[2]
started = time.monotonic()
print(f"[native preparation] {label}: starting (timeout {timeout:g}s)", flush=True, file=sys.stderr)
process = subprocess.Popen(sys.argv[3:], start_new_session=True)
try:
    status = process.wait(timeout=timeout)
except subprocess.TimeoutExpired:
    # This group belongs to the process spawned above, never another simulator or server.
    os.killpg(process.pid, signal.SIGKILL)
    process.wait()
    print(f"[native preparation] {label}: timed out after {timeout:g}s", flush=True, file=sys.stderr)
    sys.exit(124)
print(
    f"[native preparation] {label}: exit {status} after {time.monotonic() - started:.1f}s",
    flush=True, file=sys.stderr,
)
sys.exit(status)
