"""Stop an owned Maestro process when its JVM main thread has already failed."""
import os
import queue
import signal
import subprocess
import sys
import threading
import time


def run(command, timeout=1200):
    lines = queue.Queue()
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, start_new_session=True)
    def read_output():
        for line in process.stdout:
            lines.put(line)
        lines.put(None)
    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()
    deadline = time.monotonic() + timeout
    try:
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError('Maestro preparation exceeded its bounded setup budget')
            try:
                line = lines.get(timeout=.1)
            except queue.Empty:
                if process.poll() is not None:
                    return process.returncode
                continue
            if line is None:
                return process.wait(timeout=5)
            print(line, end='', flush=True)
            if 'Exception in thread "main"' in line:
                print('[native preparation] Maestro JVM main thread failed; stopping its owned process group', flush=True)
                return 1
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        reader.join(timeout=2)
        process.stdout.close()


if __name__ == '__main__':
    sys.exit(run(sys.argv[1:]))
