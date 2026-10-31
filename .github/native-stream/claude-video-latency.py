"""Accept iOS pixels using recording finalization as a conservative host-clock bound."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import subprocess
import threading
import time
import urllib.parse
import urllib.request


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def contains(text, marker):
    normalize = lambda value: re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalize(marker) in normalize(text)


def valid_bound(emitted_ns, finalized_ns, marker_found):
    return marker_found and 0 <= finalized_ns - emitted_ns <= 2_000_000_000


def record_clip(command, video, release_gate, duration=.8, startup_timeout=60, finalize_timeout=10):
    signals = queue.Queue()
    metadata = {"recordingSpawnNs": time.monotonic_ns()}
    stderr_path = video.with_suffix(".stderr.log")
    with stderr_path.open("w") as log:
        process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        def read_stderr():
            for line in process.stderr:
                log.write(line)
                log.flush()
                if "Recording started" in line:
                    signals.put(time.monotonic_ns())
        reader = threading.Thread(target=read_stderr, daemon=True)
        reader.start()
        try:
            deadline = time.monotonic() + startup_timeout
            while True:
                try:
                    metadata["recordingStartedNs"] = signals.get(timeout=.05)
                    break
                except queue.Empty:
                    if process.poll() is not None:
                        raise RuntimeError("Recorder exited before its first frame")
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Recorder did not report its first frame")
            metadata["gateRequestNs"] = time.monotonic_ns()
            release_gate()
            remaining = metadata["gateRequestNs"] / 1e9 + duration - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            metadata["sigintNs"] = time.monotonic_ns()
            process.send_signal(signal.SIGINT)
            metadata["exitCode"] = process.wait(timeout=finalize_timeout)
            metadata["finalizedNs"] = time.monotonic_ns()
            if metadata["exitCode"] != 0:
                raise RuntimeError("Recorder did not finalize normally")
            reader.join(timeout=2)
            metadata.update(video=str(video), videoSha256=sha(video), stderr=str(stderr_path))
            return metadata
        finally:
            if process.poll() is None:
                # Only this invocation's captured PID/process group belongs to this harness.
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            reader.join(timeout=2)
            process.stderr.close()
            metadata.setdefault("cleanupCompleteNs", time.monotonic_ns())
            video.with_suffix(".recording.json").write_text(json.dumps(metadata, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--device", required=True)
    parser.add_argument("--output", default="artifacts/native-session/pixel-latency")
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    control = os.environ["CONTROL_URL"].rstrip("/")
    results = []
    def request(path, **query):
        suffix = "?" + urllib.parse.urlencode(query) if query else ""
        with urllib.request.urlopen(control + path + suffix, timeout=5) as response:
            return json.load(response)
    def recognize(path):
        text = subprocess.run([os.environ["CLAUDE_SCREEN_OCR"], str(path)],
                              capture_output=True, text=True, check=True, timeout=60).stdout
        path.with_suffix(".ocr.txt").write_text(text)
        return text
    def screenshot(name):
        path = output / (name + ".bmp")
        subprocess.run(["xcrun", "simctl", "io", args.device, "screenshot", "--type=bmp", str(path)],
                       capture_output=True, check=True, timeout=60)
        return path
    baseline = screenshot("baseline-ready")
    text = recognize(baseline)
    if not contains(text, "NATIVE_CLAUDE_READY") or any(
        contains(text, "NATIVE_CLAUDE_LATENCY_" + str(stage)) for stage in range(1, 4)
    ):
        raise RuntimeError("Baseline pixels must show READY without any measurement marker")
    (output / "baseline.json").write_text(json.dumps({"path": str(baseline), "sha256": sha(baseline)}))
    for stage in range(1, 4):
        video = output / f"stage-{stage}.mov"
        metadata = record_clip(["xcrun", "simctl", "io", args.device, "recordVideo", "--codec=h264", str(video)],
                               video, lambda: request("/advance", stage=stage))
        frame_dir = output / f"stage-{stage}-frames"
        frame_dir.mkdir()
        # Decode original frames in order, without fps resampling or a timestamp-based claim.
        subprocess.run(["ffmpeg", "-v", "error", "-i", str(video), "-fps_mode", "passthrough",
                        str(frame_dir / "frame-%06d.png")], check=True, timeout=60)
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_frames", "-select_streams", "v:0",
                                "-of", "json", str(video)], capture_output=True, text=True, check=True, timeout=60)
        (frame_dir / "original-frames.json").write_text(probe.stdout)
        frames = sorted(frame_dir.glob("frame-*.png"))
        selected = frames[::6]
        if frames and frames[-1] not in selected:
            selected.append(frames[-1])
        found = next((frame for frame in selected if contains(recognize(frame), f"NATIVE_CLAUDE_LATENCY_{stage}")), None)
        if found is None:
            raise RuntimeError(f"Stage {stage}: no recording frame shows its unique marker")
        result = request("/visible", stage=stage, capturedNs=metadata["finalizedNs"])
        result.update(stage=stage, screenshot=str(found), sha256=sha(found), **metadata,
                      measurement="recording finalized normally, runner-host monotonic upper bound; frame PTS is not timing evidence")
        result["passed"] = result["passed"] and valid_bound(result["emittedNs"], metadata["finalizedNs"], True)
        results.append(result)
        (output / "results.json").write_text(json.dumps(results, indent=2))
        print(json.dumps(result), flush=True)
        if not result["passed"]:
            raise RuntimeError(f"Stage {stage}: recording finalization exceeded 2000 ms; no overhead was subtracted")
    request("/complete")
    deadline = time.monotonic() + 30
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        if contains(recognize(screenshot(f"completed-{attempt:02d}")), "NATIVE_CLAUDE_DONE"):
            return
    raise RuntimeError("Native pixels did not show DONE after foreground command completion")


if __name__ == "__main__":
    main()
