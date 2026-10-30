"""Measure actual native pixels, independently of Maestro's idle waits.

The fixture and this process run on the same CI host. A screenshot's completion
clock is a conservative visibility bound; OCR time is recorded but never
subtracted from any observation. Every matching screenshot remains evidence.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--platform", choices=["android", "ios"], required=True)
parser.add_argument("--device", default="booted")
parser.add_argument("--output", default="artifacts/native-session/pixel-latency")
args = parser.parse_args()
output = Path(args.output)
output.mkdir(parents=True, exist_ok=True)
control = os.environ["CONTROL_URL"].rstrip("/")
frames = []
results = []


def request(path, **query):
    suffix = "?" + urllib.parse.urlencode(query) if query else ""
    with urllib.request.urlopen(control + path + suffix, timeout=5) as response:
        return json.load(response)


def capture(name):
    path = output / (name + ".png")
    began = time.monotonic_ns()
    if args.platform == "android":
        command = ["adb"]
        if args.device != "booted":
            command += ["-s", args.device]
        with path.open("wb") as image:
            subprocess.run(command + ["exec-out", "screencap", "-p"], stdout=image,
                           check=True, timeout=10)
    else:
        subprocess.run(["xcrun", "simctl", "io", args.device, "screenshot", str(path)],
                       capture_output=True, check=True, timeout=10)
    captured = time.monotonic_ns()
    row = {"path": str(path), "captureStartNs": began, "captureCompleteNs": captured,
           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    frames.append(row)
    (output / "frames.json").write_text(json.dumps(frames, indent=2))
    return row


def recognize(frame):
    if args.platform == "android":
        command = ["tesseract", frame["path"], "stdout", "--psm", "6", "-l", "eng"]
    else:
        command = [os.environ["CLAUDE_SCREEN_OCR"], frame["path"]]
    frame["text"] = subprocess.run(command, capture_output=True, text=True, check=True,
                                  timeout=15).stdout
    frame["ocrCompleteNs"] = time.monotonic_ns()
    (output / "frames.json").write_text(json.dumps(frames, indent=2))
    return frame


def contains(frame, marker):
    # Preserve exact letters/digits; underscores and OCR whitespace are presentation.
    normalize = lambda value: re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalize(marker) in normalize(frame["text"])


baseline = recognize(capture("baseline-ready"))
if not contains(baseline, "NATIVE_CLAUDE_READY"):
    raise RuntimeError("The actual native screenshot does not show the open stdout capture")
if any(contains(baseline, "NATIVE_CLAUDE_LATENCY_" + str(stage)) for stage in range(1, 4)):
    raise RuntimeError("Latency marker was already visible before this measurement")

for stage in range(1, 4):
    request("/advance", stage=stage)
    # Capture first, recognize later: slow OCR must not create a blind interval
    # while native pixels are changing. The post-budget frames also show misses.
    deadline = time.monotonic_ns() + 3_000_000_000
    burst = []
    while time.monotonic_ns() < deadline:
        burst.append(capture(f"sample-{stage}-{len(burst) + 1:02d}"))
    found = None
    for frame in burst:
        recognize(frame)
        if contains(frame, "NATIVE_CLAUDE_LATENCY_" + str(stage)):
            found = frame
            break
    if found is None:
        raise RuntimeError(f"Stage {stage}: output never appeared in native screenshot pixels")
    result = request("/visible", stage=stage, capturedNs=found["captureCompleteNs"])
    result.update(stage=stage, screenshot=found["path"], sha256=found["sha256"],
                  measurement="native screenshot completion, runner-host monotonic clock")
    results.append(result)
    (output / "results.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(result), flush=True)
    if not result["passed"]:
        raise RuntimeError(f"Stage {stage}: native pixels exceeded 2000 ms; no overhead was subtracted")

request("/complete")
deadline = time.monotonic_ns() + 20_000_000_000
attempt = 0
while time.monotonic_ns() < deadline:
    attempt += 1
    frame = recognize(capture(f"completed-{attempt:02d}"))
    if contains(frame, "NATIVE_CLAUDE_DONE"):
        break
else:
    raise RuntimeError("Native UI did not show the final result after the foreground command completed")
