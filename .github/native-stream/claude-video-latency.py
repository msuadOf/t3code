"""Accept iOS pixels using verified encoded bytes or normal recording finalization."""
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
import claude_h264_prefix as h264_prefix


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def contains(text, marker):
    normalize = lambda value: re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalize(marker) in normalize(text)


def matching_prefix_marker(decoded, final_frames_by_hash, recognize, marker):
    for frame in decoded:
        final = final_frames_by_hash.get(frame["sha256"])
        if final is not None and contains(recognize(Path(frame["path"])), marker):
            return frame, final
    return None


def valid_bound(emitted_ns, finalized_ns, marker_found):
    return marker_found and 0 <= finalized_ns - emitted_ns <= 2_000_000_000


def encoder_config(probe):
    streams = probe.get("streams", [])
    if len(streams) != 1 or streams[0].get("codec_type") != "video" or streams[0].get("codec_name") != "h264":
        raise ValueError("Recording must contain exactly one H264 video track")
    stream = streams[0]
    config = stream.get("extradata")
    if not isinstance(config, str) or not config.strip() or int(stream.get("extradata_size", 0)) <= 0:
        raise ValueError("Recording has no explicit encoder configuration")
    return config


def packet_ranges(probe, file_size, expected_config):
    if encoder_config(probe) != expected_config:
        raise ValueError("Recording encoder configuration differs from the pre-emission baseline")
    packets = probe.get("packets", [])
    if not packets:
        raise ValueError("Recording has no packets")
    ranges = []
    stream_index = probe["streams"][0]["index"]
    for packet in packets:
        if packet.get("stream_index") != stream_index or packet.get("codec_type") != "video":
            raise ValueError("Unexpected packet track")
        try:
            start, size = int(packet["pos"]), int(packet["size"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("Every packet requires an explicit byte range") from None
        if start < 0 or size <= 0 or start + size > file_size:
            raise ValueError("Packet byte range is outside the recording")
        ranges.append((start, start + size))
    return ranges


def snapshot_contains_packets(snapshot, final_bytes, ranges):
    return all(end <= len(snapshot) and snapshot[start:end] == final_bytes[start:end]
               for start, end in ranges)


def read_snapshot(video, emitted_ns, deadline_ns, destination):
    began = time.monotonic_ns()
    if began < emitted_ns or began >= deadline_ns:
        return None
    with video.open("rb") as source:
        size = os.fstat(source.fileno()).st_size
        data = source.read(size)
    completed = time.monotonic_ns()
    if completed > deadline_ns or len(data) != size:
        return None
    destination.write_bytes(data)
    return {"path": str(destination), "readStartNs": began, "readCompleteNs": completed,
            "size": size, "sha256": hashlib.sha256(data).hexdigest()}


def record_clip(command, video, release_gate, duration=1.2, startup_timeout=60, finalize_timeout=10,
                snapshots=False):
    if video.exists():
        raise ValueError("Recording path must not already exist")
    signals = queue.Queue()
    snapshot_stop = threading.Event()
    snapshot_reader = None
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
            emitted = release_gate()
            if type(emitted) is not int or not metadata["gateRequestNs"] <= emitted <= time.monotonic_ns():
                raise ValueError("Emission must follow this gate on the same host monotonic clock")
            metadata["emittedNs"] = emitted
            if snapshots:
                metadata["snapshots"] = []
                def sample_prefixes():
                    deadline_ns = emitted + 1_950_000_000
                    while not snapshot_stop.is_set() and time.monotonic_ns() < deadline_ns:
                        try:
                            sample = read_snapshot(video, emitted, deadline_ns,
                                video.with_name(video.stem + f"-snapshot-{len(metadata['snapshots']):03d}.bin"))
                            if sample is not None:
                                metadata["snapshots"].append(sample)
                        except OSError as error:
                            metadata.setdefault("snapshotErrors", []).append(str(error))
                        snapshot_stop.wait(.1)
                snapshot_reader = threading.Thread(target=sample_prefixes)
                snapshot_reader.start()
            remaining = emitted / 1e9 + duration - time.monotonic()
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
            snapshot_stop.set()
            if snapshot_reader is not None:
                snapshot_reader.join()
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
    ocr_cache = {}
    def request(path, timeout=5, **query):
        suffix = "?" + urllib.parse.urlencode(query) if query else ""
        with urllib.request.urlopen(control + path + suffix, timeout=timeout) as response:
            return json.load(response)
    def recognize(path):
        digest = sha(path)
        if digest not in ocr_cache:
            ocr_cache[digest] = subprocess.run([os.environ["CLAUDE_SCREEN_OCR"], str(path)],
                                capture_output=True, text=True, check=True, timeout=60).stdout
        text = ocr_cache[digest]
        path.with_suffix(".ocr.txt").write_text(text)
        return text
    def screenshot(name):
        path = output / (name + ".bmp")
        subprocess.run(["xcrun", "simctl", "io", args.device, "screenshot", "--type=bmp", str(path)],
                       capture_output=True, check=True, timeout=60)
        return path
    def recording_command(video):
        return ["xcrun", "simctl", "io", args.device, "recordVideo", "--codec=h264", str(video)]
    def probe_video(video):
        probe = subprocess.run(["ffprobe", "-v", "error", "-show_packets", "-show_streams", "-show_data",
                                "-of", "json", str(video)], capture_output=True, text=True, check=True, timeout=60)
        video.with_suffix(".packets.json").write_text(probe.stdout)
        return json.loads(probe.stdout)
    baseline = screenshot("baseline-ready")
    text = recognize(baseline)
    if not contains(text, "NATIVE_CLAUDE_READY") or any(
        contains(text, "NATIVE_CLAUDE_LATENCY_" + str(stage)) for stage in range(1, 4)
    ):
        raise RuntimeError("Baseline pixels must show READY without any measurement marker")
    (output / "baseline.json").write_text(json.dumps({"path": str(baseline), "sha256": sha(baseline)}))
    baseline_video = output / "baseline-encoder.mov"
    record_clip(recording_command(baseline_video), baseline_video, time.monotonic_ns, duration=.2)
    trusted_config = encoder_config(probe_video(baseline_video))
    (output / "baseline-encoder.json").write_text(json.dumps({
        "videoSha256": sha(baseline_video), "extradata": trusted_config,
        "extradataSha256": hashlib.sha256(trusted_config.encode()).hexdigest(),
    }, indent=2))
    recordings = []
    for stage in range(1, 4):
        video = output / f"stage-{stage}.mov"
        def release_gate():
            deadline = time.monotonic() + 5
            request("/advance", stage=stage)
            while time.monotonic() < deadline:
                emitted = request("/timing", timeout=deadline-time.monotonic(), stage=stage).get(str(stage), {}).get("emittedNs")
                if emitted is not None:
                    return emitted
                time.sleep(.02)
            raise TimeoutError("Native command did not acknowledge emission within five seconds")
        metadata = record_clip(recording_command(video), video, release_gate, duration=3, snapshots=True)
        checkpoint = request("/foreground-checkpoint", timeout=40, stage=stage,
                             finalizedNs=metadata["finalizedNs"])["foregroundCheckpoint"]
        metadata["foregroundCheckpoint"] = checkpoint
        video.with_suffix(".recording.json").write_text(json.dumps(metadata, indent=2))
        if not checkpoint["foregroundVerified"] or not checkpoint["stillBlocked"]:
            raise RuntimeError(f"Stage {stage}: foreground checkpoint failed: {checkpoint['nativeEvidence']}")
        recordings.append((stage, video, metadata))
    # Release the unchanged foreground command before CPU-heavy offline analysis.
    request("/complete")
    deadline = time.monotonic() + 30
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        done = screenshot(f"completed-{attempt:02d}")
        if contains(recognize(done), "NATIVE_CLAUDE_DONE"):
            (output / "completion.json").write_text(json.dumps({"path": str(done), "sha256": sha(done)}))
            break
    else:
        raise RuntimeError("Native pixels did not show DONE after foreground command completion")
    for stage, video, metadata in recordings:
        final_bytes = video.read_bytes()
        if hashlib.sha256(final_bytes).hexdigest() != metadata["videoSha256"]:
            raise ValueError("Recording changed after its foreground checkpoint")
        final_probe = probe_video(video)
        ranges = packet_ranges(final_probe, len(final_bytes), trusted_config)
        captured_ns = metadata["finalizedNs"]
        metadata["prefixProofs"] = []
        frame_dir = output / f"stage-{stage}-frames"
        frame_dir.mkdir()
        # Decode original frames in order, without fps resampling or a timestamp-based claim.
        subprocess.run(["ffmpeg", "-v", "error", "-xerror", "-err_detect", "explode", "-i", str(video), "-fps_mode", "passthrough",
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
        final_frames_by_hash = {}
        for frame in frames:
            final_frames_by_hash.setdefault(sha(frame), frame)
        baseline_probe = json.loads(baseline_video.with_suffix(".packets.json").read_text())
        for index, sample in enumerate(sorted(metadata["snapshots"], key=lambda row: row["readCompleteNs"])):
            if not metadata["emittedNs"] <= sample["readStartNs"] <= sample["readCompleteNs"] <= metadata["emittedNs"] + 1_950_000_000:
                raise ValueError("Snapshot was not read completely within its explicit host-clock window")
            snapshot = Path(sample["path"]).read_bytes()
            if sha(Path(sample["path"])) != sample["sha256"]:
                raise ValueError("Immutable snapshot evidence changed after capture")
            evidence = {"snapshot": sample}
            try:
                annexb, proof = h264_prefix.build_prefix(baseline_probe, final_probe, final_bytes, snapshot,
                    emitted_ns=metadata["emittedNs"], read_complete_ns=sample["readCompleteNs"])
                evidence.update(proof)
                decoded = h264_prefix.decode_prefix(annexb, output / f"stage-{stage}-prefix-{index:03d}")
                match = matching_prefix_marker(decoded, final_frames_by_hash, recognize, f"NATIVE_CLAUDE_LATENCY_{stage}")
                if match is not None:
                    frame, final_frame = match
                    evidence["markerFrame"] = frame
                    evidence["identicalFinalFrame"] = str(final_frame)
                    captured_ns = min(captured_ns, sample["readCompleteNs"])
                    found = final_frame
            except (ValueError, subprocess.CalledProcessError) as error:
                evidence["rejected"] = str(error)
            metadata["prefixProofs"].append(evidence)
            if "markerFrame" in evidence:
                break
        metadata["packetProof"] = {"packetCount": len(ranges), "ranges": ranges,
            "capturedNs": captured_ns, "baselineConfigSha256": hashlib.sha256(trusted_config.encode()).hexdigest()}
        video.with_suffix(".recording.json").write_text(json.dumps(metadata, indent=2))
        result = request("/visible", stage=stage, capturedNs=captured_ns)
        if result["emittedNs"] != metadata["emittedNs"]:
            raise ValueError("Fixture emission timestamp changed during measurement")
        result.update(stage=stage, screenshot=str(found), sha256=sha(found), **metadata,
                      measurement="independently decoded immutable snapshot marker exactly matches a final video frame, or normal finalization; host monotonic upper bound, no PTS timing")
        result["passed"] = result["passed"] and valid_bound(result["emittedNs"], captured_ns, True)
        results.append(result)
        (output / "results.json").write_text(json.dumps(results, indent=2))
        print(json.dumps(result), flush=True)
        if not result["foregroundVerified"] or not result["stillBlocked"]:
            raise RuntimeError(f"Stage {stage}: native foreground checkpoint is not valid")
        if not result["passed"]:
            raise RuntimeError(f"Stage {stage}: pixel upper bound {result['latencyMs']} ms exceeded 2000 ms; no overhead was subtracted")


if __name__ == "__main__":
    main()
