import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("video_latency", Path(__file__).with_name("claude-video-latency.py"))
video_latency = importlib.util.module_from_spec(spec)
spec.loader.exec_module(video_latency)


class RecordingEvidenceTests(unittest.TestCase):
    def test_bound_requires_marker_and_inclusive_nonnegative_two_second_limit(self):
        self.assertTrue(video_latency.valid_bound(10, 2_000_000_010, True))
        self.assertFalse(video_latency.valid_bound(10, 2_000_000_011, True))
        self.assertFalse(video_latency.valid_bound(10, 9, True))
        self.assertFalse(video_latency.valid_bound(10, 20, False))

    def test_normal_finalization_precedes_evidence_and_gates_after_first_frame_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "recording.mov"
            command = [sys.executable, "-u", "-c", "import pathlib,signal,sys,time; pathlib.Path(sys.argv[1]).write_bytes(b'fixture-video'); signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); print('Recording started',file=sys.stderr,flush=True); time.sleep(30)", str(video)]
            metadata = video_latency.record_clip(command, video, lambda: None, duration=.05)
            self.assertLessEqual(metadata['recordingStartedNs'], metadata['gateRequestNs'])
            self.assertLessEqual(metadata['gateRequestNs'], metadata['sigintNs'])
            self.assertLessEqual(metadata['sigintNs'], metadata['finalizedNs'])
            self.assertEqual(metadata['exitCode'], 0)
            self.assertEqual(metadata['videoSha256'], video_latency.sha(video))

    def test_startup_timeout_kills_only_its_owned_process(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "recording.mov"
            pidfile = Path(directory) / "pid"
            command = [sys.executable, "-u", "-c", "import os,pathlib,sys,time; pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)", str(pidfile)]
            with self.assertRaises(TimeoutError):
                video_latency.record_clip(command, video, lambda: self.fail('Gate must stay closed'), startup_timeout=.2)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)
            self.assertIn('cleanupCompleteNs', json.loads(video.with_suffix('.recording.json').read_text()))

    def test_finalize_timeout_does_not_claim_normal_exit_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "recording.mov"
            pidfile = Path(directory) / "pid"
            command = [sys.executable, "-u", "-c", "import os,pathlib,signal,sys,time; pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); signal.signal(signal.SIGINT,signal.SIG_IGN); print('Recording started',file=sys.stderr,flush=True); time.sleep(30)", str(pidfile)]
            with self.assertRaises(video_latency.subprocess.TimeoutExpired):
                video_latency.record_clip(command, video, lambda: None, duration=.01, finalize_timeout=.1)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)
            metadata = json.loads(video.with_suffix('.recording.json').read_text())
            self.assertNotIn('finalizedNs', metadata)


if __name__ == '__main__':
    unittest.main()
