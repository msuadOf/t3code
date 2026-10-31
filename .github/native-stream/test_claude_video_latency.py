import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("video_latency", Path(__file__).with_name("claude-video-latency.py"))
video_latency = importlib.util.module_from_spec(spec)
spec.loader.exec_module(video_latency)


class RecordingEvidenceTests(unittest.TestCase):
    def probe(self):
        return {"streams": [{"index": 0, "codec_type": "video", "codec_name": "h264",
                              "extradata": "fixed-avcC", "extradata_size": 10}],
                "packets": [{"stream_index": 0, "codec_type": "video", "pos": "2", "size": "3"},
                            {"stream_index": 0, "codec_type": "video", "pos": "7", "size": "2"}]}

    def test_every_packet_must_exist_unchanged_in_immutable_snapshot(self):
        final = b'00abc00de00'
        ranges = video_latency.packet_ranges(self.probe(), len(final), 'fixed-avcC')
        self.assertTrue(video_latency.snapshot_contains_packets(final[:9], final, ranges))
        self.assertFalse(video_latency.snapshot_contains_packets(final[:8], final, ranges))
        self.assertFalse(video_latency.snapshot_contains_packets(b'00abc00dX00', final, ranges))
        # Container headers may be finalized later; only identical encoded packet bytes count.
        self.assertTrue(video_latency.snapshot_contains_packets(b'XXabcXXde', final, ranges))

    def test_track_configuration_and_packet_omissions_fail_closed(self):
        for kind in ['config', 'empty_config', 'multiple_tracks', 'non_h264', 'empty_packets', 'missing_range', 'bad_range', 'wrong_track']:
            with self.subTest(kind=kind):
                probe = self.probe()
                if kind == 'config': probe['streams'][0]['extradata'] = 'different'
                if kind == 'empty_config': probe['streams'][0]['extradata'] = ''
                if kind == 'multiple_tracks': probe['streams'].append(dict(probe['streams'][0]))
                if kind == 'non_h264': probe['streams'][0]['codec_name'] = 'hevc'
                if kind == 'empty_packets': probe['packets'] = []
                if kind == 'missing_range': del probe['packets'][0]['pos']
                if kind == 'bad_range': probe['packets'][0]['size'] = '100'
                if kind == 'wrong_track': probe['packets'][0]['stream_index'] = 1
                with self.assertRaises(ValueError):
                    video_latency.packet_ranges(probe, 11, 'fixed-avcC')

    def test_snapshot_time_is_after_complete_read_and_out_of_window_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'live.mov'
            source.write_bytes(b'immutable-packet-bytes')
            target = Path(directory) / 'snapshot.bin'
            with patch.object(video_latency.time, 'monotonic_ns', side_effect=[101, 110]):
                row = video_latency.read_snapshot(source, 100, 120, target)
            self.assertEqual(row['readStartNs'], 101)
            self.assertEqual(row['readCompleteNs'], 110)
            source.write_bytes(b'mutated-after-reading')
            self.assertEqual(target.read_bytes(), b'immutable-packet-bytes')
            target.unlink()
            with patch.object(video_latency.time, 'monotonic_ns', side_effect=[101, 121]):
                self.assertIsNone(video_latency.read_snapshot(source, 100, 120, target))
            self.assertFalse(target.exists())
            with patch.object(video_latency.time, 'monotonic_ns', return_value=99):
                self.assertIsNone(video_latency.read_snapshot(source, 100, 120, target))

    def test_recording_file_cannot_be_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / 'existing.mov'
            video.write_bytes(b'old recording')
            with self.assertRaises(ValueError):
                video_latency.record_clip([], video, time.monotonic_ns)

    def test_bound_requires_marker_and_inclusive_nonnegative_two_second_limit(self):
        self.assertTrue(video_latency.valid_bound(10, 2_000_000_010, True))
        self.assertFalse(video_latency.valid_bound(10, 2_000_000_011, True))
        self.assertFalse(video_latency.valid_bound(10, 9, True))
        self.assertFalse(video_latency.valid_bound(10, 20, False))

    def test_normal_finalization_precedes_evidence_and_gates_after_first_frame_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            video = Path(directory) / "recording.mov"
            command = [sys.executable, "-u", "-c", "import pathlib,signal,sys,time; pathlib.Path(sys.argv[1]).write_bytes(b'fixture-video'); signal.signal(signal.SIGINT,lambda *_:sys.exit(0)); print('Recording started',file=sys.stderr,flush=True); time.sleep(30)", str(video)]
            def delayed_emission():
                time.sleep(.08)
                return time.monotonic_ns()
            metadata = video_latency.record_clip(command, video, delayed_emission, duration=.05, snapshots=True)
            self.assertLessEqual(metadata['recordingStartedNs'], metadata['gateRequestNs'])
            self.assertLessEqual(metadata['gateRequestNs'], metadata['sigintNs'])
            self.assertLessEqual(metadata['sigintNs'], metadata['finalizedNs'])
            self.assertEqual(metadata['exitCode'], 0)
            self.assertEqual(metadata['videoSha256'], video_latency.sha(video))
            self.assertGreaterEqual(metadata['sigintNs'] - metadata['emittedNs'], 50_000_000)
            self.assertGreaterEqual(metadata['emittedNs'] - metadata['gateRequestNs'], 80_000_000)
            self.assertTrue(metadata['snapshots'])
            self.assertTrue(all(metadata['emittedNs'] <= sample['readCompleteNs'] <= metadata['emittedNs'] + 1_950_000_000 for sample in metadata['snapshots']))

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
                video_latency.record_clip(command, video, time.monotonic_ns, duration=.01, finalize_timeout=.1)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pidfile.read_text()), 0)
            metadata = json.loads(video.with_suffix('.recording.json').read_text())
            self.assertNotIn('finalizedNs', metadata)

    def test_emission_before_gate_or_in_future_is_rejected(self):
        for emitted in [0, time.monotonic_ns() + 60_000_000_000]:
            with self.subTest(emitted=emitted), tempfile.TemporaryDirectory() as directory:
                video = Path(directory) / 'recording.mov'
                command = [sys.executable, '-u', '-c', 'import sys,time; print("Recording started",file=sys.stderr,flush=True);time.sleep(60)']
                with self.assertRaises(ValueError):
                    video_latency.record_clip(command, video, lambda: emitted)


if __name__ == '__main__':
    unittest.main()
