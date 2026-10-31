import copy
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("prefix", Path(__file__).with_name("claude_h264_prefix.py"))
prefix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prefix)

CONFIG = bytes.fromhex('01640020ffe1001127640020ac56502f02a7a9a6a04040204001000428ee3cb0fdf8f800')


def hex_dump(data):
    return '\n'.join(f'{index:08x}: ' + data[index:index + 16].hex(' ') + '  text' for index in range(0, len(data), 16))


def stream(config=CONFIG):
    return {'index': 0, 'codec_type': 'video', 'codec_name': 'h264',
            'extradata': hex_dump(config), 'extradata_size': len(config)}


def packet(data):
    return len(data).to_bytes(4, 'big') + data


class PrefixTests(unittest.TestCase):
    def setUp(self):
        self.payloads = [packet(b'\x65first'), packet(b'\x41second'), packet(b'\x01third')]
        self.final = b'header' + b''.join(self.payloads)
        self.probe = {'streams': [stream()], 'packets': []}
        pos = 6
        for payload in self.payloads:
            self.probe['packets'].append({'codec_type': 'video', 'stream_index': 0, 'pos': str(pos), 'size': str(len(payload))})
            pos += len(payload)

    def build(self, snapshot=None, probe=None):
        return prefix.build_prefix({'streams': [stream()]}, probe or self.probe, self.final,
            self.final if snapshot is None else snapshot, emitted_ns=100, read_complete_ns=200)

    def test_baseline_real_simctl_high_profile_configuration(self):
        self.assertEqual(prefix.extradata_bytes(stream()), CONFIG)
        length_size, parameters = prefix.avcc_parameter_sets(CONFIG)
        self.assertEqual(length_size, 4)
        self.assertEqual([nal[0] & 31 for nal in parameters], [7, 8])

    def test_first_missing_packet_stops_prefix_without_skipping_to_later_packets(self):
        changed = bytearray(self.final)
        changed[6 + len(self.payloads[0])] ^= 1
        annexb, evidence = self.build(bytes(changed))
        self.assertEqual(len(evidence['packetRanges']), 1)
        self.assertTrue(annexb.endswith(b'\x00\x00\x00\x01\x65first'))
        self.assertNotIn(b'third', annexb)
        self.assertEqual(evidence['readCompleteNs'], 200)
        self.assertEqual(evidence['prefixSha256'], prefix.digest(annexb))
        self.assertEqual(evidence['snapshotSha256'], prefix.digest(changed))

    def test_incomplete_packet_is_not_reconstructed_from_final_bytes(self):
        annexb, evidence = self.build(self.final[:-1])
        self.assertEqual(len(evidence['packetRanges']), 2)
        self.assertNotIn(b'third', annexb)

    def test_container_bytes_are_not_decoder_input(self):
        changed = b'XXXXXX' + self.final[6:]
        self.assertEqual(self.build(changed)[0], self.build()[0])

    def test_invalid_ranges_tracks_and_encoder_config_fail_closed(self):
        for kind in ['overlap', 'negative', 'fraction', 'boolean', 'missing', 'outofbounds', 'empty', 'track', 'side_data', 'config', 'multiple', 'codec']:
            with self.subTest(kind=kind):
                probe = copy.deepcopy(self.probe)
                if kind == 'overlap': probe['packets'][1]['pos'] = '7'
                if kind == 'negative': probe['packets'][0]['pos'] = '-1'
                if kind == 'fraction': probe['packets'][0]['size'] = 1.5
                if kind == 'boolean': probe['packets'][0]['size'] = True
                if kind == 'missing': del probe['packets'][0]['size']
                if kind == 'outofbounds': probe['packets'][0]['size'] = '999'
                if kind == 'empty': probe['packets'] = []
                if kind == 'track': probe['packets'][0]['stream_index'] = 2
                if kind == 'side_data': probe['packets'][0]['side_data_list'] = [{'side_data_type': 'extra'}]
                if kind == 'config': probe['streams'][0]['extradata'] = hex_dump(CONFIG[:-1])
                if kind == 'multiple': probe['streams'].append(stream())
                if kind == 'codec': probe['streams'][0]['codec_name'] = 'hevc'
                with self.assertRaises(ValueError): self.build(probe=probe)

    def test_invalid_avcc_and_packet_nals_fail_closed(self):
        for config in [b'', CONFIG[:-1], CONFIG + b'junk', CONFIG[:4] + b'\xfe' + CONFIG[5:], CONFIG[:5] + b'\xe0' + CONFIG[6:]]:
            with self.subTest(config=config), self.assertRaises(ValueError):
                prefix.avcc_parameter_sets(config)
        for data in [b'', b'\x00', b'\x00\x00\x00\x00', packet(b'\x80bad'), b'\x00\x00\x00\x05a']:
            with self.subTest(data=data), self.assertRaises(ValueError):
                prefix.packet_nals(data, 4)

    def test_snapshot_deadline_and_absent_first_packet_reject(self):
        with self.assertRaises(ValueError): self.build(b'')
        for time in [99, 2_000_000_101]:
            with self.assertRaises(ValueError):
                prefix.build_prefix({'streams': [stream()]}, self.probe, self.final, self.final,
                    emitted_ns=100, read_complete_ns=time)

    def test_decoder_errors_and_existing_output_directory_reject(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'decode'
            with patch.object(prefix.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, stderr=b'decode error')):
                with self.assertRaises(ValueError): prefix.decode_prefix(b'coded', target)
            with self.assertRaises(FileExistsError): prefix.decode_prefix(b'coded', target)
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(prefix.subprocess, 'run', side_effect=subprocess.CalledProcessError(1, 'ffmpeg')):
                with self.assertRaises(subprocess.CalledProcessError): prefix.decode_prefix(b'coded', Path(directory) / 'decode')

    @unittest.skipUnless(shutil.which('ffmpeg') and shutil.which('ffprobe'), 'Real codec proof runs where ffmpeg/ffprobe are installed (iOS CI).')
    def test_real_b_frames_decode_identical_to_final_movie(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            movie = root / 'fixture.mp4'
            subprocess.run(['ffmpeg', '-v', 'error', '-f', 'lavfi', '-i', 'testsrc2=size=64x64:rate=10',
                '-frames:v', '12', '-c:v', 'libx264', '-bf', '2', '-g', '12', str(movie)], check=True)
            probe = json.loads(subprocess.check_output(['ffprobe', '-v', 'error', '-show_streams', '-show_packets', '-show_data', '-of', 'json', str(movie)]))
            data = movie.read_bytes()
            # Omit the last decode-order packet: preceding B frames may still be delayed.
            last = probe['packets'][-1]
            snapshot = data[:int(last['pos'])]
            annexb, evidence = prefix.build_prefix({'streams': probe['streams']}, probe, data, snapshot,
                emitted_ns=100, read_complete_ns=200)
            self.assertEqual(len(evidence['packetRanges']), len(probe['packets']) - 1)
            frames = prefix.decode_prefix(annexb, root / 'early')
            final = root / 'final'
            final.mkdir()
            subprocess.run(['ffmpeg', '-v', 'error', '-i', str(movie), '-fps_mode', 'passthrough', str(final / 'frame-%06d.png')], check=True)
            hashes = {prefix.digest(path.read_bytes()) for path in final.glob('*.png')}
            self.assertTrue(frames)
            self.assertTrue(all(frame['sha256'] in hashes for frame in frames))


if __name__ == '__main__':
    unittest.main()
