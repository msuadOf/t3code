import importlib.util
from pathlib import Path
import struct
import unittest
import zlib

spec = importlib.util.spec_from_file_location("android_screencap", Path(__file__).with_name("android-screencap.py"))
screencap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(screencap)


class RawScreencapTests(unittest.TestCase):
    def test_rgba_legacy_and_colorspace_headers_preserve_exact_pixels(self):
        pixels = b"\x12\x34\x56\xff\xab\xcd\xef\xff"
        for colorspace in (None, 0, 1, 2):
            raw = struct.pack("<III", 2, 1, 1) + (b"" if colorspace is None else struct.pack("<I", colorspace)) + pixels
            metadata, decoded = screencap.decode_raw(raw)
            self.assertEqual(decoded, pixels)
            self.assertEqual(metadata["headerBytes"], 12 if colorspace is None else 16)
            self.assertEqual(metadata["colorspace"], colorspace)
            png = screencap.encode_png(metadata, decoded)
            self.assertTrue(png.startswith(b"\x89PNG\r\n\x1a\n"))
            cursor = 8
            while cursor < len(png):
                size = struct.unpack_from(">I", png, cursor)[0]
                kind = png[cursor + 4:cursor + 8]
                content = png[cursor + 8:cursor + 8 + size]
                self.assertEqual(struct.unpack_from(">I", png, cursor + 8 + size)[0], zlib.crc32(kind + content))
                if kind == b"IDAT": self.assertEqual(zlib.decompress(content), b"\x00" + pixels)
                cursor += 12 + size

    def test_fixed_api_header_rejects_modern_header_with_four_missing_pixel_bytes(self):
        raw = struct.pack("<IIII", 2, 1, 1, 1) + b"\x12\x34\x56\xff" * 2
        self.assertEqual(screencap.decode_raw(raw, expected_header=16)[0]["headerBytes"], 16)
        with self.assertRaises(ValueError): screencap.decode_raw(raw[:-4], expected_header=16)

    def test_rgbx_discards_only_padding_and_rgb_is_unchanged(self):
        meta, pixels = screencap.decode_raw(struct.pack("<IIII", 1, 1, 2, 1) + b"\x12\x34\x56\x00")
        self.assertEqual(pixels, b"\x12\x34\x56")
        self.assertEqual(meta["channels"], 3)
        _, pixels = screencap.decode_raw(struct.pack("<III", 1, 1, 3) + b"\x12\x34\x56")
        self.assertEqual(pixels, b"\x12\x34\x56")

    def test_rejects_unknown_or_truncated_headers_pixels_and_dimensions(self):
        invalid = [b"short", struct.pack("<III", 0, 1, 1), struct.pack("<III", 8193, 1, 1),
                   struct.pack("<III", 1, 1, 99) + b"\x00" * 4,
                   struct.pack("<IIII", 1, 1, 1, 99) + b"\x00" * 4,
                   struct.pack("<III", 2, 2, 1) + b"\x00" * 15,
                   struct.pack("<III", 2, 2, 1) + b"\x00" * 21]
        for raw in invalid:
            with self.assertRaises(ValueError): screencap.decode_raw(raw)


if __name__ == "__main__": unittest.main()
