"""Strict Android raw screencap -> lossless PNG for offline OCR.

AOSP writes uint32 width, height, PixelFormat, then optional colorspace (0/1/2),
followed by tightly packed rows (the buffer stride is not serialized):
https://github.com/aosp-mirror/platform_frameworks_base/blob/main/cmds/screencap/screencap.cpp
Only the explicitly decoded 8-bit formats below are accepted.
"""
import struct
import zlib


def decode_raw(data, expected_header=None):
    if len(data) < 12:
        raise ValueError("Truncated Android screencap header")
    width, height, pixel_format = struct.unpack_from("<III", data)
    if not 1 <= width <= 8192 or not 1 <= height <= 8192 or width * height > 40_000_000:
        raise ValueError("Unsupported Android screencap dimensions")
    bytes_per_pixel = {1: 4, 2: 4, 3: 3}.get(pixel_format)
    if bytes_per_pixel is None:
        raise ValueError(f"Unsupported Android PixelFormat: {pixel_format}")
    expected = width * height * bytes_per_pixel
    header = len(data) - expected
    if expected_header is not None and header != expected_header:
        raise ValueError("Android screencap does not match the known API-level header length")
    if header == 12:
        colorspace = None
    elif header == 16:
        colorspace = struct.unpack_from("<I", data, 12)[0]
        if colorspace not in (0, 1, 2):
            raise ValueError("Unsupported Android screencap colorspace")
    else:
        raise ValueError("Android screencap length does not match its header and packed pixels")
    pixels = data[header:]
    if pixel_format == 2:  # RGBX: discard the undefined padding byte, not image content.
        pixels = bytes(value for offset, value in enumerate(pixels) if offset % 4 != 3)
    channels = 4 if pixel_format == 1 else 3
    return {"width": width, "height": height, "pixelFormat": pixel_format,
            "headerBytes": header, "colorspace": colorspace, "channels": channels}, pixels


def encode_png(metadata, pixels):
    width, height, channels = metadata["width"], metadata["height"], metadata["channels"]
    if len(pixels) != width * height * channels:
        raise ValueError("Decoded pixel length changed before PNG encoding")
    def chunk(kind, content):
        return struct.pack(">I", len(content)) + kind + content + struct.pack(">I", zlib.crc32(kind + content))
    stride = width * channels
    rows = b"".join(b"\x00" + pixels[row * stride:(row + 1) * stride] for row in range(height))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8,
            6 if channels == 4 else 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(rows, 1)) + chunk(b"IEND", b""))
