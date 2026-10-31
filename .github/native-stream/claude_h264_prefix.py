"""Strict independently decodable H264 evidence from bytes read before a deadline.

ffprobe supplies packet order/ranges. No timestamps or MOV metadata supply timing.
"""
import hashlib
from pathlib import Path
import re
import subprocess


def digest(data):
    return hashlib.sha256(data).hexdigest()


def extradata_bytes(stream):
    dump = stream.get("extradata")
    size = stream.get("extradata_size")
    if not isinstance(dump, str) or type(size) is not int or size <= 0:
        raise ValueError("Explicit ffprobe encoder extradata is required")
    result = bytearray()
    for line in dump.strip().splitlines():
        match = re.fullmatch(r"([0-9a-fA-F]{8}): (.*)", line)
        if not match or int(match[1], 16) != len(result):
            raise ValueError("Malformed ffprobe extradata offset")
        hex_part = match[2].split("  ", 1)[0].strip()
        if not re.fullmatch(r"(?:[0-9a-fA-F]{2,4})(?: [0-9a-fA-F]{2,4})*", hex_part):
            raise ValueError("Malformed ffprobe extradata bytes")
        try:
            result.extend(bytes.fromhex(hex_part))
        except ValueError:
            raise ValueError("Malformed ffprobe extradata bytes") from None
    if len(result) != size:
        raise ValueError("Extradata size differs from its bytes")
    return bytes(result)


def avcc_parameter_sets(config):
    if len(config) < 7 or config[0] != 1 or config[4] & 0xFC != 0xFC or config[5] & 0xE0 != 0xE0:
        raise ValueError("Invalid AVC decoder configuration")
    length_size = (config[4] & 3) + 1
    if length_size == 3:
        raise ValueError("Reserved NAL length size")
    offset = 6
    nals = []

    def take_sets(count, nal_type):
        nonlocal offset
        for _ in range(count):
            if offset + 2 > len(config):
                raise ValueError("Truncated parameter-set length")
            size = int.from_bytes(config[offset:offset + 2], "big")
            offset += 2
            if size <= 0 or offset + size > len(config):
                raise ValueError("Truncated or empty parameter set")
            nal = config[offset:offset + size]
            offset += size
            if nal[0] & 0x80 or nal[0] & 31 != nal_type:
                raise ValueError("Unexpected parameter-set NAL type")
            nals.append(nal)

    sps_count = config[5] & 31
    if not sps_count:
        raise ValueError("SPS is required")
    take_sets(sps_count, 7)
    if offset >= len(config) or not config[offset]:
        raise ValueError("PPS is required")
    pps_count = config[offset]
    offset += 1
    take_sets(pps_count, 8)
    if offset < len(config):
        if config[1] not in {100, 110, 122, 144, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135}:
            raise ValueError("Unexpected AVC configuration extension")
        if offset + 4 > len(config) or config[offset] & 0xFC != 0xFC or config[offset + 1] & 0xF8 != 0xF8 or config[offset + 2] & 0xF8 != 0xF8:
            raise ValueError("Malformed AVC configuration extension")
        count = config[offset + 3]
        offset += 4
        take_sets(count, 13)
    if offset != len(config):
        raise ValueError("Trailing AVC configuration bytes")
    return length_size, nals


def single_h264_stream(probe):
    streams = probe.get("streams")
    if not isinstance(streams, list) or len(streams) != 1:
        raise ValueError("Exactly one track is required")
    stream = streams[0]
    if stream.get("codec_type") != "video" or stream.get("codec_name") != "h264" or type(stream.get("index")) is not int or stream["index"] < 0:
        raise ValueError("Exactly one indexed H264 video track is required")
    return stream


def packet_nals(packet, length_size):
    offset = 0
    nals = []
    while offset < len(packet):
        if offset + length_size > len(packet):
            raise ValueError("Incomplete packet NAL length")
        size = int.from_bytes(packet[offset:offset + length_size], "big")
        offset += length_size
        if size <= 0 or offset + size > len(packet):
            raise ValueError("Incomplete or empty packet NAL")
        nal = packet[offset:offset + size]
        offset += size
        if nal[0] & 0x80 or not 1 <= (nal[0] & 31) <= 23:
            raise ValueError("Invalid AVC packet NAL header")
        nals.append(nal)
    if not nals:
        raise ValueError("Empty AVC packet")
    return nals


def build_prefix(baseline_probe, final_probe, final_bytes, snapshot, *, emitted_ns, read_complete_ns):
    """Return Annex B built exclusively from trusted pre-emission config and snapshot bytes."""
    if type(emitted_ns) is not int or type(read_complete_ns) is not int or not 0 <= read_complete_ns - emitted_ns <= 2_000_000_000:
        raise ValueError("Snapshot is outside the explicit host-clock window")
    baseline_config = extradata_bytes(single_h264_stream(baseline_probe))
    stream = single_h264_stream(final_probe)
    if extradata_bytes(stream) != baseline_config:
        raise ValueError("Encoder configuration differs from the pre-emission baseline")
    length_size, parameters = avcc_parameter_sets(baseline_config)
    packets = final_probe.get("packets")
    if not isinstance(packets, list) or not packets:
        raise ValueError("Nonempty complete ffprobe packet inventory is required")
    ranges = []
    for packet in packets:
        if packet.get("stream_index") != stream["index"] or packet.get("codec_type") != "video" or packet.get("side_data_list"):
            raise ValueError("Unexpected track or packet side data")
        values = [packet.get("pos"), packet.get("size")]
        if any(not (type(value) is int or isinstance(value, str) and re.fullmatch(r"[0-9]+", value)) for value in values):
            raise ValueError("Explicit integral packet byte range is required")
        start, size = map(int, values)
        if start < 0 or size <= 0 or start + size > len(final_bytes):
            raise ValueError("Packet range is outside the final recording")
        ranges.append((start, start + size))
    ordered = sorted(ranges)
    if any(previous[1] > following[0] for previous, following in zip(ordered, ordered[1:])):
        raise ValueError("Packet ranges overlap")
    annexb = bytearray(b"".join(b"\x00\x00\x00\x01" + nal for nal in parameters))
    matched = []
    for index, (start, end) in enumerate(ranges):
        if end > len(snapshot) or snapshot[start:end] != final_bytes[start:end]:
            break
        # The payload source is the actual early snapshot, never later final bytes.
        payload = snapshot[start:end]
        nals = packet_nals(payload, length_size)
        annexb.extend(b"".join(b"\x00\x00\x00\x01" + nal for nal in nals))
        matched.append({"packetIndex": index, "start": start, "end": end, "sha256": digest(payload)})
    if not matched:
        raise ValueError("No complete contiguous packet prefix exists in the snapshot")
    return bytes(annexb), {"emittedNs": emitted_ns, "readCompleteNs": read_complete_ns,
        "snapshotSha256": digest(snapshot), "baselineConfigSha256": digest(baseline_config),
        "prefixSha256": digest(annexb), "packetRanges": matched, "totalPacketCount": len(ranges)}


def decode_prefix(annexb, output_dir, ffmpeg="ffmpeg"):
    """Decode only the reconstructed stream; any decoder error rejects this evidence."""
    output_dir = Path(output_dir)
    output_dir.mkdir()  # Unique directory prevents stale frames from becoming evidence.
    source = output_dir / "prefix.h264"
    source.write_bytes(annexb)
    result = subprocess.run([ffmpeg, "-v", "error", "-xerror", "-err_detect", "explode",
        "-f", "h264", "-i", str(source), "-fps_mode", "passthrough", str(output_dir / "frame-%06d.png")],
        capture_output=True, check=True, timeout=60)
    if result.stderr.strip():
        raise ValueError("Independent H264 decoder reported errors: " + result.stderr.decode(errors="replace"))
    frames = sorted(output_dir.glob("frame-*.png"))
    if not frames:
        raise ValueError("Independent prefix decoded no complete frames")
    return [{"path": str(frame), "sha256": digest(frame.read_bytes())} for frame in frames]
