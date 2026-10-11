"""Binary wire format. All integer fields use unsigned network byte order."""

import struct
from enum import IntEnum


HEADER = b"P2PFILESHARINGPROJ"
UINT32 = struct.Struct("!I")
HANDSHAKE = struct.Struct("!18s10sI")


class ProtocolError(ValueError):
    """A remote peer sent a frame that does not follow the specification."""


class Kind(IntEnum):
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7


def handshake(peer_id):
    return HANDSHAKE.pack(HEADER, bytes(10), peer_id)


async def read_handshake(reader):
    header, reserved, peer_id = HANDSHAKE.unpack(await reader.readexactly(32))
    if header != HEADER or reserved != bytes(10):
        raise ProtocolError("Invalid handshake header or reserved bytes")
    return peer_id


def frame(kind, payload=b""):
    return UINT32.pack(1 + len(payload)) + bytes([kind]) + payload


def index_payload(index):
    return UINT32.pack(index)


def piece_index(payload, piece_count):
    index = UINT32.unpack(payload[:4])[0]
    if index >= piece_count:
        raise ProtocolError("Piece index out of range: %s" % index)
    return index


def encode_bitfield(pieces, piece_count):
    data = bytearray((piece_count + 7) // 8)
    for index in pieces:
        if not 0 <= index < piece_count:
            raise ValueError("Piece index out of range")
        data[index // 8] |= 1 << (7 - index % 8)
    return bytes(data)


def decode_bitfield(payload, piece_count):
    if len(payload) != (piece_count + 7) // 8:
        raise ProtocolError("Wrong bitfield length")
    spare = (-piece_count) % 8
    if spare and payload[-1] & ((1 << spare) - 1):
        raise ProtocolError("Nonzero spare bitfield bits")
    return {index for index in range(piece_count)
            if payload[index // 8] & (1 << (7 - index % 8))}


