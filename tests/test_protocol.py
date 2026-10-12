import asyncio
import struct
import unittest

from p2p.config import CommonConfig
from p2p.protocol import (
    HEADER, Kind, ProtocolError, decode_bitfield, encode_bitfield, frame,
    handshake, index_payload, piece_index, read_frame, read_handshake,
)


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = CommonConfig(2, 1, 2, "test.dat", 35, 4)

    def reader(self, data):
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return reader

    async def test_exact_handshake_bytes(self):
        encoded = handshake(1001)
        self.assertEqual(encoded, HEADER + bytes(10) + b"\x00\x00\x03\xe9")
        self.assertEqual(len(encoded), 32)
        self.assertEqual(await read_handshake(self.reader(encoded)), 1001)

    async def test_fragmented_stream(self):
        reader = asyncio.StreamReader()
        data = handshake(1002) + frame(Kind.REQUEST, index_payload(7))

        async def feed():
            for byte in data:
                reader.feed_data(bytes([byte]))
                await asyncio.sleep(0)
            reader.feed_eof()

        task = asyncio.create_task(feed())
        self.assertEqual(await read_handshake(reader), 1002)
        self.assertEqual(await read_frame(reader, self.config), (Kind.REQUEST, index_payload(7)))
        await task

    async def test_coalesced_messages_and_every_opcode(self):
        messages = [(Kind(i), b"") for i in range(4)] + [
            (Kind.HAVE, index_payload(8)), (Kind.BITFIELD, b"\x81\x80"),
            (Kind.REQUEST, index_payload(0)), (Kind.PIECE, index_payload(8) + b"end"),
        ]
        reader = self.reader(b"".join(frame(kind, payload) for kind, payload in messages))
        for message in messages:
            self.assertEqual(await read_frame(reader, self.config), message)

    def test_length_excludes_its_own_four_bytes(self):
        self.assertEqual(frame(Kind.HAVE, index_payload(8)), b"\x00\x00\x00\x05\x04\x00\x00\x00\x08")

    def test_bitfield_high_bit_first_and_padding(self):
        self.assertEqual(encode_bitfield({0, 7, 8}, 9), b"\x81\x80")
        self.assertEqual(decode_bitfield(b"\x81\x80", 9), {0, 7, 8})
        for bad in (b"\x81\x81", b"\x81", b"\x81\x80\x00"):
            with self.assertRaises(ProtocolError):
                decode_bitfield(bad, 9)

    async def test_rejects_bad_handshakes(self):
        for bad in (b"X" + handshake(1001)[1:], HEADER + b"x" + bytes(9) + index_payload(1001)):
            with self.assertRaises(ProtocolError):
                await read_handshake(self.reader(bad))

    async def test_rejects_malformed_lengths_and_types(self):
        bad = [struct.pack("!I", 0), struct.pack("!I", 0xFFFFFFFF),
               frame(99), frame(Kind.CHOKE, b"x"), frame(Kind.HAVE, b"x"),
               frame(Kind.REQUEST), frame(Kind.BITFIELD, b"x"),
               frame(Kind.PIECE, index_payload(0))]
        for data in bad:
            with self.subTest(data=data), self.assertRaises(ProtocolError):
                await read_frame(self.reader(data), self.config)

    async def test_truncated_frame_does_not_hang(self):
        with self.assertRaises(asyncio.IncompleteReadError):
            await read_frame(self.reader(frame(Kind.REQUEST, index_payload(1))[:-1]), self.config)

    def test_piece_boundaries(self):
        self.assertEqual(self.config.piece_count, 9)
        self.assertEqual(self.config.piece_length(8), 3)
        with self.assertRaises(ProtocolError):
            piece_index(index_payload(9), 9)
