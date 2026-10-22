import asyncio
from pathlib import Path
import random
import tempfile
import unittest

from p2p.config import CommonConfig, PeerInfo
from p2p.peer import Connection, Peer
from p2p.protocol import Kind, ProtocolError, encode_bitfield, index_payload


class MemoryWriter:
    def __init__(self):
        self.messages = []
        self.closing = False

    def write(self, data):
        self.messages.append((Kind(data[4]), data[5:]))

    def is_closing(self):
        return self.closing

    def close(self):
        self.closing = True


class PeerStateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = CommonConfig(2, 1, 2, "file.dat", 10, 4)
        peers = [PeerInfo(i, "localhost", 20000 + i, i == 2) for i in range(1, 6)]
        self.peer = Peer(self.root, 1, self.config, peers, random.Random(7))
        self.addCleanup(self.peer.log.close)
        self.addCleanup(self.peer.store.close)
        self.connections = []
        for info in peers[1:]:
            connection = Connection(info.peer_id, None, MemoryWriter())
            self.peer.connections[info.peer_id] = connection
            self.connections.append(connection)

    def fill_store(self):
        for index, data in enumerate((b"abcd", b"efgh", b"ij")):
            self.peer.store.write(index, data)

    async def test_random_requests_are_unique_and_not_pipelined(self):
        for connection in self.connections:
            connection.pieces = {0, 1, 2}
            connection.download_choked = False
        self.peer.request_available()
        self.assertEqual(len(self.peer.reservations), 3)
        self.assertEqual(sum(c.pending is not None for c in self.connections), 3)
        before = [len(c.writer.messages) for c in self.connections]
        self.peer.request_available()
        self.assertEqual(before, [len(c.writer.messages) for c in self.connections])

    async def test_choke_releases_piece_for_another_neighbor(self):
        first, second = self.connections[:2]
        first.pieces = second.pieces = {0}
        first.download_choked = second.download_choked = False
        self.peer.request_available()
        self.assertEqual(first.pending, 0)
        self.assertIsNone(second.pending)
        self.peer.receive(first, Kind.CHOKE, b"")
        self.assertIsNone(first.pending)
        self.assertEqual(second.pending, 0)
        self.assertEqual(self.peer.reservations, {0: second.peer_id})

    async def test_late_piece_and_duplicate_do_not_double_count(self):
        first, second = self.connections[:2]
        first.pieces = second.pieces = {0}
        first.download_choked = second.download_choked = False
        self.peer.request_available()
        self.peer.receive(first, Kind.CHOKE, b"")
        self.peer.receive(first, Kind.PIECE, index_payload(0) + b"abcd")
        self.assertEqual(self.peer.store.pieces, {0})
        self.peer.receive(second, Kind.PIECE, index_payload(0) + b"abcd")
        self.assertEqual(self.peer.store.pieces, {0})
        self.assertEqual(first.downloaded_bytes, 4)
        self.assertEqual(second.downloaded_bytes, 0)
        self.assertEqual(self.peer.reservations, {})

    async def test_interest_changes_as_pieces_arrive(self):
        connection = self.connections[0]
        self.peer.receive(connection, Kind.BITFIELD, encode_bitfield({0}, 3))
        self.assertIn((Kind.INTERESTED, b""), connection.writer.messages)
        connection.requested.add(0)
        self.peer.receive(connection, Kind.PIECE, index_payload(0) + b"abcd")
        self.assertIn((Kind.NOT_INTERESTED, b""), connection.writer.messages)
        self.assertTrue(all((Kind.HAVE, index_payload(0)) in c.writer.messages for c in self.connections))

    async def test_rate_selection_excludes_uninterested_and_resets_counters(self):
        for connection, rate in zip(self.connections, (5, 20, 10, 1000)):
            connection.downloaded_bytes = rate
            connection.remote_interested = connection is not self.connections[-1]
        self.peer.choose_preferred()
        self.assertEqual(self.peer.preferred, {3, 4})
        self.assertEqual([c.downloaded_bytes for c in self.connections], [0] * 4)
        self.assertEqual(sum(not c.upload_choked for c in self.connections), 2)

    async def test_ties_are_broken_randomly(self):
        for connection in self.connections:
            connection.remote_interested = True
        choices = set()
        for _ in range(20):
            self.peer.choose_preferred()
            choices.add(tuple(sorted(self.peer.preferred)))
        self.assertGreater(len(choices), 1)

    async def test_complete_peer_selects_randomly_without_rate_preference(self):
        self.fill_store()
        for connection in self.connections:
            connection.remote_interested = True
        excluded_high_rate = False
        for _ in range(20):
            self.connections[0].downloaded_bytes = 1000000
            self.peer.choose_preferred()
            excluded_high_rate |= self.connections[0].peer_id not in self.peer.preferred
        self.assertTrue(excluded_high_rate)

    async def test_optimistic_rotation_and_upload_limit(self):
        for connection in self.connections:
            connection.remote_interested = True
        self.peer.choose_preferred()
        self.peer.choose_optimistic()
        first = self.peer.optimistic
        self.assertNotIn(first, self.peer.preferred)
        self.assertEqual(sum(not c.upload_choked for c in self.connections), 3)
        self.peer.choose_optimistic()
        self.assertNotEqual(self.peer.optimistic, first)
        self.assertEqual(sum(not c.upload_choked for c in self.connections), 3)
        self.assertTrue(self.peer.connections[first].upload_choked)

    async def test_optimistic_neighbor_can_become_preferred(self):
        for connection in self.connections:
            connection.remote_interested = True
        self.peer.choose_optimistic()
        optimistic = self.peer.optimistic
        self.peer.connections[optimistic].downloaded_bytes = 1000
        self.peer.choose_preferred()
        self.assertIn(optimistic, self.peer.preferred)
        self.assertFalse(self.peer.connections[optimistic].upload_choked)
        self.peer.choose_optimistic()
        self.assertNotEqual(optimistic, self.peer.optimistic)

    async def test_choked_request_is_ignored(self):
        self.fill_store()
        connection = self.connections[0]
        self.peer.receive(connection, Kind.REQUEST, index_payload(0))
        self.assertEqual(connection.writer.messages, [])
        connection.upload_choked = False
        self.peer.receive(connection, Kind.REQUEST, index_payload(0))
        self.assertEqual(connection.writer.messages, [(Kind.PIECE, index_payload(0) + b"abcd")])

    async def test_rejects_late_bitfield_and_unsolicited_piece(self):
        connection = self.connections[0]
        self.peer.receive(connection, Kind.INTERESTED, b"")
        with self.assertRaises(ProtocolError):
            self.peer.receive(connection, Kind.BITFIELD, b"\x00")
        with self.assertRaises(ProtocolError):
            self.peer.receive(connection, Kind.PIECE, index_payload(0) + b"abcd")

    async def test_rejects_wrong_final_piece_length(self):
        connection = self.connections[0]
        connection.requested.add(2)
        with self.assertRaises(ProtocolError):
            self.peer.receive(connection, Kind.PIECE, index_payload(2) + b"toolong")

    async def test_timed_out_request_is_released_and_connection_closed(self):
        connection = self.connections[0]
        connection.pieces = {0}
        connection.download_choked = False
        self.peer.request_available()
        connection.requested_at = asyncio.get_running_loop().time() - 100
        self.peer.expire_requests()
        self.assertTrue(connection.writer.is_closing())
        self.assertIsNone(connection.pending)
        self.assertFalse(self.peer.reservations)

    async def test_completion_requires_every_remote_bitfield(self):
        self.fill_store()
        self.peer.check_complete()
        self.assertFalse(self.peer.done.is_set())
        for connection in self.connections[:-1]:
            self.peer.receive(connection, Kind.BITFIELD, b"\xe0")
        self.assertFalse(self.peer.done.is_set())
        self.peer.receive(self.connections[-1], Kind.BITFIELD, b"\xe0")
        self.assertTrue(self.peer.done.is_set())
