"""Real TCP processes and independent wire-level clients, using temporary files."""

import asyncio
from pathlib import Path
import random
import socket
import tempfile
import unittest

from p2p.config import CommonConfig, load_peers
from p2p.peer import Peer
from p2p.protocol import Kind, frame, handshake, read_frame, read_handshake
from tools.prepare_demo import prepare
from tools.run_local import run


def free_port_block(count):
    # Keep every probe bound until the entire block has been checked.
    for _ in range(100):
        sockets = []
        try:
            first = socket.socket()
            sockets.append(first)
            # Stay below ephemeral client ports: outbound connections could
            # otherwise claim a later peer's port before that peer is launched.
            base = random.SystemRandom().randrange(10000, 30000 - count)
            first.bind(("0.0.0.0", base))
            for port in range(base + 1, base + count):
                probe = socket.socket()
                sockets.append(probe)
                probe.bind(("0.0.0.0", port))
            return base
        except OSError:
            continue
        finally:
            for probe in sockets:
                probe.close()
    raise RuntimeError("Could not find available local ports")


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "run"

    def test_four_processes_partial_last_piece_and_connection_order(self):
        prepare(self.root, count=4, size=170123, base_port=free_port_block(4),
                preferred=1, interval=1, optimistic=2)
        # Use IDs whose numerical order differs from the configuration order.
        path = self.root / "PeerInfo.cfg"
        path.write_text(path.read_text().replace("1001 ", "9001 "), encoding="utf-8")
        (self.root / "peer_1001").rename(self.root / "peer_9001")
        result = run(self.root, timeout=20)
        self.assertTrue(result["passed"])
        for index, peer_id in enumerate((9001, 1002, 1003, 1004)):
            log = (self.root / ("log_peer_%s.log" % peer_id)).read_text()
            self.assertEqual(log.count("makes a connection to Peer "), index)
            self.assertEqual(log.count("is connected from Peer "), 3 - index)
            self.assertIn("all peers have the complete file", log)
            if index:
                self.assertEqual(log.count("has downloaded the complete file."), 1)
                self.assertEqual(log.count("has downloaded the piece "), 11)

    def test_optimistic_only_transfer(self):
        prepare(self.root, count=3, size=32769, base_port=free_port_block(3),
                preferred=0, interval=1, optimistic=1)
        result = run(self.root, timeout=20)
        self.assertTrue(result["passed"])
        self.assertGreater(sum(row["optimistic_selections"] for row in result["peers"]), 0)

    def test_single_seed_exits_without_connections(self):
        prepare(self.root, count=1, size=10, base_port=free_port_block(1))
        self.assertTrue(run(self.root, timeout=10)["passed"])


class NetworkRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / "run"
        prepare(self.root, count=2, size=32769, base_port=free_port_block(2),
                preferred=1, interval=1, optimistic=2)
        self.config = CommonConfig.load(self.root / "Common.cfg")
        self.infos = load_peers(self.root / "PeerInfo.cfg")
        self.peer = Peer(self.root, 1001, self.config, self.infos)
        self.task = asyncio.create_task(self.peer.run())
        for _ in range(100):
            if self.peer.server is not None:
                break
            if self.task.done():
                await self.task
            await asyncio.sleep(0.01)

    async def asyncTearDown(self):
        if not self.task.done():
            self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.temp.cleanup()

    async def connect(self, data):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.infos[0].port)
        writer.write(data)
        await writer.drain()
        self.assertEqual(await asyncio.wait_for(read_handshake(reader), 3), 1001)
        return reader, writer

    async def test_invalid_handshake_and_frame_do_not_crash_listener(self):
        for data in (handshake(9999), b"X" + handshake(1002)[1:]):
            reader, writer = await self.connect(data)
            self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
            writer.close()
            await writer.wait_closed()
        reader, writer = await self.connect(handshake(1002))
        kind, _ = await read_frame(reader, self.config)
        self.assertEqual(kind, Kind.BITFIELD)
        writer.write(b"\xff\xff\xff\xff")
        await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
        writer.close()
        await writer.wait_closed()
        self.assertFalse(self.task.done())

    async def test_empty_peer_may_skip_bitfield_and_reconnect(self):
        reader, writer = await self.connect(handshake(1002))
        await read_frame(reader, self.config)
        writer.write(frame(Kind.INTERESTED))
        await writer.drain()
        self.assertEqual(await asyncio.wait_for(read_frame(reader, self.config), 3), (Kind.UNCHOKE, b""))
        writer.close()
        await writer.wait_closed()
        for _ in range(100):
            if 1002 not in self.peer.connections:
                break
            await asyncio.sleep(0.01)
        downloader = Peer(self.root, 1002, self.config, self.infos)
        task = asyncio.create_task(downloader.run())
        try:
            await asyncio.wait_for(asyncio.gather(self.task, task), 10)
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual((self.root / "peer_1002" / self.config.file_name).read_bytes(),
                         (self.root / "peer_1001" / self.config.file_name).read_bytes())

    async def test_later_peer_retries_until_earlier_peer_starts(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        downloader = Peer(self.root, 1002, self.config, self.infos)
        download_task = asyncio.create_task(downloader.run())
        try:
            await asyncio.sleep(0.6)
            self.assertFalse(download_task.done())
            self.peer = Peer(self.root, 1001, self.config, self.infos)
            self.task = asyncio.create_task(self.peer.run())
            await asyncio.wait_for(asyncio.gather(self.task, download_task), 10)
        finally:
            if not download_task.done():
                download_task.cancel()
                await asyncio.gather(download_task, return_exceptions=True)
