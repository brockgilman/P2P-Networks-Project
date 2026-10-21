"""single-threaded asynch peer: state changes never cross an await

each connection has a reader task. scheduling, piece reservations, and file writes
run on the same event loop, so two neighbors cannot reserve the same missing piece.
"""

import asyncio
import random
import socket
from dataclasses import dataclass, field

from .eventlog import EventLog
from .protocol import (
    Kind, ProtocolError, decode_bitfield, encode_bitfield, frame, handshake,
    index_payload, piece_index, read_frame, read_handshake,
)
from .storage import PieceStore


@dataclass(eq=False)
class Connection:
    peer_id: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    pieces: set = field(default_factory=set)
    remote_interested: bool = False
    interested: object = None
    upload_choked: bool = True
    download_choked: bool = True
    pending: object = None
    requested_at: float = 0
    requested: set = field(default_factory=set)
    downloaded_bytes: int = 0
    received_messages: int = 0

    def send(self, kind, payload=b""):
        # StreamWriter preserves message ordering, including the initial bitfield
        self.writer.write(frame(kind, payload))


class Peer:
    def __init__(self, root, peer_id, config, peers, rng=None):
        self.root, self.config, self.peers = root, config, peers
        self.by_id = {peer.peer_id: peer for peer in peers}
        if peer_id not in self.by_id:
            raise ValueError("Peer ID %s is not in PeerInfo.cfg" % peer_id)
        self.info = self.by_id[peer_id]
        self.order = {peer.peer_id: index for index, peer in enumerate(peers)}
        self.store = PieceStore(root, self.info, config)
        try:
            self.log = EventLog(root, peer_id)
        except OSError:
            self.store.close()
            raise
        self.rng = rng or random.Random()
        self.connections = {}
        # Do not infer remote completion merely from the initial seed flags.
        # A peer must announce its pieces before it counts as finished.
        self.known_pieces = {peer.peer_id: set() for peer in peers if peer.peer_id != peer_id}
        self.reservations = {}
        self.preferred = set()
        self.optimistic = None
        self.tasks = set()
        self.writers = set()
        self.server = None
        self.done = asyncio.Event()
        self.stopping = False
        self.failure = None
        self.request_timeout = max(30, config.unchoking_interval * 3)

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.task_finished)
        return task

    def task_finished(self, task):
        self.tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            self.fail(task.exception())

    def fail(self, error):
        if self.failure is None:
            self.failure = error
            self.log.write("encountered a fatal error: %s." % error)
        self.done.set()

    def check_complete(self):
        if self.store.complete and all(len(pieces) == self.config.piece_count
                                       for pieces in self.known_pieces.values()):
            self.done.set()

    async def periodic(self, interval, callback):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + interval
        while not self.stopping and not self.done.is_set():
            await asyncio.sleep(max(0, deadline - loop.time()))
            if not self.done.is_set():
                callback()
            deadline += interval

    async def close(self):
        self.stopping = True
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        # Drain first: canceling reader tasks immediately could discard final
        # HAVE messages in their paired writers during simultaneous shutdown.
        async def drain(writer):
            try:
                await asyncio.wait_for(writer.drain(), 3)
            except (OSError, asyncio.TimeoutError):
                pass
        await asyncio.gather(*(drain(w) for w in list(self.writers)))
        for writer in list(self.writers):
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.store.close()
        self.log.close()
