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

    def receive(self, connection, kind, payload):
        remote_id = connection.peer_id
        if kind == Kind.BITFIELD:
            if connection.received_messages:
                raise ProtocolError("BITFIELD must be the first message")
            connection.pieces = decode_bitfield(payload, self.config.piece_count)
            self.known_pieces[remote_id] = set(connection.pieces)
            self.update_interest(connection)
        elif kind == Kind.HAVE:
            index = piece_index(payload, self.config.piece_count)
            connection.pieces.add(index)
            self.known_pieces[remote_id].add(index)
            self.log.write("received the 'have' message from %s for the piece %s." % (remote_id, index))
            self.update_interest(connection)
        elif kind == Kind.INTERESTED:
            connection.remote_interested = True
            self.log.write("received the 'interested' message from %s." % remote_id)
        elif kind == Kind.NOT_INTERESTED:
            connection.remote_interested = False
            self.log.write("received the 'not interested' message from %s." % remote_id)
            if remote_id in self.preferred:
                self.preferred.discard(remote_id)
                self.log_preferred()
            if self.optimistic == remote_id:
                self.optimistic = None
            self.apply_choking()
        elif kind == Kind.CHOKE:
            connection.download_choked = True
            self.log.write("is choked by %s." % remote_id)
            # The sender may have dropped our unanswered request. Free it so
            # another unchoking neighbor can provide the same piece.
            self.release_request(connection)
        elif kind == Kind.UNCHOKE:
            connection.download_choked = False
            self.log.write("is unchoked by %s." % remote_id)
        elif kind == Kind.REQUEST:
            index = piece_index(payload, self.config.piece_count)
            if not connection.upload_choked and index in self.store.pieces:
                connection.send(Kind.PIECE, index_payload(index) + self.store.read(index))
        elif kind == Kind.PIECE:
            self.receive_piece(connection, payload)
        connection.received_messages += 1
        self.request_available()
        self.check_complete()

    def receive_piece(self, connection, payload):
        index = piece_index(payload, self.config.piece_count)
        content = payload[4:]
        if len(content) != self.config.piece_length(index):
            raise ProtocolError("PIECE data does not match the configured piece length")
        if index not in connection.requested:
            raise ProtocolError("Received a piece that was never requested")
        connection.requested.discard(index)
        if connection.pending == index:
            self.release_request(connection)
        # A late response following a choke is valid, but duplicates do not
        # increase the piece count or the measured useful downloading rate.
        if not self.store.write(index, content):
            return
        connection.downloaded_bytes += len(content)
        self.log.write("has downloaded the piece %s from %s. Now the number of pieces it has is %s." %
                       (index, connection.peer_id, len(self.store.pieces)))
        for neighbor in self.connections.values():
            neighbor.send(Kind.HAVE, index_payload(index))
            self.update_interest(neighbor)
        if self.store.complete:
            self.log.write("has downloaded the complete file.")

    def update_interest(self, connection):
        interested = bool(connection.pieces - self.store.pieces)
        if interested != connection.interested:
            connection.interested = interested
            connection.send(Kind.INTERESTED if interested else Kind.NOT_INTERESTED)

    def release_request(self, connection):
        index = connection.pending
        if index is not None and self.reservations.get(index) == connection.peer_id:
            self.reservations.pop(index)
        connection.pending = None

    def request_available(self):
        for connection in self.connections.values():
            if (connection.download_choked or connection.pending is not None or
                    connection.writer.is_closing()):
                continue
            choices = connection.pieces - self.store.pieces - self.reservations.keys()
            if not choices:
                continue
            index = self.rng.choice(sorted(choices))
            self.reservations[index] = connection.peer_id
            connection.pending = index
            connection.requested.add(index)
            connection.requested_at = asyncio.get_running_loop().time()
            connection.send(Kind.REQUEST, index_payload(index))

    def choose_preferred(self):
        candidates = [c for c in self.connections.values()
                      if c.remote_interested and not c.writer.is_closing()]
        self.rng.shuffle(candidates)
        if not self.store.complete:
            # All byte counters cover the same p-second interval. Ranking bytes
            # therefore gives exactly the same ordering as ranking bytes/second.
            candidates.sort(key=lambda c: c.downloaded_bytes, reverse=True)
        selected = {c.peer_id for c in candidates[:self.config.preferred_neighbors]}
        self.preferred = selected
        self.log_preferred()
        for connection in self.connections.values():
            connection.downloaded_bytes = 0
        self.apply_choking()

    def log_preferred(self):
        neighbors = ",".join(str(peer_id) for peer_id in sorted(self.preferred)) or "none"
        self.log.write("has the preferred neighbors %s." % neighbors)

    def choose_optimistic(self):
        candidates = [c.peer_id for c in self.connections.values()
                      if c.remote_interested and c.upload_choked and not c.writer.is_closing()]
        previous = self.optimistic
        if candidates:
            self.optimistic = self.rng.choice(candidates)
        else:
            current = self.connections.get(previous)
            if current is None or not current.remote_interested or current.writer.is_closing():
                self.optimistic = None
        if self.optimistic != previous and self.optimistic is not None:
            self.log.write("has the optimistically unchoked neighbor %s." % self.optimistic)
        self.apply_choking()

    def apply_choking(self):
        allowed = self.preferred | ({self.optimistic} if self.optimistic is not None else set())
        for connection in self.connections.values():
            choked = connection.peer_id not in allowed or not connection.remote_interested
            if choked != connection.upload_choked:
                connection.upload_choked = choked
                connection.send(Kind.CHOKE if choked else Kind.UNCHOKE)

    def expire_requests(self):
        now = asyncio.get_running_loop().time()
        for connection in list(self.connections.values()):
            if connection.pending is not None and now - connection.requested_at > self.request_timeout:
                self.log.write("timed out waiting for piece %s from %s." %
                               (connection.pending, connection.peer_id))
                self.release_request(connection)
                # Reconnect instead of issuing a second outstanding request on
                # a stream whose sender has stopped answering the first one.
                connection.download_choked = True
                connection.writer.close()
        self.request_available()

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
