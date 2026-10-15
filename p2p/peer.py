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


