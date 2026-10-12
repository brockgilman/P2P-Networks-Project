"""keep file bytes on disk and advertise a piece only after it is written"""

import os
from pathlib import Path


class PieceStore:
    def __init__(self, root, peer, config):
        self.config = config
        self.directory = Path(root) / ("peer_%s" % peer.peer_id)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / config.file_name
        self.pieces = set()
        self.file = None
        if peer.has_file:
            if not self.path.is_file() or self.path.stat().st_size != config.file_size:
                raise ValueError("Seed file %s must exist and contain exactly %s bytes" %
                                 (self.path, config.file_size))
            self.file = self.path.open("rb", buffering=0)
            self.pieces.update(range(config.piece_count))
        else:
            # starts non-seeds empty; it does not specify resuming
            self.path = self.directory / (config.file_name + ".part")
            self.file = self.path.open("w+b", buffering=0)
            self.file.truncate(config.file_size)

    @property
    def complete(self):
        return len(self.pieces) == self.config.piece_count

    def read(self, index):
        length = self.config.piece_length(index)
        if index not in self.pieces:
            raise ValueError("Cannot upload a piece that is not available")
        self.file.seek(index * self.config.piece_size)
        data = self.file.read(length)
        if len(data) != length:
            raise OSError("Unexpected short read from the shared file")
        return data

    def write(self, index, data):
        if len(data) != self.config.piece_length(index):
            raise ValueError("Incorrect piece size")
        if index in self.pieces:
            return False
        self.file.seek(index * self.config.piece_size)
        view = memoryview(data)
        while view:
            written = self.file.write(view)
            if not written:
                raise OSError("Could not write the complete piece")
            view = view[written:]
        self.pieces.add(index)
        if self.complete:
            self.file.flush()
            os.fsync(self.file.fileno())
            self.file.close()
            destination = self.directory / self.config.file_name
            os.replace(self.path, destination)
            self.path = destination
            # finished downloaders keep serving until the whole swarm completes
            self.file = self.path.open("rb", buffering=0)
        return True

    def close(self):
        if self.file is not None:
            self.file.close()

