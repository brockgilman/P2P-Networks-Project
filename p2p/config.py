"""parse the two assignment configuration files without assuming peer-ID order"""

from dataclasses import dataclass
from pathlib import Path


def rows(path):
    # allow blank lines and comments; retain line numbers for useful errors
    for number, raw in enumerate(Path(path).read_text(encoding="utf-8-sig").splitlines(), 1):
        fields = raw.split("#", 1)[0].split()
        if fields:
            yield number, fields


@dataclass(frozen=True)
class CommonConfig:
    preferred_neighbors: int
    unchoking_interval: int
    optimistic_interval: int
    file_name: str
    file_size: int
    piece_size: int

    @property
    def piece_count(self):
        return (self.file_size + self.piece_size - 1) // self.piece_size

    @property
    def bitfield_size(self):
        return (self.piece_count + 7) // 8

    def piece_length(self, index):
        if not 0 <= index < self.piece_count:
            raise ValueError("Piece index out of range: %s" % index)
        return min(self.piece_size, self.file_size - index * self.piece_size)

    @classmethod
    def load(cls, path):
        expected = {
            "NumberOfPreferredNeighbors", "UnchokingInterval",
            "OptimisticUnchokingInterval", "FileName", "FileSize", "PieceSize",
        }
        values = {}
        for line, fields in rows(path):
            if len(fields) != 2 or fields[0] not in expected:
                raise ValueError("%s:%s: expected a recognized key and value" % (path, line))
            key, value = fields
            if key in values:
                raise ValueError("%s:%s: duplicate key %s" % (path, line, key))
            values[key] = value
        missing = expected - values.keys()
        if missing:
            raise ValueError("Missing Common.cfg keys: " + ", ".join(sorted(missing)))
        config = cls(
            int(values["NumberOfPreferredNeighbors"]), int(values["UnchokingInterval"]),
            int(values["OptimisticUnchokingInterval"]), values["FileName"],
            int(values["FileSize"]), int(values["PieceSize"]),
        )
        if config.preferred_neighbors < 0:
            raise ValueError("NumberOfPreferredNeighbors must be nonnegative")
        if min(config.unchoking_interval, config.optimistic_interval,
               config.file_size, config.piece_size) <= 0:
            raise ValueError("Intervals, FileSize, and PieceSize must be positive")
        if (config.file_name in (".", "..") or
                any(c in config.file_name for c in "/\\:\x00")):
            raise ValueError("FileName must be a single file name")
        if config.piece_size > 0xFFFFFFFF - 5 or config.piece_count > 0xFFFFFFFF:
            raise ValueError("File dimensions exceed the protocol's 32-bit fields")
        return config


@dataclass(frozen=True)
class PeerInfo:
    peer_id: int
    host: str
    port: int
    has_file: bool


def load_peers(path):
    peers = []
    ids, endpoints = set(), set()
    for line, fields in rows(path):
        if len(fields) != 4:
            raise ValueError("%s:%s: expected ID host port has-file" % (path, line))
        peer_id, host, port, has_file = fields
        peer_id, port = int(peer_id), int(port)
        if not 1 <= peer_id <= 0xFFFFFFFF or not 1 <= port <= 65535:
            raise ValueError("%s:%s: invalid peer ID or port" % (path, line))
        if has_file not in ("0", "1"):
            raise ValueError("%s:%s: has-file must be 0 or 1" % (path, line))
        endpoint = (host.lower(), port)
        if peer_id in ids or endpoint in endpoints:
            raise ValueError("%s:%s: duplicate peer ID or endpoint" % (path, line))
        ids.add(peer_id)
        endpoints.add(endpoint)
        peers.append(PeerInfo(peer_id, host, port, has_file == "1"))
    if not peers or not any(peer.has_file for peer in peers):
        raise ValueError("PeerInfo.cfg must contain at least one initial seed")
    return peers

