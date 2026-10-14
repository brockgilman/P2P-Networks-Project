"""Assignment event messages plus clearly identified diagnostic messages."""

from datetime import datetime
from pathlib import Path


class EventLog:
    def __init__(self, root, peer_id):
        self.peer_id = peer_id
        self.file = (Path(root) / ("log_peer_%s.log" % peer_id)).open(
            "w", encoding="utf-8", buffering=1)

    def write(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.file.write("[%s]: Peer %s %s\n" % (timestamp, self.peer_id, message))

    def close(self):
        self.file.close()
