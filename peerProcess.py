#!/usr/bin/env python3
"""Run from the directory containing Common.cfg and PeerInfo.cfg."""

import argparse
import asyncio
import sys
from pathlib import Path

from p2p.config import CommonConfig, load_peers
from p2p.peer import Peer


def main():
    parser = argparse.ArgumentParser(description="CNT4007 P2P file-sharing peer")
    parser.add_argument("peer_id", type=int, help="peer ID from PeerInfo.cfg")
    args = parser.parse_args()
    try:
        root = Path.cwd()
        config = CommonConfig.load(root / "Common.cfg")
        peers = load_peers(root / "PeerInfo.cfg")
        asyncio.run(Peer(root, args.peer_id, config, peers).run())
    except KeyboardInterrupt:
        print("Peer interrupted.", file=sys.stderr)
        return 130
    except (OSError, ValueError, RuntimeError) as error:
        print("peerProcess: %s" % error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

