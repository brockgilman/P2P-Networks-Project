#!/usr/bin/env python3
"""Launch peers in configuration order, verify hashes, and reap every process."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from p2p.config import CommonConfig, load_peers


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def run(directory, timeout=180, startup_delay=0.1):
    directory = Path(directory).resolve()
    config = CommonConfig.load(directory / "Common.cfg")
    peers = load_peers(directory / "PeerInfo.cfg")
    if any(peer.host.lower() not in ("localhost", "127.0.0.1") for peer in peers):
        raise ValueError("run_local requires localhost addresses; start remote peers on their own hosts")
    if list(directory.glob("log_peer_*.log")):
        raise ValueError("This run already has logs. Prepare a fresh directory to preserve the evidence.")
    expected = None
    for peer in peers:
        if peer.has_file:
            seed = directory / ("peer_%s" % peer.peer_id) / config.file_name
            if seed.stat().st_size != config.file_size:
                raise ValueError("Seed file has the wrong size: %s" % seed)
            seed_digest = digest(seed)
            if expected is not None and expected != seed_digest:
                raise ValueError("Initial seeds do not contain identical files")
            expected = seed_digest
    processes, streams = [], []
    started = time.monotonic()
    try:
        for peer in peers:
            stream = (directory / ("console_peer_%s.log" % peer.peer_id)).open("wb")
            streams.append(stream)
            process = subprocess.Popen(
                [sys.executable, str(REPO / "peerProcess.py"), str(peer.peer_id)],
                cwd=directory, stdout=stream, stderr=subprocess.STDOUT)
            processes.append((peer, process))
            time.sleep(startup_delay)
        while any(process.poll() is None for _, process in processes):
            failed = [(peer.peer_id, process.returncode) for peer, process in processes
                      if process.poll() not in (None, 0)]
            if failed:
                details = []
                for peer_id, _ in failed:
                    console = directory / ("console_peer_%s.log" % peer_id)
                    details.append(console.read_text(encoding="utf-8", errors="replace")[-2000:])
                raise RuntimeError("Peers failed: %s\n%s" % (failed, "\n".join(details)))
            if time.monotonic() - started > timeout:
                raise TimeoutError("Swarm did not terminate within %ss; see peer logs" % timeout)
            time.sleep(0.1)
        if any(process.returncode != 0 for _, process in processes):
            raise RuntimeError("A peer failed; see console_peer_*.log")
        records = []
        for peer, process in processes:
            path = directory / ("peer_%s" % peer.peer_id) / config.file_name
            checksum = digest(path)
            if path.stat().st_size != config.file_size or checksum != expected:
                raise RuntimeError("Peer %s did not reconstruct the correct file" % peer.peer_id)
            log = (directory / ("log_peer_%s.log" % peer.peer_id)).read_text(encoding="utf-8")
            records.append({"peer_id": peer.peer_id, "exit_code": process.returncode,
                            "bytes": path.stat().st_size, "sha256": checksum,
                            "downloaded_pieces": log.count("has downloaded the piece "),
                            "optimistic_selections": log.count("has the optimistically unchoked neighbor "),
                            "chokes_received": log.count(" is choked by ")})
        result = {"passed": True, "elapsed_seconds": round(time.monotonic() - started, 3),
                  "piece_size": config.piece_size, "piece_count": config.piece_count,
                  "peers": records}
        (directory / "verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        for _, process in processes:
            if process.poll() is None:
                process.terminate()
        for _, process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for stream in streams:
            stream.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--startup-delay", type=float, default=0.1)
    args = parser.parse_args()
    if args.timeout <= 0 or args.startup_delay < 0:
        parser.error("Timeout must be positive and startup delay must be nonnegative")
    try:
        result = run(args.directory, args.timeout, args.startup_delay)
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        parser.exit(1, "run_local: %s\n" % error)
    print("PASS: %s peers, %s pieces each, identical SHA-256 hashes, all exited in %ss" %
          (len(result["peers"]), result["piece_count"], result["elapsed_seconds"]))
    print("Evidence: %s" % (args.directory.resolve() / "verification.json"))


if __name__ == "__main__":
    main()
