#!/usr/bin/env python3
"""Create a fresh run directory; never overwrite an existing experiment."""

import argparse
from pathlib import Path
import random
import shutil
import sys
import zipfile

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from p2p.config import CommonConfig, load_peers


def prepare(output, dataset="generated", source=None, count=6, size=25 * 1024 * 1024,
            base_port=6001, preferred=None, interval=None, optimistic=None):
    output = Path(output)
    if output.exists():
        raise ValueError("Output already exists; choose a fresh directory: %s" % output)
    if dataset == "generated":
        if count < 1 or size < 1:
            raise ValueError("Peer count and file size must be positive")
        config_text = (
            "NumberOfPreferredNeighbors %s\nUnchokingInterval %s\n"
            "OptimisticUnchokingInterval %s\nFileName demo.dat\n"
            "FileSize %s\nPieceSize 16384\n" % (
                preferred if preferred is not None else 3,
                interval if interval is not None else 5,
                optimistic if optimistic is not None else 10, size))
        ids = list(range(1001, 1001 + count))
        seeds = {ids[0]}
        if not 1 <= base_port <= 65536 - len(ids):
            raise ValueError("Local ports must all be between 1 and 65535")
        output.mkdir(parents=True)
        (output / "Common.cfg").write_text(config_text, encoding="utf-8")
        config = CommonConfig.load(output / "Common.cfg")
        seed_dir = output / ("peer_%s" % ids[0])
        seed_dir.mkdir()
        rng = random.Random(4007)
        with (seed_dir / config.file_name).open("wb") as file:
            remaining = size
            while remaining:
                block = rng.randbytes(min(1024 * 1024, remaining))
                file.write(block)
                remaining -= len(block)
    else:
        source = Path(source) if source else REPO / "reference" / ("project_config_file_%s.zip" % dataset)
        with zipfile.ZipFile(source) as archive:
            config_name = next(n for n in archive.namelist()
                               if n.endswith("/Common.cfg") and not n.startswith("__MACOSX/"))
            prefix = config_name.rsplit("/", 1)[0]
            config_text = archive.read(config_name).decode("utf-8-sig")
            replacements = {"NumberOfPreferredNeighbors": preferred,
                            "UnchokingInterval": interval,
                            "OptimisticUnchokingInterval": optimistic}
            lines = []
            for line in config_text.splitlines():
                fields = line.split()
                if fields and replacements.get(fields[0]) is not None:
                    line = "%s %s" % (fields[0], replacements[fields[0]])
                lines.append(line)
            config_text = "\n".join(lines) + "\n"
            peer_text = archive.read(prefix + "/PeerInfo.cfg").decode("utf-8-sig")
            rows = [row.split() for row in peer_text.splitlines() if row.strip()]
            ids = [int(row[0]) for row in rows]
            seeds = {int(row[0]) for row in rows if row[3] == "1"}
            if not 1 <= base_port <= 65536 - len(ids):
                raise ValueError("Local ports must all be between 1 and 65535")
            output.mkdir(parents=True)
            (output / "Common.cfg").write_text(config_text, encoding="utf-8")
            config = CommonConfig.load(output / "Common.cfg")
            for peer_id in seeds:
                destination = output / ("peer_%s" % peer_id)
                destination.mkdir()
                # Copy only known data entries, not arbitrary archive paths.
                with archive.open("%s/%s/%s" % (prefix, peer_id, config.file_name)) as src:
                    with (destination / config.file_name).open("wb") as dst:
                        shutil.copyfileobj(src, dst)
    for peer_id in ids:
        (output / ("peer_%s" % peer_id)).mkdir(exist_ok=True)
    peer_text = "".join("%s 127.0.0.1 %s %s\n" % (peer_id, base_port + i, int(peer_id in seeds))
                        for i, peer_id in enumerate(ids))
    (output / "PeerInfo.cfg").write_text(peer_text, encoding="utf-8")
    load_peers(output / "PeerInfo.cfg")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--dataset", choices=("generated", "small", "large"), default="generated")
    parser.add_argument("--source", type=Path, help="path to the instructor's small/large ZIP")
    parser.add_argument("--peers", type=int, default=6, help="generated dataset only")
    parser.add_argument("--size", type=int, default=25 * 1024 * 1024, help="generated file bytes")
    parser.add_argument("--base-port", type=int, default=6001)
    parser.add_argument("--preferred", type=int)
    parser.add_argument("--interval", type=int)
    parser.add_argument("--optimistic", type=int)
    args = parser.parse_args()
    try:
        directory = prepare(args.output, args.dataset, args.source, args.peers, args.size,
                            args.base_port, args.preferred, args.interval, args.optimistic)
    except (OSError, ValueError, KeyError, StopIteration, zipfile.BadZipFile) as error:
        parser.exit(1, "prepare_demo: %s\n" % error)
    print("Prepared %s" % directory.resolve())


if __name__ == "__main__":
    main()

