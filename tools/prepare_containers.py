#!/usr/bin/env python3
"""Create six isolated container working directories with a generated seed."""

import argparse
from pathlib import Path
import shutil
import tempfile

from prepare_demo import prepare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("runs/containers"))
    args = parser.parse_args()
    if args.output.exists():
        parser.exit(1, "Choose a fresh output directory to preserve previous runs.\n")
    args.output.mkdir(parents=True)
    with tempfile.TemporaryDirectory() as temporary:
        source = prepare(Path(temporary) / "source")
        peer_text = "".join("%s peer%s 6001 %s\n" % (peer_id, peer_id, int(peer_id == 1001))
                            for peer_id in range(1001, 1007))
        for peer_id in range(1001, 1007):
            work = args.output / str(peer_id)
            work.mkdir()
            shutil.copy2(source / "Common.cfg", work / "Common.cfg")
            (work / "PeerInfo.cfg").write_text(peer_text, encoding="utf-8")
            shutil.copytree(source / ("peer_%s" % peer_id), work / ("peer_%s" % peer_id))
    print("Prepared six isolated working directories in %s" % args.output.resolve())


if __name__ == "__main__":
    main()
