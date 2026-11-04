#!/usr/bin/env python3
"""Print a file's byte size and SHA-256 hash without loading it all into memory."""

import argparse
import hashlib
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    args = parser.parse_args()
    digest = hashlib.sha256()
    with args.file.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    print("%s bytes  %s  %s" % (args.file.stat().st_size, digest.hexdigest(), args.file))


if __name__ == "__main__":
    main()
