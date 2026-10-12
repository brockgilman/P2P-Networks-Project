from pathlib import Path
import tempfile
import unittest

from p2p.config import CommonConfig, PeerInfo, load_peers
from p2p.storage import PieceStore


COMMON = ("NumberOfPreferredNeighbors 2\nUnchokingInterval 1\n"
          "OptimisticUnchokingInterval 2\nFileName test.dat\nFileSize 10\nPieceSize 4\n")


class ConfigStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "Common.cfg"
        self.path.write_text(COMMON, encoding="utf-8")
        self.config = CommonConfig.load(self.path)

    def test_config_comments_and_bom(self):
        self.path.write_text("\ufeff# Configuration\n\n" + COMMON + "# end\n", encoding="utf-8")
        self.assertEqual(CommonConfig.load(self.path), self.config)

    def test_invalid_common_configs(self):
        cases = [COMMON + "FileSize 10\n", COMMON.replace("PieceSize 4\n", ""),
                 COMMON.replace("PieceSize 4", "PieceSize 0"),
                 COMMON.replace("FileSize 10", "FileSize -1"),
                 COMMON.replace("test.dat", "../outside.dat"),
                 COMMON.replace("UnchokingInterval 1", "UnchokingInterval 0"),
                 COMMON + "Unexpected 2\n"]
        for text in cases:
            with self.subTest(text=text):
                self.path.write_text(text, encoding="utf-8")
                with self.assertRaises(ValueError):
                    CommonConfig.load(self.path)

    def test_peer_order_is_file_order(self):
        path = self.root / "PeerInfo.cfg"
        path.write_text("9000 localhost 8001 1\n1000 localhost 8002 0\n", encoding="utf-8")
        self.assertEqual([p.peer_id for p in load_peers(path)], [9000, 1000])

    def test_invalid_peers(self):
        path = self.root / "PeerInfo.cfg"
        cases = ["", "1 localhost 8000 0\n", "1 localhost 8000 2\n",
                 "1 localhost 70000 1\n", "0 localhost 8000 1\n",
                 "1 localhost 8000 1\n1 localhost 8001 0\n",
                 "1 localhost 8000 1\n2 localhost 8000 0\n"]
        for text in cases:
            path.write_text(text, encoding="utf-8")
            with self.subTest(text=text), self.assertRaises(ValueError):
                load_peers(path)

    def test_out_of_order_pieces_and_atomic_completion(self):
        store = PieceStore(self.root, PeerInfo(1, "localhost", 8000, False), self.config)
        self.addCleanup(store.close)
        destination = self.root / "peer_1" / "test.dat"
        self.assertFalse(destination.exists())
        self.assertTrue(store.write(2, b"ij"))
        self.assertTrue(store.write(0, b"abcd"))
        self.assertFalse(store.write(0, b"abcd"))
        self.assertFalse(store.complete)
        self.assertEqual(store.read(2), b"ij")
        self.assertTrue(store.write(1, b"efgh"))
        self.assertTrue(store.complete)
        self.assertEqual(destination.read_bytes(), b"abcdefghij")
        self.assertFalse(destination.with_suffix(".dat.part").exists())
        self.assertEqual(store.read(0), b"abcd")

    def test_rejects_wrong_piece_size_and_missing_upload(self):
        store = PieceStore(self.root, PeerInfo(1, "localhost", 8000, False), self.config)
        self.addCleanup(store.close)
        with self.assertRaises(ValueError):
            store.write(2, b"bad")
        with self.assertRaises(ValueError):
            store.read(0)
        self.assertEqual(store.pieces, set())

    def test_seed_requires_exact_file_size(self):
        info = PeerInfo(1, "localhost", 8000, True)
        with self.assertRaises(ValueError):
            PieceStore(self.root, info, self.config)
        (self.root / "peer_1" / "test.dat").write_bytes(b"short")
        with self.assertRaises(ValueError):
            PieceStore(self.root, info, self.config)
        (self.root / "peer_1" / "test.dat").write_bytes(b"abcdefghij")
        store = PieceStore(self.root, info, self.config)
        self.addCleanup(store.close)
        self.assertTrue(store.complete)
        self.assertEqual(store.read(2), b"ij")
