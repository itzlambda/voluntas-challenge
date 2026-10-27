import importlib.util
import os
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("reclaim", Path(__file__).with_name("reclaim-output.py"))
reclaimer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reclaimer)


class ReclaimTests(unittest.TestCase):
    def test_reclaims_blocks_preserving_tail_and_active_writer(self):
        chunk = b"Z" * reclaimer.CHUNK
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "output.sbe"
            state = Path(directory) / "state.json"
            with path.open("w+b", buffering=0) as writer:
                for _ in range(3):
                    writer.write(chunk)
                os.fsync(writer.fileno())
                before = os.fstat(writer.fileno())
                position = writer.tell()
                self.assertEqual(reclaimer.reclaim(path, state, reclaimer.CHUNK, 300, now=1000), 0)
                self.assertEqual(reclaimer.reclaim(path, state, reclaimer.CHUNK, 300, now=1301), 2 * reclaimer.CHUNK)
                after = os.fstat(writer.fileno())
                self.assertEqual(after.st_size, before.st_size)
                self.assertLess(after.st_blocks, before.st_blocks)
                self.assertEqual(writer.tell(), position)
                self.assertEqual(os.pread(writer.fileno(), 1024, 0), bytes(1024))
                self.assertEqual(os.pread(writer.fileno(), reclaimer.CHUNK, 2 * reclaimer.CHUNK), chunk)
                writer.write(chunk)
                reclaimer.reclaim(path, state, reclaimer.CHUNK, 300, now=1302)
                self.assertEqual(os.pread(writer.fileno(), reclaimer.CHUNK, 3 * reclaimer.CHUNK), chunk)

    def test_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "target").touch()
            (root / "link").symlink_to(root / "target")
            with self.assertRaises(OSError):
                reclaimer.reclaim(root / "link", root / "state.json", reclaimer.CHUNK, 300)
