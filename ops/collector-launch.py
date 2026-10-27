#!/usr/bin/env python3
"""Hold the benchmark coordination lock for the collector's entire lifetime."""
import fcntl
import os
from pathlib import Path
import sys

lock = open("/srv/collector-test/validation.lock", "rb")
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit("Benchmark coordination lock is held; collector startup refused")
for entry in Path("/proc").iterdir():
    if not entry.name.isdigit() or int(entry.name) == os.getpid():
        continue
    try:
        name = (entry / "comm").read_text().strip()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
    if name in ("collector", "engineering-cha"):
        sys.exit("Another collector is active; collector startup refused")
os.set_inheritable(lock.fileno(), True)
os.execv(sys.argv[1], sys.argv[1:])
