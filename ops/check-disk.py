#!/usr/bin/env python3
"""Stop only the managed collector if its volume has less than 5 GiB free."""
import shutil
import subprocess

free = shutil.disk_usage("/var/lib/voluntas-collector").free
if free < 5 * 1024**3:
    print("Low disk space; stopping voluntas-collector.service", flush=True)
    subprocess.run(["systemctl", "--no-block", "stop", "voluntas-collector.service"], check=True)
