#!/usr/bin/env python3
"""List open file descriptors as a JSON array.

Reads /dev/fd and filters with fstat to exclude the transient
directory handle that os.listdir opens internally.

Output: JSON array on stdout, e.g. [0,1,2] or [0,1,2,3,5]
"""

import json
import os
import sys


def _is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
        return True
    except OSError:
        return False


fds = sorted(fd for entry in os.listdir("/dev/fd") if _is_open(fd := int(entry)))

json.dump(fds, sys.stdout)
print()
