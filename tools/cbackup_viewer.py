#!/usr/bin/env python3
"""
c-backup repository viewer — launcher shim.

Usage:
    python cbackup_viewer.py [/path/to/repo]
    python -m viewer [/path/to/repo]       (from the tools/ directory)

Requirements:
    tkinter (Python stdlib)
    lz4     (optional, for payload decompression: pip install lz4)
"""

import os
import sys

# Allow running as a plain script from anywhere
sys.path.insert(0, os.path.dirname(__file__))

from viewer.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
