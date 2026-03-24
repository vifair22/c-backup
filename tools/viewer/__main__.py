#!/usr/bin/env python3
"""Entry point: python -m viewer [/path/to/repo]"""

import os
import sys
from .app import ViewerApp


def main() -> None:
    app = ViewerApp()
    if len(sys.argv) > 1:
        path = sys.argv[1]
        if os.path.isdir(path):
            app.load_repo(path)
        else:
            app._open_single_file(path)
    app.mainloop()


if __name__ == "__main__":
    main()
