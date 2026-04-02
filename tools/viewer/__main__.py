#!/usr/bin/env python3
"""Entry point: python -m viewer [--remote-bin PATH] [/path/to/repo | host:/path/to/repo]"""

import sys
from . import rpc
from .app import ViewerApp


def main() -> None:
    args = sys.argv[1:]

    # Parse --remote-bin before handing off to the app
    if "--remote-bin" in args:
        idx = args.index("--remote-bin")
        if idx + 1 >= len(args):
            print("Error: --remote-bin requires a path argument",
                  file=sys.stderr)
            sys.exit(1)
        rpc.REMOTE_BIN = args[idx + 1]
        del args[idx:idx + 2]

    app = ViewerApp()
    if args:
        app.load_repo(args[0])
    app.mainloop()


if __name__ == "__main__":
    main()
