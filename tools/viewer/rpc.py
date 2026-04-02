"""
RPC bridge to the C backup binary.

All data access goes through ``call(repo_path, action, **params)`` which
invokes ``backup --json <repo>`` as a subprocess, passes a JSON request
on stdin, and returns the parsed response data dict.

The backup binary is located via the CBACKUP_BIN environment variable,
falling back to a build-relative path and then the system PATH.
"""

import json
import os
import subprocess

# Locate the backup binary: env override > build dir relative to repo > PATH
_HERE = os.path.dirname(os.path.abspath(__file__))
_BUILD_BIN = os.path.normpath(os.path.join(_HERE, "..", "..", "build", "backup"))

BACKUP_BIN = os.environ.get("CBACKUP_BIN") or (
    _BUILD_BIN if os.path.isfile(_BUILD_BIN) else "backup"
)


class RPCError(Exception):
    """Raised when the backup binary returns an error response."""
    pass


def call(repo_path: str, action: str, **params) -> dict:
    """Invoke a JSON API action and return the data dict.

    Raises RPCError on action errors, RuntimeError on protocol failures.
    """
    req = {"action": action}
    if params:
        req["params"] = {k: v for k, v in params.items() if v is not None}

    try:
        proc = subprocess.run(
            [BACKUP_BIN, "--json", repo_path],
            input=json.dumps(req).encode(),
            capture_output=True,
            timeout=120,
        )
    except FileNotFoundError:
        raise RPCError(
            f"backup binary not found at '{BACKUP_BIN}'. "
            "Set CBACKUP_BIN or run 'make' first."
        )
    except subprocess.TimeoutExpired:
        raise RPCError(f"backup --json timed out for action '{action}'")

    if not proc.stdout:
        stderr = proc.stderr.decode(errors="replace").strip()
        raise RPCError(f"backup --json produced no output: {stderr}")

    try:
        resp = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RPCError(f"invalid JSON from backup: {e}")

    if resp.get("status") == "error":
        raise RPCError(resp.get("message", "unknown error"))

    return resp.get("data", {})
