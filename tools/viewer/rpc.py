"""
RPC bridge to the C backup binary.

All data access goes through ``call(repo_path, action, **params)`` which
invokes ``backup --json <repo>`` as a subprocess (locally or over SSH),
passes a JSON request on stdin, and returns the parsed response data dict.

**Local repos**: pass a plain path, e.g. ``/mnt/backup/c-backup``.
**Remote repos**: pass ``host:path`` or ``user@host:path``, e.g.
``nas:/mnt/backup/c-backup``.  The viewer will invoke the backup binary
on the remote host via ``ssh <host> backup --json <path>``.

SSH authentication:
  - Key auth is attempted first (BatchMode).
  - If key auth fails, the caller can provide a password via
    ``ssh_connect(host, password)`` which uses a pty to feed it.
  - A ControlMaster socket is established on first connect and reused
    for all subsequent RPC calls, so auth only happens once per session.
"""

import atexit
import json
import os
import pty
import select
import subprocess
import tempfile

# Locate the backup binary: env override > build dir relative to repo > PATH
_HERE = os.path.dirname(os.path.abspath(__file__))
_BUILD_BIN = os.path.normpath(
    os.path.join(_HERE, "..", "..", "build", "backup"))

BACKUP_BIN = os.environ.get("CBACKUP_BIN") or (
    _BUILD_BIN if os.path.isfile(_BUILD_BIN) else "backup"
)

REMOTE_BIN = os.environ.get("CBACKUP_REMOTE_BIN", "backup")

# SSH ControlMaster sockets keyed by host string
_control_sockets: dict[str, str] = {}


class RPCError(Exception):
    """Raised when the backup binary returns an error response."""
    pass


def parse_remote(repo_spec: str) -> tuple[str | None, str]:
    """Split ``[user@]host:path`` into (host, path).

    Returns (None, repo_spec) for local paths.  A leading ``/`` or ``.``
    is treated as local even if it contains a colon (e.g. ``/mnt/c:drive``).
    Windows-style ``C:\\...`` is also treated as local.
    """
    if not repo_spec:
        return None, repo_spec
    if repo_spec[0] in ("/", "."):
        return None, repo_spec
    if len(repo_spec) >= 2 and repo_spec[1] == ":":
        return None, repo_spec
    colon = repo_spec.find(":")
    if colon < 1:
        return None, repo_spec
    host = repo_spec[:colon]
    path = repo_spec[colon + 1:]
    if not path:
        return None, repo_spec
    return host, path


def is_remote(repo_spec: str) -> bool:
    """Return True if repo_spec targets a remote host."""
    host, _ = parse_remote(repo_spec)
    return host is not None


# ------------------------------------------------------------------
# SSH connection management
# ------------------------------------------------------------------

def _control_path(host: str) -> str:
    """Get or create a control socket path for a host."""
    if host not in _control_sockets:
        tmpdir = tempfile.mkdtemp(prefix="cbackup-ssh-")
        _control_sockets[host] = os.path.join(tmpdir, "ctrl")
    return _control_sockets[host]


def _ssh_base_opts(host: str) -> list[str]:
    """Common SSH options for a host that has a control socket."""
    ctrl = _control_sockets.get(host)
    if not ctrl:
        return []
    return ["-o", f"ControlPath={ctrl}", "-o", "ControlMaster=auto"]


def ssh_connect(host: str, password: str | None = None) -> bool:
    """Establish an SSH ControlMaster connection to *host*.

    Tries key auth first (BatchMode).  If *password* is provided and
    key auth fails, uses a pty to feed the password to SSH.

    Returns True on success, False on auth failure.
    """
    ctrl = _control_path(host)

    master_opts = [
        "-o", f"ControlPath={ctrl}",
        "-o", "ControlMaster=yes",
        "-o", "ControlPersist=600",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=10",
    ]

    # Try key auth (BatchMode suppresses password prompts)
    try:
        r = subprocess.run(
            ["ssh"] + master_opts + ["-o", "BatchMode=yes", host, "true"],
            capture_output=True, timeout=15)
        if r.returncode == 0:
            return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass

    if password is None:
        return False

    # Password auth via pty
    cmd = ["ssh"] + master_opts + [host, "true"]

    pid, fd = pty.fork()
    if pid == 0:
        # Child — replace with ssh
        os.execvp("ssh", cmd)
        os._exit(1)

    # Parent — watch for password prompt, feed it
    try:
        buf = b""
        sent = False
        while True:
            ready, _, _ = select.select([fd], [], [], 15)
            if not ready:
                break
            try:
                data = os.read(fd, 4096)
            except OSError:
                break
            if not data:
                break
            buf += data
            low = buf.lower()
            if not sent and (b"password:" in low or b"passphrase" in low):
                os.write(fd, (password + "\n").encode())
                sent = True
                buf = b""
    except Exception:
        pass
    finally:
        try:
            os.close(fd)
        except OSError:
            pass

    _, status = os.waitpid(pid, 0)
    return os.WEXITSTATUS(status) == 0


def ssh_disconnect(host: str) -> None:
    """Tear down the ControlMaster for *host*."""
    ctrl = _control_sockets.pop(host, None)
    if not ctrl:
        return
    try:
        subprocess.run(
            ["ssh", "-o", f"ControlPath={ctrl}", "-O", "exit", host],
            capture_output=True, timeout=5)
    except Exception:
        pass
    try:
        os.unlink(ctrl)
        os.rmdir(os.path.dirname(ctrl))
    except OSError:
        pass


def ssh_disconnect_all() -> None:
    """Tear down all ControlMaster connections."""
    for host in list(_control_sockets):
        ssh_disconnect(host)


atexit.register(ssh_disconnect_all)


# ------------------------------------------------------------------
# RPC call
# ------------------------------------------------------------------

def call(repo_spec: str, action: str, **params) -> dict:
    """Invoke a JSON API action and return the data dict.

    *repo_spec* is either a local path or ``host:path`` for SSH.

    Raises RPCError on action errors, RuntimeError on protocol failures.
    """
    host, repo_path = parse_remote(repo_spec)

    req = {"action": action}
    if params:
        req["params"] = {k: v for k, v in params.items() if v is not None}

    payload = json.dumps(req).encode()

    if host is None:
        cmd = [BACKUP_BIN, "--json", repo_path]
    else:
        cmd = ["ssh"] + _ssh_base_opts(host) + [host,
               REMOTE_BIN, "--json", repo_path]

    try:
        proc = subprocess.run(
            cmd,
            input=payload,
            capture_output=True,
            timeout=120,
        )
    except FileNotFoundError:
        if host:
            raise RPCError(
                f"ssh not found — cannot reach remote host '{host}'")
        raise RPCError(
            f"backup binary not found at '{BACKUP_BIN}'. "
            "Set CBACKUP_BIN or run 'make' first."
        )
    except subprocess.TimeoutExpired:
        target = f"{host}:{repo_path}" if host else repo_path
        raise RPCError(
            f"backup --json timed out for action '{action}' on {target}")

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
