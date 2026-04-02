"""
RPC bridge to the C backup binary.

Two modes of operation:

1. **Session mode** (preferred): a persistent ``backup --json-session <repo>``
   subprocess reads newline-delimited JSON requests on stdin and writes
   responses on stdout.  One process, one repo open — zero fork overhead
   per call.

2. **One-shot mode** (fallback): each ``call()`` spawns a fresh
   ``backup --json <repo>`` subprocess.  Used when the binary does not
   support ``--json-session`` or the session process dies.

**Local repos**: pass a plain path, e.g. ``/mnt/backup/c-backup``.
**Remote repos**: pass ``host:path`` or ``user@host:path``, e.g.
``nas:/mnt/backup/c-backup``.  The viewer will invoke the backup binary
on the remote host via SSH.

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
import threading

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


# ------------------------------------------------------------------
# Persistent session
# ------------------------------------------------------------------

class Session:
    """Persistent JSON session with the backup binary."""

    def __init__(self, repo_spec: str):
        self._repo_spec = repo_spec
        self._proc: subprocess.Popen | None = None
        self._lock = threading.Lock()

    def start(self) -> bool:
        """Launch the persistent process.  Returns True on success."""
        host, repo_path = parse_remote(self._repo_spec)

        if host is None:
            cmd = [BACKUP_BIN, "--json-session", repo_path]
        else:
            cmd = ["ssh"] + _ssh_base_opts(host) + [host,
                   REMOTE_BIN, "--json-session", repo_path]

        try:
            self._proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            return False

        # Read ready banner (with timeout)
        try:
            line = self._readline(timeout=15)
        except RPCError:
            self._kill()
            return False

        try:
            banner = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            self._kill()
            return False

        if banner.get("status") == "error":
            self._kill()
            raise RPCError(banner.get("message", "session start failed"))

        return banner.get("status") == "ready"

    def call(self, action: str, **params) -> dict:
        """Send a request and read the response.  Thread-safe."""
        with self._lock:
            proc = self._proc
            if not proc or proc.poll() is not None:
                raise RPCError("session process not running")

            req = {"action": action}
            if params:
                req["params"] = {k: v for k, v in params.items()
                                 if v is not None}

            line_out = json.dumps(req, separators=(",", ":")) + "\n"
            try:
                proc.stdin.write(line_out.encode())
                proc.stdin.flush()
            except (BrokenPipeError, OSError) as e:
                raise RPCError(f"session write failed: {e}")

            try:
                resp_line = self._readline(timeout=120)
            except RPCError:
                raise

            try:
                resp = json.loads(resp_line)
            except json.JSONDecodeError as e:
                raise RPCError(f"invalid JSON from session: {e}")

            if resp.get("status") == "error":
                raise RPCError(resp.get("message", "unknown error"))

            return resp.get("data", {})

    def close(self) -> None:
        """Gracefully shut down the session."""
        proc = self._proc
        if not proc:
            return
        if proc.poll() is None:
            try:
                proc.stdin.write(b'{"action":"quit"}\n')
                proc.stdin.flush()
                proc.wait(timeout=5)
            except Exception:
                self._kill()
        self._proc = None

    @property
    def alive(self) -> bool:
        p = self._proc
        return p is not None and p.poll() is None

    def _readline(self, timeout: float = 120) -> str:
        """Read one line from stdout with timeout."""
        proc = self._proc
        if not proc or not proc.stdout:
            raise RPCError("session not started")
        fd = proc.stdout.fileno()
        ready, _, _ = select.select([fd], [], [], timeout)
        if not ready:
            raise RPCError("session read timed out")
        line = proc.stdout.readline()
        if not line:
            raise RPCError("session process EOF")
        return line.decode()

    def _kill(self) -> None:
        proc = self._proc
        if proc:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass
        self._proc = None


# Module-level session keyed by repo_spec
_sessions: dict[str, Session] = {}


def open_session(repo_spec: str) -> Session | None:
    """Open a persistent session for *repo_spec*.

    Returns the Session on success, None if the binary doesn't support
    ``--json-session``.  Caches by repo_spec.
    """
    existing = _sessions.get(repo_spec)
    if existing and existing.alive:
        return existing

    sess = Session(repo_spec)
    try:
        if sess.start():
            _sessions[repo_spec] = sess
            return sess
    except RPCError:
        pass
    return None


def close_session(repo_spec: str) -> None:
    """Close the persistent session for *repo_spec*, if any."""
    sess = _sessions.pop(repo_spec, None)
    if sess:
        sess.close()


def close_all_sessions() -> None:
    """Close all persistent sessions."""
    for spec in list(_sessions):
        close_session(spec)


atexit.register(close_all_sessions)
atexit.register(ssh_disconnect_all)


# ------------------------------------------------------------------
# RPC call (unified)
# ------------------------------------------------------------------

def call(repo_spec: str, action: str, **params) -> dict:
    """Invoke a JSON API action and return the data dict.

    Uses a persistent session if available, falls back to one-shot.
    *repo_spec* is either a local path or ``host:path`` for SSH.

    Raises RPCError on action errors, RuntimeError on protocol failures.
    """
    # Try session first
    sess = _sessions.get(repo_spec)
    if sess and sess.alive:
        try:
            return sess.call(action, **params)
        except RPCError:
            # Session died — remove it, fall through to one-shot
            _sessions.pop(repo_spec, None)

    # One-shot fallback
    return _call_oneshot(repo_spec, action, **params)


def _call_oneshot(repo_spec: str, action: str, **params) -> dict:
    """One-shot RPC: spawn a subprocess, send one request, return response."""
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
