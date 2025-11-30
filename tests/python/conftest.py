import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError


DEFAULT_URL = "postgresql+psycopg://clarium:clarium@127.0.0.1:5433/clarium"


def get_db_url() -> str:
    # Allow overriding via env var; fall back to default pgwire port
    return os.getenv("CLARIUM_SQLA_URL", DEFAULT_URL)


def _host_port_from_url(url: str) -> Tuple[str, int]:
    u = make_url(url)
    host = u.host or "127.0.0.1"
    port = int(u.port or 5433)
    return host, port


def _can_connect(host: str, port: int, timeout_sec: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True
    except OSError:
        return False


# def _start_server_if_needed(url: str) -> Optional[subprocess.Popen]:
#     if os.getenv("CLARIUM_SKIP_AUTOSTART") in ("1", "true", "yes", "on"):
#         return None
#
#     host, port = _host_port_from_url(url)
#     if _can_connect(host, port):
#         # Already running
#         return None
#
#     # Prepare logs dir
#     logs_dir = Path("target") / "test-logs"
#     logs_dir.mkdir(parents=True, exist_ok=True)
#     log_path = logs_dir / "clarium_server.log"
#
#     # Build command; allow overrides
#     cargo = os.getenv("CLARIUM_CARGO", "cargo")
#     server_bin = os.getenv("CLARIUM_SERVER_BIN", "clarium_server")
#     extra_args = os.getenv("CLARIUM_ARGS", "").strip()
#
#     cmd = [
#         cargo,
#         "run",
#         "--release",
#         "--features",
#         "pgwire",
#         "--bin",
#         server_bin,
#         "--",
#         "--pgwire",
#     ]
#     if extra_args:
#         # rudimentary split on spaces
#         cmd += extra_args.split()
#
#     env = os.environ.copy()
#     # If TIMELINE_PG_PORT is set, server will honor it; otherwise, rely on defaults.
#     # Ensure RUST_LOG is not too noisy unless the user set it.
#     env.setdefault("RUST_LOG", "info")
#
#     # Start background process, redirecting output to a log file
#     creationflags = 0
#     if sys.platform.startswith("win"):
#         # Create new process group to improve termination behavior
#         creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
#
#     log_file = open(log_path, "ab", buffering=0)
#     proc = subprocess.Popen(
#         cmd,
#         stdout=log_file,
#         stderr=subprocess.STDOUT,
#         env=env,
#         creationflags=creationflags,
#     )
#
#     # Wait for readiness (port accepting connections)
#     deadline = time.time() + float(os.getenv("CLARIUM_STARTUP_TIMEOUT", "45"))
#     last_err: Optional[str] = None
#     while time.time() < deadline:
#         if proc.poll() is not None:
#             last_err = f"clarium_server exited early with code {proc.returncode}. See {log_path}"
#             break
#         if _can_connect(host, port):
#             return proc
#         time.sleep(0.5)
#
#     # If not ready, stop process and return None (caller will skip)
#     try:
#         if proc.poll() is None:
#             proc.terminate()
#             try:
#                 proc.wait(timeout=5)
#             except subprocess.TimeoutExpired:
#                 proc.kill()
#     finally:
#         try:
#             log_file.flush()
#             log_file.close()
#         except Exception:
#             pass
#     raise RuntimeError(last_err or f"timeline_server did not become ready on {host}:{port} within timeout. See {log_path}")


# @pytest.fixture(scope="session", autouse=True)
# def _ensure_server_running():
#     """Autostart the Clarium clarium_server with pgwire if not already running.
#
#     Honors environment variables:
#       - CLARIUM_SQLA_URL: SQLAlchemy URL to detect host/port (default {DEFAULT_URL}).
#       - CLARIUM_SKIP_AUTOSTART=1 to disable autostart.
#       - CLARIUM_CARGO: cargo executable path (default 'cargo').
#       - CLARIUM_SERVER_BIN: server binary name (default 'clarium_server').
#       - CLARIUM_ARGS: extra CLI args passed after '--pgwire'.
#       - CLARIUM_STARTUP_TIMEOUT: seconds to wait for readiness (default 45).
#     """
#     url = get_db_url()
#     proc: Optional[subprocess.Popen] = None
#     log_file_obj = None  # Keep a reference to avoid GC closing the file
#     try:
#         try:
#             proc = _start_server_if_needed(url)
#         except RuntimeError as e:
#             pytest.skip(str(e))
#         yield
#     finally:
#         # If we started it, attempt to shut it down
#         if proc is not None and proc.poll() is None:
#             try:
#                 proc.terminate()
#                 proc.wait(timeout=10)
#             except Exception:
#                 try:
#                     proc.kill()
#                 except Exception:
#                     pass


@pytest.fixture(scope="session")
def engine() -> Engine:
    url = get_db_url()
    host, port = _host_port_from_url(url)

    # If DB is not required, short-circuit quickly when the port is closed
    require_db = os.getenv("CLARIUM_REQUIRE_DB", "").lower() in ("1", "true", "yes", "on")
    if not require_db and not _can_connect(host, port, timeout_sec=0.3):
        pytest.skip(f"Clarium pgwire is not listening on {host}:{port}; skipping tests. Set CLARIUM_REQUIRE_DB=1 to fail instead.")

    # Ensure failed connections don't hang indefinitely when the server isn't up.
    # libpq honors the 'connect_timeout' parameter (seconds).
    eng = create_engine(
        url,
        future=True,
        connect_args={"connect_timeout": 2},  # fast-fail if pgwire isn't listening
        pool_pre_ping=True,
    )

    # Bounded wait in case the server is just starting up
    # Defaults are conservative and configurable via env vars
    max_wait_sec = float(os.getenv("CLARIUM_CONNECT_MAX_SEC", "4"))
    retry_interval = float(os.getenv("CLARIUM_CONNECT_RETRY_INTERVAL_SEC", "0.3"))
    deadline = time.time() + max_wait_sec

    last_err = None
    while time.time() < deadline:
        try:
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
            last_err = None
            break
        except OperationalError as e:
            last_err = e
            time.sleep(retry_interval)

    if last_err is not None:
        msg = f"Cannot connect to Clarium pgwire at {url}: {last_err}"
        if require_db:
            pytest.fail(msg)
        else:
            pytest.skip(msg)

    return eng


@pytest.fixture(scope="function")
def conn(engine: Engine):
    with engine.begin() as connection:
        yield connection
