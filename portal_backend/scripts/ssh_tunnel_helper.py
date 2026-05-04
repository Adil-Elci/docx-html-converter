#!/usr/bin/env python3
"""SSH tunnel helper for accessing remote database during development."""
from __future__ import annotations

import atexit
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


_tunnel_process: Optional[subprocess.Popen] = None
_ssh_config = {
    "user": "adilaltameemi",
    "host": "76.13.143.101",
    "remote_host": "localhost",
    "remote_port": 9876,
    "local_port": 5432,
    "url_port": 9876,  # external port used in DATABASE_URL
}


def _load_env_file(env_file: Path) -> dict[str, str]:
    """Load environment variables from a .env file."""
    env_vars = {}
    if not env_file.exists():
        return env_vars
    try:
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    env_vars[key.strip()] = value.strip()
    except Exception:
        pass
    return env_vars


def setup_ssh_tunnel() -> bool:
    """
    Establish SSH tunnel to remote database if DATABASE_URL is not already set.
    Returns True if tunnel was set up, False if already accessible or not needed.
    """
    global _tunnel_process

    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url and "localhost" in database_url:
        return False

    if not database_url:
        print("DATABASE_URL not set. Attempting to establish SSH tunnel...", file=sys.stderr)

    print(f"Setting up SSH tunnel to {_ssh_config['host']}...", file=sys.stderr)

    try:
        _tunnel_process = subprocess.Popen(
            [
                "ssh",
                "-N",
                "-L",
                f"{_ssh_config['local_port']}:{_ssh_config['remote_host']}:{_ssh_config['remote_port']}",
                f"{_ssh_config['user']}@{_ssh_config['host']}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        time.sleep(2)

        if _tunnel_process.poll() is not None:
            stderr = _tunnel_process.stderr.read().decode() if _tunnel_process.stderr else ""
            raise RuntimeError(f"SSH tunnel failed: {stderr}")

        print("SSH tunnel established. Using localhost:5432", file=sys.stderr)

        if not database_url:
            env_file = Path(__file__).resolve().parent.parent / ".env.live"
            env_vars = _load_env_file(env_file)
            if "DATABASE_URL" in env_vars:
                remote_url = env_vars["DATABASE_URL"]
                local_url = remote_url.replace(
                    f"{_ssh_config['host']}:{_ssh_config['url_port']}",
                    f"localhost:{_ssh_config['local_port']}",
                )
                os.environ["DATABASE_URL"] = local_url
                print(f"Loaded DATABASE_URL from .env.live", file=sys.stderr)
            else:
                default_db_url = "postgresql://postgres:postgres@localhost:5432/portal_db"
                os.environ["DATABASE_URL"] = default_db_url
                print(f"Set DATABASE_URL to default: {default_db_url}", file=sys.stderr)

        os.environ["ALLOW_LOCALHOST_DB"] = "1"
        atexit.register(cleanup_ssh_tunnel)
        return True
    except Exception as exc:
        print(f"Warning: Failed to set up SSH tunnel: {exc}", file=sys.stderr)
        print(
            "You may need to manually set up an SSH tunnel:",
            f"ssh -N -L 5432:localhost:5432 {_ssh_config['user']}@{_ssh_config['host']}",
            sep="\n",
            file=sys.stderr,
        )
        return False


def cleanup_ssh_tunnel() -> None:
    """Terminate SSH tunnel if it was created."""
    global _tunnel_process
    if _tunnel_process is not None:
        try:
            _tunnel_process.terminate()
            _tunnel_process.wait(timeout=5)
            print("SSH tunnel closed", file=sys.stderr)
        except Exception as exc:
            print(f"Warning: Failed to close SSH tunnel: {exc}", file=sys.stderr)
        finally:
            _tunnel_process = None


