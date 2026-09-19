"""
ssh_executor.py
Paramiko SSH communication client with sudo escalation, clean error parsing,
timeout guards, and mock simulation support for testing without a live cluster.
"""

import os
import time
import logging
import shlex
from pathlib import Path
from dataclasses import dataclass
from typing import Tuple, Optional, Dict, Any, Callable
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()


@dataclass
class ExecutionResult:
    """Structured result from an executed command."""
    command: str
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int

    @property
    def is_success(self) -> bool:
        return self.exit_code == 0


class SSHExecutor:
    """Manages SSH connections and remote command execution on the Ceph host/VM."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        sudo_password: Optional[str] = None,
        key_path: Optional[str] = None
    ):
        self.host = host or os.getenv("VM_SSH_HOST", "127.0.0.1")
        self.port = port or int(os.getenv("VM_SSH_PORT", "2222"))
        self.user = user or os.getenv("VM_SSH_USER", "vboxuser")

        # Key-based auth takes precedence over password auth. When
        # CEPH_AI_SSH_KEY_PATH is set (or key_path is passed explicitly), the
        # connection uses the private key and no plaintext SSH password is
        # required. Password fields remain available as an explicit fallback
        # for environments that still rely on them.
        self.key_path = key_path or os.getenv("CEPH_AI_SSH_KEY_PATH") or None
        self.password = password or os.getenv("VM_SSH_PASSWORD") or None

        # Passwordless sudo (NOPASSWD) is assumed when key-based auth is in
        # use, so no sudo password is required in that case. sudo_password /
        # VM_SUDO_PASSWORD remain available for environments that still need
        # to pipe a sudo password (e.g. password-auth fallback).
        self.sudo_password = sudo_password or os.getenv("VM_SUDO_PASSWORD") or None
        self.use_passwordless_sudo = bool(self.key_path) and not self.sudo_password

        self._client = None

    def connect(self, timeout: int = 10):
        """Establishes Paramiko SSH client connection.

        Prefers key-based authentication (CEPH_AI_SSH_KEY_PATH) over
        plaintext password authentication. Falls back to password auth only
        if no key path is configured, preserving compatibility with
        environments that haven't migrated to key-based auth yet.
        """
        import paramiko
        if self._client is not None:
            return self._client

        if not self.key_path and not self.password:
            raise RuntimeError(
                "No SSH credentials configured. Set CEPH_AI_SSH_KEY_PATH "
                "(preferred) or VM_SSH_PASSWORD."
            )

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        connect_kwargs: Dict[str, Any] = dict(
            hostname=self.host,
            port=self.port,
            username=self.user,
            timeout=timeout,
            banner_timeout=timeout,
            auth_timeout=timeout,
        )

        if self.key_path:
            expanded_key_path = os.path.expanduser(self.key_path)
            if not os.path.isfile(expanded_key_path):
                raise RuntimeError(
                    f"CEPH_AI_SSH_KEY_PATH is set but no key file was found "
                    f"at '{expanded_key_path}'."
                )
            connect_kwargs["key_filename"] = expanded_key_path
            # Still pass password (if any) as a fallback for encrypted keys
            # that need a passphrase, or as a secondary auth method.
            if self.password:
                connect_kwargs["password"] = self.password
        else:
            connect_kwargs["password"] = self.password

        client.connect(**connect_kwargs)
        self._client = client
        return self._client

    def execute(self, cmd: str, timeout: int = 60) -> ExecutionResult:
        """Executes a command with sudo escalation and returns an ExecutionResult."""
        client = self.connect()
        start_time = time.time()

        if self.sudo_password:
            full_cmd = (
                f"echo {shlex.quote(self.sudo_password)} | "
                f"sudo -S bash -c {shlex.quote(cmd)}"
            )
        else:
            full_cmd = f"sudo -n bash -c {shlex.quote(cmd)}"
            
        stdin, stdout, stderr = client.exec_command(full_cmd, timeout=timeout)

        out_raw = stdout.read().decode("utf-8", errors="replace").strip()
        err_raw = stderr.read().decode("utf-8", errors="replace").strip()

        # Clean out sudo password prompt prefix without dropping actual stderr content
        import re
        err_clean = re.sub(r"\[sudo\] password for [^:]+:\s*", "", err_raw, flags=re.IGNORECASE).strip()

        exit_code = stdout.channel.recv_exit_status()
        duration_ms = int((time.time() - start_time) * 1000)

        return ExecutionResult(
            command=cmd,
            stdout=out_raw,
            stderr=err_clean,
            exit_code=exit_code,
            duration_ms=duration_ms
        )

    def test_connectivity(self, timeout: int = 10) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validates SSH connectivity, sudo privileges, and Ceph cluster presence.
        Returns (is_ready, message, details).
        """
        details = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "ceph_version": None,
            "cluster_health": None,
        }
        try:
            self.connect(timeout=timeout)
        except Exception as e:
            return False, f"Cannot connect to SSH host {self.host}:{self.port} ({e})", details

        try:
            # Check whoami and sudo
            res = self.execute("whoami", timeout=timeout)
            if not res.is_success or "root" not in res.stdout:
                return False, f"Sudo escalation failed (exit {res.exit_code}): {res.stderr}", details

            # Check Ceph CLI
            res_ver = self.execute("ceph --version", timeout=timeout)
            if res_ver.is_success:
                details["ceph_version"] = res_ver.stdout.strip()
            else:
                return False, f"Ceph CLI is not installed or not in PATH on remote host: {res_ver.stderr}", details

            # Check Ceph cluster status
            res_stat = self.execute("ceph health || ceph -s", timeout=timeout)
            if res_stat.is_success:
                details["cluster_health"] = res_stat.stdout.splitlines()[0].strip() if res_stat.stdout else "ACTIVE"

            return True, f"Connected to {self.user}@{self.host}:{self.port}. Ceph: {details['ceph_version']}", details
        except Exception as e:
            return False, f"Preflight check error: {e}", details

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Uploads a local file to the remote Ceph VM using SFTP."""
        try:
            client = self.connect()
            sftp = client.open_sftp()
            sftp.put(local_path, remote_path)
            sftp.close()
            return True
        except Exception as e:
            logger.warning(f"SFTP upload failed for {local_path} -> {remote_path}: {e}")
            return False

    def upload_path(self, local_path: str, remote_path: str) -> bool:
        """Upload a local file or directory to the remote Ceph VM using SFTP."""
        try:
            local = Path(local_path)

            if not local.exists():
                logger.warning(f"Local payload does not exist: {local_path}")
                return False

            client = self.connect()
            sftp = client.open_sftp()

            if local.is_file():
                sftp.put(str(local), remote_path)
            elif local.is_dir():
                self._upload_directory(sftp, local, remote_path)
            else:
                logger.warning(f"Unsupported payload type: {local_path}")
                sftp.close()
                return False

            sftp.close()
            return True

        except Exception as e:
            logger.warning(
                f"SFTP path upload failed for {local_path} -> {remote_path}: {e}"
            )
            return False


    def _upload_directory(self, sftp, local_dir: Path, remote_dir: str):
        """Recursively upload a local directory through SFTP."""
        try:
            sftp.mkdir(remote_dir)
        except IOError:
            # Directory may already exist.
            pass

        for item in local_dir.iterdir():
            remote_item = f"{remote_dir.rstrip('/')}/{item.name}"

            if item.is_dir():
                self._upload_directory(sftp, item, remote_item)
            else:
                sftp.put(str(item), remote_item)

    def close(self):
        """Closes the active SSH connection."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None


class MockSSHExecutor(SSHExecutor):
    """Offline mock executor for dry-run simulations and automated unit testing."""

    def __init__(self, default_exit_code: int = 0):
        super().__init__()
        self.default_exit_code = default_exit_code
        self.command_history: list = []
        self.responses: Dict[str, Tuple[str, str, int]] = {}
        self.custom_handler: Optional[Callable[[str], Tuple[str, str, int]]] = None

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Mock upload always succeeds."""
        return True

    def upload_path(self, local_path: str, remote_path: str) -> bool:
        """Mock upload always succeeds."""
        return True

    def test_connectivity(self, timeout: int = 10) -> Tuple[bool, str, Dict[str, Any]]:
        """Mock connectivity check always reports ready."""
        return True, "Connected to Mock SSH Environment (Simulation)", {
            "host": "mock_host",
            "port": 2222,
            "user": "mock_user",
            "ceph_version": "ceph version 17.2.8 (quincy) mock",
            "cluster_health": "HEALTH_OK"
        }

    def register_response(self, command_pattern: str, stdout: str = "", stderr: str = "", exit_code: int = 0):
        """Registers a predefined output for any command containing command_pattern."""
        self.responses[command_pattern] = (stdout, stderr, exit_code)

    def execute(self, cmd: str, timeout: int = 60) -> ExecutionResult:
        self.command_history.append(cmd)
        start = time.time()

        if self.custom_handler:
            out, err, code = self.custom_handler(cmd)
        else:
            # Check registered responses
            matched = False
            out, err, code = "", "", self.default_exit_code
            for pattern, resp in self.responses.items():
                if pattern in cmd:
                    out, err, code = resp
                    matched = True
                    break

            if not matched:
                if "ceph -s" in cmd:
                    out = "cluster: health: HEALTH_OK"
                    code = 0
                elif "radosgw-admin user create" in cmd or "user info" in cmd:
                    out = '{"user_id": "agent_s3_user", "keys": [{"access_key": "MOCK", "secret_key": "MOCK"}]}'
                    code = 0
                elif "mount" in cmd or "modprobe" in cmd or "mkdir" in cmd:
                    code = 0
                elif "rbd" in cmd or "rados" in cmd:
                    code = 0

        duration_ms = int((time.time() - start) * 1000)
        return ExecutionResult(
            command=cmd,
            stdout=out,
            stderr=err,
            exit_code=code,
            duration_ms=duration_ms
        )

    def connect(self, timeout: int = 10):
        return self

    def close(self):
        pass