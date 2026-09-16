"""
ssh_executor.py
Paramiko SSH communication client with sudo escalation, clean error parsing,
timeout guards, and mock simulation support for testing without a live cluster.
"""

import os
import time
import logging
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
        sudo_password: Optional[str] = None
    ):
        self.host = host or os.getenv("VM_SSH_HOST", "127.0.0.1")
        self.port = port or int(os.getenv("VM_SSH_PORT", "2222"))
        self.user = user or os.getenv("VM_SSH_USER", "vboxuser")
        self.password = password or os.getenv("VM_SSH_PASSWORD", "admin")
        self.sudo_password = sudo_password or os.getenv("VM_SUDO_PASSWORD", self.password)
        self._client = None

    def connect(self, timeout: int = 10):
        """Establishes Paramiko SSH client connection."""
        import paramiko
        if self._client is not None:
            return self._client

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            timeout=timeout,
            banner_timeout=timeout,
            auth_timeout=timeout
        )
        self._client = client
        return self._client

    def execute(self, cmd: str, timeout: int = 60) -> ExecutionResult:
        """Executes a command with sudo escalation and returns an ExecutionResult."""
        client = self.connect()
        start_time = time.time()

        full_cmd = f"echo '{self.sudo_password}' | sudo -S bash -c \"{cmd}\""
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
        """
        Uploads a local file or directory tree to the remote Ceph VM.

        For directories: packages into a tar archive, uploads via SFTP, extracts on the
        remote with `tar -xf ... -C /tmp/`, then verifies the extracted directory actually
        exists before returning True.  Without this verification, a failed extraction returns
        True and downstream recipe steps find nothing in /tmp/, silently hitting the
        'else touch' fallback instead of triggering self-healing.
        """
        try:
            if os.path.isfile(local_path):
                return self.upload_file(local_path, remote_path)

            elif os.path.isdir(local_path):
                import tarfile
                import tempfile
                dir_name = os.path.basename(os.path.normpath(local_path))
                tar_name = f"{dir_name}.tar"
                tmp_dir = tempfile.gettempdir()
                local_tar = os.path.join(tmp_dir, tar_name)

                with tarfile.open(local_tar, "w") as tar:
                    tar.add(local_path, arcname=dir_name)

                remote_tar = f"/tmp/{tar_name}"
                remote_dir = f"/tmp/{dir_name}"

                if not self.upload_file(local_tar, remote_tar):
                    logger.warning(f"SFTP upload of tar archive failed: {local_tar} -> {remote_tar}")
                    return False

                # Extract and verify in one command — if tar fails, the test command
                # will also fail and we get a clear non-zero exit code.
                extract_result = self.execute(
                    f"tar -xf {remote_tar} -C /tmp/ && rm -f {remote_tar} && "
                    f"[ -d {remote_dir} ] || {{ echo 'ERROR: extraction produced no directory at {remote_dir}'; exit 1; }}"
                )
                if os.path.exists(local_tar):
                    os.remove(local_tar)

                if not extract_result.is_success:
                    logger.warning(
                        f"Tar extraction failed or directory not found at {remote_dir}: "
                        f"exit={extract_result.exit_code} stderr={extract_result.stderr!r}"
                    )
                    return False

                return True

            return False
        except Exception as e:
            logger.warning(f"Upload path failed for {local_path} -> {remote_path}: {e}")
            return False

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
        """Mock upload path always succeeds."""
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
