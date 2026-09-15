"""
test_agent.py
Unit and integration test suite for Ceph Autonomous Workload Orchestration and Self-Healing Agent.
Tests:
- End-to-end workflow execution across RBD, RGW, CephFS, and RADOS.
- Dynamic fault injection and automated self-healing recovery.
- State machine lifecycle and SQLite trace persistence.
- Iteration budget guardrails and failure containment.
- Regression: RGW bucket creation uses S3 API (boto3), NOT broken `radosgw-admin bucket create`.
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path
from ceph_agent.core.agent import CephSelfHealingAgent
from ceph_agent.core.ssh_executor import MockSSHExecutor
from ceph_agent.core.tracker import ExecutionTracker, TaskState
from ceph_agent.knowledge.retriever import RemediationRetriever
from ceph_classifier.classifier import WorkflowClassifier


class TestCephSelfHealingAgent(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.traces_db = Path(self.temp_dir) / "test_traces.db"
        self.tracker = ExecutionTracker(db_path=self.traces_db)
        self.retriever = RemediationRetriever()
        self.classifier = WorkflowClassifier()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_mock_file(self, filename: str, header_bytes: bytes) -> str:
        filepath = os.path.join(self.temp_dir, filename)
        with open(filepath, "wb") as f:
            f.write(header_bytes)
            f.write(b"\x00" * 512)
        return filepath

    def test_rbd_clean_execution(self):
        """Clean-path: QCOW2 disk image provisions via RBD."""
        qcow2_magic = b"QFI\xfb\x00\x00\x00\x03"
        filepath = self._create_mock_file("ubuntu_server.qcow2", qcow2_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "RBD")
        self.assertEqual(summary.steps_completed, summary.steps_total)
        self.assertEqual(len(summary.healing_actions_applied), 0)

    def test_rgw_clean_execution(self):
        """Clean-path: Parquet columnar dataset provisions via RGW."""
        parquet_magic = b"PAR1"
        filepath = self._create_mock_file("analytics.parquet", parquet_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        # Register a handler that simulates boto3 bucket creation step succeeding
        def rgw_handler(cmd: str):
            if "radosgw-admin user create" in cmd or "user info" in cmd:
                return '{"user_id": "agent_s3_user", "keys": [{"access_key": "TESTKEY", "secret_key": "TESTSECRET"}]}', "", 0
            if "base64 -d" in cmd and "_ceph_s3_upload.py" in cmd:
                # Simulate successful S3 bucket creation and upload
                return "bucket created: analytics_parquet\nupload complete: analytics.parquet -> analytics_parquet", "", 0
            if "radosgw-admin bucket list" in cmd:
                return '["analytics_parquet"]', "", 0
            return "ok", "", 0
        mock_ssh.custom_handler = rgw_handler

        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "RGW")
        self.assertEqual(summary.steps_completed, summary.steps_total)

    def test_cephfs_clean_execution(self):
        """Clean-path: Hierarchical source directory provisions via CephFS."""
        project_dir = os.path.join(self.temp_dir, "my_app_project")
        os.makedirs(os.path.join(project_dir, ".git"), exist_ok=True)
        with open(os.path.join(project_dir, "main.py"), "w") as f:
            f.write("print('hello cephfs')")

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=project_dir)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "CephFS")
        self.assertEqual(summary.steps_completed, summary.steps_total)

    def test_rados_clean_execution(self):
        """Clean-path: Raw shard provisions via RADOS."""
        omap_magic = b"OMAP_DB_V1"
        filepath = self._create_mock_file("metadata.omap", omap_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "RADOS")
        self.assertEqual(summary.steps_completed, summary.steps_total)

    def test_self_healing_rbd_pool_uninitialized_fault(self):
        """
        Fault Injection: 'rbd create' fails because pool lacks application tag (POOL_APP_NOT_ENABLED).
        The agent diagnoses 'pool not initialized for rbd', applies 'rbd pool init',
        and autonomously retries to success.
        """
        qcow2_magic = b"QFI\xfb\x00\x00\x00\x03"
        filepath = self._create_mock_file("database_vol.qcow2", qcow2_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        pool_initialized = False
        first_create_attempt = True

        def dynamic_handler(cmd: str):
            nonlocal pool_initialized, first_create_attempt
            if "rbd pool init" in cmd or "pool_application_enable" in cmd or "pool init" in cmd:
                pool_initialized = True
                return "pool initialized for rbd", "", 0
            if "rbd create" in cmd and first_create_attempt:
                first_create_attempt = False
                return "", "rbd: error opening pool 'rbd': Pool not initialized for rbd. POOL_APP_NOT_ENABLED", 2
            return "ok", "", 0

        mock_ssh.custom_handler = dynamic_handler

        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "RBD")
        self.assertGreater(len(summary.healing_actions_applied), 0)
        self.assertTrue(pool_initialized)

    def test_self_healing_rgw_missing_user_fault(self):
        """
        Fault Injection: 'radosgw-admin bucket list' fails with NoSuchUser.
        The agent diagnoses the missing user, creates credentials via radosgw-admin,
        and retries to success.
        """
        parquet_magic = b"PAR1"
        filepath = self._create_mock_file("events.parquet", parquet_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        user_created = False
        first_bucket_list_attempt = True

        def dynamic_handler(cmd: str):
            nonlocal user_created, first_bucket_list_attempt
            if "radosgw-admin user create" in cmd:
                user_created = True
                return '{"user_id": "agent_s3_user", "keys": [{"access_key": "TESTKEY", "secret_key": "TESTSECRET"}]}', "", 0
            if "base64 -d" in cmd and "_ceph_s3_upload.py" in cmd:
                return "bucket created: events_parquet\nupload complete: events.parquet -> events_parquet", "", 0
            if "radosgw-admin bucket list" in cmd and first_bucket_list_attempt:
                first_bucket_list_attempt = False
                return "", "NoSuchUser: AccessDenied S3 user not found", 1
            return "ok", "", 0

        mock_ssh.custom_handler = dynamic_handler

        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertEqual(summary.target_workflow, "RGW")
        self.assertGreater(len(summary.healing_actions_applied), 0)
        self.assertTrue(user_created)

    def test_rgw_bucket_creation_uses_s3_api_not_radosgw_admin(self):
        """
        Regression Test: Verify that the RGW recipe uses the S3 API (boto3 via python3 -c)
        to create buckets, NOT the broken `radosgw-admin bucket create` command.

        Background: `radosgw-admin bucket create` only re-links existing S3 buckets and silently
        fails when called for a new bucket, masked by `|| true`. This caused the colleague's
        demo to show bucket creation as [OK] while `radosgw-admin bucket list` returned [].
        """
        parquet_magic = b"PAR1"
        filepath = self._create_mock_file("hierarchy.png", b"\x89PNG\r\n")

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        s3_api_call_made = False
        radosgw_bucket_create_called = False

        def audit_handler(cmd: str):
            nonlocal s3_api_call_made, radosgw_bucket_create_called
            # Check for the broken old command
            if "radosgw-admin bucket create" in cmd:
                radosgw_bucket_create_called = True
            # Check for the correct new S3 API approach (base64-decoded script)
            if "base64 -d" in cmd and "_ceph_s3_upload.py" in cmd:
                s3_api_call_made = True
                return "bucket created: hierarchy_png\nupload complete: hierarchy.png -> hierarchy_png", "", 0
            if "radosgw-admin user" in cmd:
                return '{"user_id": "agent_s3_user", "keys": [{"access_key": "TESTKEY", "secret_key": "TESTSECRET"}]}', "", 0
            if "radosgw-admin bucket list" in cmd:
                return '["hierarchy_png"]', "", 0
            return "ok", "", 0

        mock_ssh.custom_handler = audit_handler

        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        self.assertEqual(summary.status, "SUCCESS")
        self.assertTrue(s3_api_call_made, "RGW recipe must call the S3 API (boto3) to create buckets")
        self.assertFalse(radosgw_bucket_create_called, 
                         "RGW recipe must NOT use `radosgw-admin bucket create` — it silently fails on Reef/Squid")

    def test_max_retry_budget_exhaustion(self):
        """
        Safety Guardrail: Persistent fatal error halts after exceeding max_iterations.
        """
        qcow2_magic = b"QFI\xfb\x00\x00\x00\x03"
        filepath = self._create_mock_file("corrupted.qcow2", qcow2_magic)

        # Persistent failure on all commands
        mock_ssh = MockSSHExecutor(default_exit_code=1)
        mock_ssh.register_response("ceph -s", stdout="HEALTH_ERR", stderr="Cluster quorum lost", exit_code=1)
        mock_ssh.register_response("ceph osd pool create", stdout="", stderr="Cluster quorum lost", exit_code=1)

        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath, max_iterations=3)
        self.assertEqual(summary.status, "FAILED")
        self.assertEqual(summary.iteration_count, 3)
        self.assertIn("Exceeded maximum self-healing retry limit", summary.final_message)

    def test_state_machine_tracker_persistence(self):
        """Verify SQLite trace logs persist state transitions, steps, and healing history."""
        parquet_magic = b"PAR1"
        filepath = self._create_mock_file("telemetry.parquet", parquet_magic)

        mock_ssh = MockSSHExecutor(default_exit_code=0)
        agent = CephSelfHealingAgent(
            classifier=self.classifier,
            retriever=self.retriever,
            executor=mock_ssh,
            tracker=self.tracker
        )

        summary = agent.run(payload_path=filepath)
        task_summary = self.tracker.get_task_summary(summary.task_id)
        self.assertIsNotNone(task_summary)
        self.assertEqual(task_summary.state, TaskState.SUCCESS.value)
        self.assertEqual(task_summary.workflow, "RGW")
        self.assertGreater(task_summary.steps_executed, 0)


if __name__ == "__main__":
    unittest.main()
