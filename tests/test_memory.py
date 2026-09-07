"""
test_memory.py
Unit tests for WorkingMemory, EpisodicMemory, and Memory-Augmented Remediation.
"""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ceph_agent.core.memory import WorkingMemory, EpisodicMemory
from ceph_agent.knowledge.retriever import RemediationRetriever
from ceph_agent.knowledge.schema import QueryContext


class TestAgentMemory(unittest.TestCase):
    """Verifies WorkingMemory and EpisodicMemory functionality."""

    def test_working_memory_slot_filling(self):
        """Verify dynamic slot-filling parameter binder."""
        wm = WorkingMemory()
        wm.set_slot("pool", "production_rbd_pool")
        wm.set_slot("user", "s3_admin_user")
        wm.set_slot("daemon", "osd.0")

        # Test various template styles
        cmd1 = wm.bind_slots("ceph osd pool set-quota {pool} max_bytes 104857600")
        self.assertEqual(cmd1, "ceph osd pool set-quota production_rbd_pool max_bytes 104857600")

        cmd2 = wm.bind_slots("radosgw-admin user info --uid=<user>")
        self.assertEqual(cmd2, "radosgw-admin user info --uid=s3_admin_user")

        cmd3 = wm.bind_slots("ceph orch daemon restart <daemon-name>")
        self.assertEqual(cmd3, "ceph orch daemon restart osd.0")

    def test_working_memory_inferred_facts_and_negative_history(self):
        """Verify belief state inference and failed command tracking."""
        wm = WorkingMemory()
        step_name = "ensure_s3_user"

        # Record failed attempt
        wm.record_attempt(
            step_name=step_name,
            attempt_idx=1,
            command="radosgw-admin user create --uid=test_user",
            exit_code=17,
            stdout="",
            stderr="could not create user: user: test_user already exists"
        )

        # Assert inferred facts
        self.assertTrue(wm.get_fact("user_already_exists"))
        self.assertTrue(wm.has_failed_previously(step_name, "radosgw-admin user create --uid=test_user"))
        self.assertFalse(wm.has_failed_previously(step_name, "radosgw-admin user info --uid=test_user"))

        # Verify failed commands set
        failed = wm.get_failed_commands(step_name)
        self.assertIn("radosgw-admin user create --uid=test_user", failed)

    def test_episodic_memory_recall(self):
        """Verify cross-task recall of winning remediations from SQLite traces."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tf:
            db_path = Path(tf.name)

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE tasks (
                    task_id TEXT PRIMARY KEY,
                    state TEXT
                );
            """)
            cursor.execute("""
                CREATE TABLE healing_traces (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT,
                    error_context TEXT,
                    remediation_command TEXT,
                    danger_level TEXT,
                    rationale TEXT,
                    confidence REAL,
                    applied INTEGER
                );
            """)
            # Insert historical task and successful healing
            cursor.execute("INSERT INTO tasks VALUES ('task_success_001', 'SUCCESS')")
            cursor.execute("""
                INSERT INTO healing_traces (task_id, error_context, remediation_command, danger_level, rationale, confidence, applied)
                VALUES ('task_success_001', 'could not create user: user: agent_user exists', 'radosgw-admin user info --uid=agent_user', 'read-only', 'User already exists, fetch metadata', 0.95, 1)
            """)
            conn.commit()
            conn.close()

            episodic = EpisodicMemory(db_path=db_path)
            proposal = episodic.recall_winning_remediation(
                error_signature="could not create user: unable to parse parameters, user: agent_user exists"
            )

            self.assertIsNotNone(proposal)
            self.assertEqual(proposal.fix_command, "radosgw-admin user info --uid=agent_user")
            self.assertEqual(proposal.confidence, 0.95)
            self.assertIn("Episodic Memory", proposal.rationale)

        finally:
            if db_path.exists():
                os.unlink(db_path)

    def test_retriever_negative_filtering_with_working_memory(self):
        """Verify retriever skips failed commands and chooses next alternative."""
        retriever = RemediationRetriever()
        wm = WorkingMemory()
        wm.set_slot("pool", "test_quota_pool")

        ctx = QueryContext(
            stderr="[errno 122] Disk quota exceeded",
            failed_command="rados -p test_quota_pool put obj1 /tmp/file"
        )

        # Attempt 1: Should propose pool set-quota with slot filled
        prop1 = retriever.query(ctx, working_memory=wm)
        self.assertIn("test_quota_pool", prop1.fix_command)
        self.assertIn("set-quota", prop1.fix_command)

        # Simulate that this command was tried and failed
        wm.record_attempt(
            step_name="rados -p test_quota_pool put obj1 /tmp/file",
            attempt_idx=1,
            command=prop1.fix_command,
            exit_code=1,
            stdout="",
            stderr="Disk quota still exceeded"
        )

        # Attempt 2: Proposing again must NOT return the exact failed command
        prop2 = retriever.query(ctx, working_memory=wm)
        self.assertNotEqual(prop2.fix_command, prop1.fix_command)


if __name__ == "__main__":
    unittest.main()
