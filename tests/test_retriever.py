"""
test_retriever.py
Unit and integration test suite for Ceph Self-Healing Remediation Retriever.
Validates LLM-based diagnosis, FTS5 semantic search, and structured proposals across all workflows.
"""

import unittest
from pathlib import Path
from ceph_agent.knowledge.indexer import KnowledgeIndexer
from ceph_agent.knowledge.retriever import RemediationRetriever
from ceph_agent.knowledge.schema import QueryContext, RemediationProposal


class TestRemediationRetriever(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Ensure database is indexed
        cls.indexer = KnowledgeIndexer()
        cls.indexer.index_all()
        cls.retriever = RemediationRetriever()

    def test_database_populated(self):
        """Verify that knowledge store contains all expected assets."""
        conn = self.retriever._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT count(*) FROM doc_chunks;")
        chunk_count = cursor.fetchone()[0]
        self.assertEqual(chunk_count, 1610)

        cursor.execute("SELECT count(*) FROM health_checks;")
        health_count = cursor.fetchone()[0]
        self.assertEqual(health_count, 99)

        cursor.execute("SELECT count(*) FROM command_specs;")
        spec_count = cursor.fetchone()[0]
        self.assertEqual(spec_count, 284)
        conn.close()

    def test_rbd_pool_uninitialized_diagnosis(self):
        """Scenario: RBD pool exists but application tag is missing."""
        ctx = QueryContext(
            stderr="rbd: error opening default pool 'datapool': (2) No such file or directory. Pool not initialized for rbd.",
            exit_code=2,
            workflow="RBD",
            target_destination="datapool",
            failed_command="rbd create datapool/disk01 --size 10G"
        )
        proposal = self.retriever.query(ctx)
        self.assertIsInstance(proposal, RemediationProposal)
        self.assertIn("rbd", proposal.fix_command.lower())
        self.assertTrue("pool" in proposal.fix_command.lower() or "init" in proposal.fix_command.lower())
        self.assertIn(proposal.danger_level, ["read-only", "moderate"])
        self.assertGreaterEqual(proposal.confidence, 0.70)

    def test_cephfs_mds_down_diagnosis(self):
        """Scenario: CephFS mount fails because MDS daemon is offline."""
        ctx = QueryContext(
            stderr="mount error 5 = Input/output error. MDS_ALL_DOWN: no active filesystem",
            exit_code=5,
            workflow="CephFS",
            target_destination="/mnt/cephfs",
            cluster_health="HEALTH_WARN 1 MDSs down; MDS_ALL_DOWN"
        )
        proposal = self.retriever.query(ctx)
        self.assertIsInstance(proposal, RemediationProposal)
        self.assertTrue(
            "mds" in proposal.fix_command.lower() or
            "fs" in proposal.fix_command.lower() or
            "orch" in proposal.fix_command.lower()
        )
        self.assertGreaterEqual(proposal.confidence, 0.70)

    def test_rgw_access_denied_diagnosis(self):
        """Scenario: S3 / RGW client gets AccessDenied due to missing user."""
        ctx = QueryContext(
            stderr="botocore.exceptions.ClientError: An error occurred (AccessDenied) when calling the PutObject operation: NoSuchUser",
            exit_code=1,
            workflow="RGW",
            target_destination="mybucket",
            failed_command="boto3.client('s3').put_object(Bucket='mybucket')"
        )
        proposal = self.retriever.query(ctx)
        self.assertIsInstance(proposal, RemediationProposal)
        self.assertTrue(
            "radosgw-admin" in proposal.fix_command.lower() or
            "user" in proposal.fix_command.lower() or
            "rgw" in proposal.fix_command.lower()
        )
        self.assertGreaterEqual(proposal.confidence, 0.70)

    def test_osd_down_diagnosis(self):
        """Scenario: Cluster alerts OSD_DOWN with degraded placement groups."""
        ctx = QueryContext(
            stderr="Health check failed: OSD_DOWN 1 osds down. PG_DEGRADED Degraded data redundancy: 12 pgs degraded",
            exit_code=1,
            workflow="CLUSTER_OPS",
            cluster_health="HEALTH_WARN 1 osds down"
        )
        proposal = self.retriever.query(ctx)
        self.assertIsInstance(proposal, RemediationProposal)
        self.assertTrue(
            "osd" in proposal.fix_command.lower() or
            "tree" in proposal.fix_command.lower() or
            "health" in proposal.fix_command.lower()
        )

    def test_search_health_checks(self):
        """Verify health check search returns structured details with resolution commands."""
        results = self.retriever.search_health_checks("MON_CLOCK_SKEW", limit=3)
        self.assertGreater(len(results), 0)
        top = results[0]
        self.assertEqual(top["code"], "MON_CLOCK_SKEW")
        self.assertIsInstance(top["resolution_commands"], list)
        self.assertGreater(len(top["resolution_commands"]), 0)

    def test_search_command_specs(self):
        """Verify command specs retrieval matches subsystem and workflow."""
        results = self.retriever.search_command_specs("radosgw-admin user create", workflow="RGW", limit=5)
        self.assertGreater(len(results), 0)
        found_rgw = any("radosgw-admin" in r["command"] for r in results)
        self.assertTrue(found_rgw)

    def test_search_doc_chunks(self):
        """Verify multi-version documentation search across releases."""
        results = self.retriever.search_doc_chunks("bluestore compression", release="quincy", limit=3)
        self.assertGreater(len(results), 0)
        self.assertEqual(results[0]["ceph_release"], "quincy")


if __name__ == "__main__":
    unittest.main()
