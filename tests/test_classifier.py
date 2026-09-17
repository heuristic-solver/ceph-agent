"""
test_classifier.py
Comprehensive unit test suite for Ceph Ingestion Workflow Classifier (Perception Layer).
Tests all 15+ synthetic payloads and edge cases.
"""

import os
import shutil
import tempfile
import unittest
from ceph_classifier.classifier import WorkflowClassifier

class TestWorkflowClassifier(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.mkdtemp(prefix="ceph_test_payloads_")
        cls.classifier = WorkflowClassifier(enable_llm_fallback=True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.test_dir, ignore_errors=True)

    def _create_file(self, filename: str, content: bytes, seek_offset: int = 0) -> str:
        filepath = os.path.join(self.test_dir, filename)
        with open(filepath, "wb") as f:
            if seek_offset > 0:
                f.seek(seek_offset)
            f.write(content)
        return filepath

    # ── VIRTUAL DISK TESTS (RBD) ───────────────────────────────────

    def test_qcow2_disk_image(self):
        path = self._create_file("ubuntu_server.qcow2", b"QFI\xfb\x00\x00\x00\x03" + b"\x00" * 1024)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.95)
        self.assertEqual(res.item_type, "virtual_disk_image")

    def test_iso9660_disk_image_exact_offset(self):
        # ISO 9660 has CD001 at offset 32768 (0x8000)
        filepath = os.path.join(self.test_dir, "debian_netinst.iso")
        with open(filepath, "wb") as f:
            f.write(b"\x00" * 32768)
            f.write(b"\x01CD001\x01")
            f.write(b"\x00" * 2048)
        res = self.classifier.classify(filepath)
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.95)

    def test_vmdk_disk_image(self):
        path = self._create_file("windows_vm.vmdk", b"KDMV\x00\x00\x00\x01" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.95)

    def test_vdi_disk_image(self):
        path = self._create_file("arch_linux.vdi", b"<<< Oracle VM VirtualBox Disk Image >>>" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.95)

    def test_vhdx_disk_image(self):
        path = self._create_file("server2022.vhdx", b"vhdxfile" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.95)

    # ── DIRECTORY TREE TESTS (CephFS) ──────────────────────────────

    def test_hierarchical_project_directory(self):
        proj_dir = os.path.join(self.test_dir, "my_react_project")
        os.makedirs(os.path.join(proj_dir, "src", "components"), exist_ok=True)
        os.makedirs(os.path.join(proj_dir, ".git"), exist_ok=True)
        
        with open(os.path.join(proj_dir, "package.json"), "w") as f:
            f.write('{"name": "react-app"}')
        with open(os.path.join(proj_dir, "src", "index.js"), "w") as f:
            f.write("console.log('hello');")
            
        res = self.classifier.classify(proj_dir)
        self.assertEqual(res.target_workflow, "CephFS")
        self.assertGreaterEqual(res.confidence, 0.95)
        self.assertEqual(res.item_type, "hierarchical_directory")
        self.assertTrue(res.metadata.has_project_markers)

    # ── STANDALONE OBJECTS & DATASETS (RGW / S3) ───────────────────

    def test_parquet_columnar_dataset(self):
        path = self._create_file("transactions.parquet", b"PAR1\x00\x00\x00\x00" + b"\x00" * 512 + b"PAR1")
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)
        self.assertEqual(res.item_type, "columnar_dataset")

    def test_mp4_video(self):
        path = self._create_file("intro_video.mp4", b"\x00\x00\x00\x20ftypisom\x00\x00\x02\x00" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)

    def test_pdf_document(self):
        path = self._create_file("architecture_spec.pdf", b"%PDF-1.7\n%abc\n" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)

    def test_png_image(self):
        path = self._create_file("cluster_diagram.png", b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + b"\x00" * 512)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)

    def test_posix_tar_archive(self):
        # TAR header with ustar at offset 257
        filepath = os.path.join(self.test_dir, "dataset_archive.tar")
        with open(filepath, "wb") as f:
            f.write(b"a" * 257)
            f.write(b"ustar\x0000")
            f.write(b"\x00" * 512)
        res = self.classifier.classify(filepath)
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)

    # ── NATIVE RADOS KEY-VALUE TESTS ───────────────────────────────

    def test_rados_omap_payload(self):
        path = self._create_file("bucket_index_shard_01.omap", b"CEPH_RADOS_OMAP_KEY_VAL_BUFFER" + b"\x00" * 128)
        res = self.classifier.classify(path)
        self.assertEqual(res.target_workflow, "RADOS")
        self.assertGreaterEqual(res.confidence, 0.90)
        self.assertEqual(res.item_type, "raw_key_value_shard")

    # ── PACKAGED DIRECTORY ARCHIVE TESTS (CephFS) ─────────────────

    def test_zip_packaged_codebase_archive(self):
        import zipfile
        zip_path = os.path.join(self.test_dir, "web_app_bundle.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("src/main.py", "print('hello from app')")
            zf.writestr("src/utils.py", "def helper(): pass")
            zf.writestr("pyproject.toml", "[tool.poetry]\nname = 'web-app'")
        
        res = self.classifier.classify(zip_path)
        self.assertEqual(res.target_workflow, "CephFS")
        self.assertGreaterEqual(res.confidence, 0.95)
        self.assertEqual(res.item_type, "hierarchical_directory")
        self.assertTrue(res.metadata.is_packaged_directory)
        self.assertTrue(res.metadata.has_project_markers)
        self.assertTrue(res.tuning_parameters.get("unpack_to_mount"))

    def test_tar_gz_packaged_codebase_archive(self):
        import tarfile, io
        tar_path = os.path.join(self.test_dir, "service_module.tar.gz")
        with tarfile.open(tar_path, "w:gz") as tf:
            for name, content in [("server.js", b"console.log(1)"), ("package.json", b'{"name":"srv"}')]:
                ti = tarfile.TarInfo(name=name)
                ti.size = len(content)
                tf.addfile(ti, io.BytesIO(content))

        res = self.classifier.classify(tar_path)
        self.assertEqual(res.target_workflow, "CephFS")
        self.assertGreaterEqual(res.confidence, 0.95)
        self.assertTrue(res.metadata.is_packaged_directory)
        self.assertTrue(res.metadata.has_project_markers)

    # ── CONTEXTUAL INTENT DISAMBIGUATION TESTS ─────────────────────

    def test_sqlite_transactional_intent(self):
        path = self._create_file("orders.db", b"SQLite format 3\x00" + b"\x00" * 512)
        res = self.classifier.classify(path, user_intent="high-frequency transactional writes from backend")
        self.assertEqual(res.target_workflow, "RBD")
        self.assertGreaterEqual(res.confidence, 0.90)

    def test_sqlite_cold_archive_intent(self):
        path = self._create_file("orders_2023_backup.db", b"SQLite format 3\x00" + b"\x00" * 512)
        res = self.classifier.classify(path, user_intent="cold analytical archive backup to s3")
        self.assertEqual(res.target_workflow, "RGW")
        self.assertGreaterEqual(res.confidence, 0.90)

    def test_tar_shared_workspace_intent(self):
        filepath = os.path.join(self.test_dir, "linux_kernel_source.tar.gz")
        with open(filepath, "wb") as f:
            f.write(b"\x1f\x8b\x08\x00" + b"\x00" * 512)
        res = self.classifier.classify(filepath, user_intent="unpack into a shared workspace for team development")
        self.assertEqual(res.target_workflow, "CephFS")
        self.assertGreaterEqual(res.confidence, 0.90)


if __name__ == "__main__":
    unittest.main(verbosity=2)
