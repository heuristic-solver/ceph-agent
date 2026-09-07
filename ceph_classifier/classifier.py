"""
classifier.py
Master Workflow Classifier (Two-Tier Hybrid Architecture).
Combines:
- Tier 1: Deterministic Multi-Probe Fast-Path (<2ms)
- Tier 2: Local LLM Fallback Oracle (Ollama / Gemma)
"""

import os
from typing import Optional, Dict, Any
from ceph_classifier.models import PayloadMetadata, ClassificationResult, ItemCategory
from ceph_classifier.probes.magic_probe import MagicProbe
from ceph_classifier.probes.fs_probe import FSProbe
from ceph_classifier.probes.context_probe import ContextProbe
from ceph_classifier.llm_oracle import LLMOracle

DEFAULT_RBD_POOL = "joel"
DEFAULT_S3_BUCKET = "s3://default-bucket/"
DEFAULT_CEPHFS_MOUNT = "/mnt/cephfs/"
DEFAULT_RADOS_POOL = "test_data_pool"

class WorkflowClassifier:
    def __init__(self, enable_llm_fallback: bool = True):
        self.magic_probe = MagicProbe()
        self.fs_probe = FSProbe()
        self.context_probe = ContextProbe()
        self.llm_oracle = LLMOracle() if enable_llm_fallback else None

    def extract_metadata(self, item_path: str) -> PayloadMetadata:
        """Extracts complete structural, filesystem, and binary metadata for a payload."""
        fs_info = self.fs_probe.inspect(item_path)
        is_dir = fs_info["is_directory"]
        
        if is_dir:
            magic_sig = "DIRECTORY_TREE"
            mime_type = "inode/directory"
            sample_hex = ""
            ext = ""
        else:
            magic_info = self.magic_probe.inspect(item_path)
            magic_sig = magic_info["magic_name"]
            mime_type = magic_info["mime_type"]
            sample_hex = magic_info["sample_hex"]
            ext = os.path.splitext(item_path)[1].lower()

        return PayloadMetadata(
            path=os.path.abspath(item_path),
            is_directory=is_dir,
            size_bytes=fs_info["size_bytes"],
            size_mb=fs_info["size_mb"],
            mime_type=mime_type,
            extension=ext,
            magic_signature=magic_sig,
            file_count=fs_info["file_count"],
            max_depth=fs_info["max_depth"],
            has_project_markers=fs_info["has_project_markers"],
            detected_markers=fs_info["detected_markers"],
            sample_header_hex=sample_hex
        )

    def classify(self, item_path: str, user_intent: Optional[str] = None) -> ClassificationResult:
        """
        Classifies an input item and outputs the authoritative Ceph routing decision.
        Evaluates Tier 1 deterministic probes first; escalates to Tier 2 LLM if ambiguous or intent is given.
        """
        if not os.path.exists(item_path):
            raise FileNotFoundError(f"Path does not exist: {item_path}")

        meta = self.extract_metadata(item_path)
        item_name = os.path.basename(os.path.normpath(item_path))
        if not item_name or item_name == ".":
            item_name = "workspace"
        
        # 1. Evaluate Context Probe for specific intent / ambiguities
        magic_info = self.magic_probe.inspect(item_path) if not meta.is_directory else {"magic_name": "DIRECTORY_TREE"}
        fs_info = {"is_directory": meta.is_directory, "max_depth": meta.max_depth, "has_project_markers": meta.has_project_markers}
        ctx_eval = self.context_probe.evaluate(item_path, magic_info, fs_info, user_intent=user_intent)

        # ── TIER 1: DETERMINISTIC CLASSIFICATION ─────────────────────
        
        # Rule A: Virtual Disk Images -> RBD
        if meta.magic_signature in ["QCOW2", "ISO9660", "VMDK", "VDI", "VHDX", "VHD"] or meta.magic_signature.startswith("DISK_EXT_"):
            return ClassificationResult(
                item_path=meta.path,
                item_type="virtual_disk_image",
                target_workflow="RBD",
                confidence=0.98,
                rationale=f"Binary magic signature `{meta.magic_signature}` detected. Block storage format best suited for thin-provisioned RBD volume with copy-on-write capabilities.",
                target_destination=f"{DEFAULT_RBD_POOL}/{item_name}",
                tuning_parameters={
                    "image_format": 2,
                    "features": ["layering", "exclusive-lock", "object-map", "fast-diff"],
                    "stripe_unit": "4MB",
                    "target_pool": DEFAULT_RBD_POOL
                },
                decision_tier="tier1_deterministic",
                metadata=meta
            )

        # Rule B: Directories -> CephFS
        if meta.is_directory:
            markers_str = f" with POSIX markers {meta.detected_markers}" if meta.detected_markers else ""
            return ClassificationResult(
                item_path=meta.path,
                item_type="hierarchical_directory",
                target_workflow="CephFS",
                confidence=0.96,
                rationale=f"Directory structure ({meta.file_count} files, max depth {meta.max_depth}){markers_str}. Best mounted as a distributed POSIX filesystem preserving permissions and directory hierarchy.",
                target_destination=f"{DEFAULT_CEPHFS_MOUNT.rstrip('/')}/{item_name}",
                tuning_parameters={
                    "preserve_posix_permissions": True,
                    "preserve_xattrs": True,
                    "mount_point": DEFAULT_CEPHFS_MOUNT
                },
                decision_tier="tier1_deterministic",
                metadata=meta
            )

        # Rule C: Context Probe Explicit Decision (e.g. SQLite with active write intent, or Model weights)
        if ctx_eval.get("recommended_workflow") and not ctx_eval.get("is_ambiguous"):
            target_wf = ctx_eval["recommended_workflow"]
            dest = f"{DEFAULT_RBD_POOL}/{item_name}" if target_wf == "RBD" else (f"{DEFAULT_CEPHFS_MOUNT.rstrip('/')}/{item_name}" if target_wf == "CephFS" else f"{DEFAULT_S3_BUCKET}{item_name}")
            return ClassificationResult(
                item_path=meta.path,
                item_type="relational_database" if "SQLite" in meta.magic_signature else "archive_bundle",
                target_workflow=target_wf,
                confidence=ctx_eval["confidence"],
                rationale=ctx_eval["rationale"],
                target_destination=dest,
                tuning_parameters=ctx_eval.get("tuning", {}),
                decision_tier="tier1_deterministic",
                metadata=meta
            )

        # Rule D: Raw Key-Value or OMAP Payloads -> Native RADOS
        if meta.magic_signature == "RADOS_OMAP_PAYLOAD" or meta.extension in [".omap", ".kv", ".rados"]:
            return ClassificationResult(
                item_path=meta.path,
                item_type="raw_key_value_shard",
                target_workflow="RADOS",
                confidence=0.92,
                rationale="Raw key-value payload or direct object shard detected. Direct librados OMAP storage provides minimal latency bypassing block/filesystem protocol layers.",
                target_destination=f"{DEFAULT_RADOS_POOL}/{item_name}",
                tuning_parameters={"pool": DEFAULT_RADOS_POOL, "write_mode": "direct_omap"},
                decision_tier="tier1_deterministic",
                metadata=meta
            )

        # Rule E: Standalone Media, Datasets, Columnar, Archives -> RGW (S3)
        if meta.magic_signature in ["Parquet", "ZIP", "GZIP", "BZIP2", "XZ", "7Z", "PDF", "PNG", "JPEG", "MP4_CONTAINER"] or meta.magic_signature.startswith("MEDIA_EXT_"):
            return ClassificationResult(
                item_path=meta.path,
                item_type="columnar_dataset" if meta.magic_signature == "Parquet" else ("archive_bundle" if "ZIP" in meta.magic_signature or "GZIP" in meta.magic_signature else "flat_media_object"),
                target_workflow="RGW",
                confidence=0.95,
                rationale=f"Immutable object format ({meta.mime_type}, {meta.size_mb} MB) detected. Optimal for Amazon S3 / Ceph RGW REST storage with high throughput multipart streaming.",
                target_destination=f"{DEFAULT_S3_BUCKET}{item_name}",
                tuning_parameters={
                    "storage_class": "STANDARD",
                    "multipart_threshold_mb": 100,
                    "target_bucket": "default-bucket"
                },
                decision_tier="tier1_deterministic",
                metadata=meta
            )

        # ── TIER 2: LLM FALLBACK ORACLE (FOR AMBIGUOUS / USER INTENT CASES) ──
        
        if self.llm_oracle and (user_intent or ctx_eval.get("is_ambiguous")):
            llm_result = self.llm_oracle.query(meta, user_intent=user_intent)
            if llm_result and "target_workflow" in llm_result:
                target_wf = llm_result["target_workflow"]
                dest = f"{DEFAULT_RBD_POOL}/{item_name}" if target_wf == "RBD" else (f"{DEFAULT_CEPHFS_MOUNT.rstrip('/')}/{item_name}" if target_wf == "CephFS" else f"{DEFAULT_S3_BUCKET}{item_name}")
                return ClassificationResult(
                    item_path=meta.path,
                    item_type="generic_stream",
                    target_workflow=target_wf,
                    confidence=float(llm_result.get("confidence", 0.90)),
                    rationale=llm_result.get("rationale", "Classified by Tier 2 LLM Reasoning Oracle based on semantic intent."),
                    target_destination=llm_result.get("target_destination", dest),
                    tuning_parameters=llm_result.get("tuning_parameters", {}),
                    decision_tier="tier2_llm_fallback",
                    metadata=meta
                )

        # ── DEFAULT FALLBACK (SAFE HIGH-CONFIDENCE DEFAULT -> RGW S3) ──
        return ClassificationResult(
            item_path=meta.path,
            item_type="generic_stream",
            target_workflow="RGW",
            confidence=0.88,
            rationale=f"Standalone binary file ({meta.size_mb} MB). Stored as an immutable S3 object in RGW storage.",
            target_destination=f"{DEFAULT_S3_BUCKET}{item_name}",
            tuning_parameters={"storage_class": "STANDARD"},
            decision_tier="tier1_deterministic",
            metadata=meta
        )
