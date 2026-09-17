"""
context_probe.py
Contextual and access pattern heuristic evaluator for disambiguating multi-use payloads.
"""

from typing import Dict, Any, Optional

class ContextProbe:
    """Evaluates contextual hints, user intent strings, and access pattern requirements."""
    
    def evaluate(self, item_path: str, magic_info: Dict[str, Any], fs_info: Dict[str, Any], user_intent: Optional[str] = None) -> Dict[str, Any]:
        intent_lower = (user_intent or "").lower()
        magic_name = magic_info.get("magic_name", "")
        is_dir = fs_info.get("is_directory", False)
        
        # 1. Evaluate SQLite / Relational DBs
        if "SQLite" in magic_name or item_path.endswith((".db", ".sqlite", ".sqlite3", ".duckdb")):
            if any(k in intent_lower for k in ["transaction", "write", "high iops", "active", "db server", "exclusive"]):
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "RBD",
                    "confidence": 0.94,
                    "rationale": "Active transactional database requires exclusive-locking block I/O with low random-write latency.",
                    "tuning": {"image_format": 2, "features": ["layering", "exclusive-lock"]}
                }
            elif any(k in intent_lower for k in ["archive", "backup", "cold", "read-only", "export", "s3"]):
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "RGW",
                    "confidence": 0.92,
                    "rationale": "Database file designated as an immutable backup / cold archive object.",
                    "tuning": {"storage_class": "STANDARD"}
                }
            else:
                # Ambiguous default: SQLite single file defaults to RBD if moderate/large, RGW if small
                return {
                    "is_ambiguous": True,
                    "recommended_workflow": "RBD",
                    "confidence": 0.78,
                    "rationale": "SQLite database detected without explicit access intent; defaulting to RBD block mapping for POSIX lock safety.",
                    "tuning": {"image_format": 2}
                }

        # 2. Evaluate TAR / GZIP / ZIP Archives
        if magic_name in ["POSIX_TAR", "GZIP", "ZIP", "7Z"] or item_path.endswith((".tar", ".tar.gz", ".tgz", ".zip", ".tar.bz2", ".tar.xz")):
            is_pkg_dir = fs_info.get("is_packaged_directory", False)
            if any(k in intent_lower for k in ["s3", "cold", "backup", "raw object", "immutable", "rgw", "bucket"]):
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "RGW",
                    "confidence": 0.94,
                    "rationale": "Archive designated as an immutable backup / cold object for S3 storage.",
                    "tuning": {"storage_class": "STANDARD"}
                }
            elif is_pkg_dir or any(k in intent_lower for k in ["unpack", "workspace", "codebase", "extract", "shared folder", "cephfs", "posix", "folder", "directory"]):
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "CephFS",
                    "confidence": 0.95,
                    "rationale": "Packaged archive contains a hierarchical directory tree; unpacked into a shared CephFS POSIX filesystem.",
                    "tuning": {"unpack_to_mount": True}
                }
            else:
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "RGW",
                    "confidence": 0.92,
                    "rationale": "Compressed archive container best preserved as an immutable object in S3 storage.",
                    "tuning": {"storage_class": "STANDARD"}
                }

        # 3. Evaluate Machine Learning Checkpoints (.safetensors, .pt, .onnx, .bin)
        if item_path.endswith((".safetensors", ".pt", ".bin", ".onnx", ".ckpt", ".gguf")):
            if any(k in intent_lower for k in ["multi-node", "distributed training", "posix mount"]):
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "CephFS",
                    "confidence": 0.91,
                    "rationale": "Model weights configured for high-concurrency shared POSIX access across training nodes.",
                    "tuning": {"stripe_unit": "4MB"}
                }
            else:
                return {
                    "is_ambiguous": False,
                    "recommended_workflow": "RGW",
                    "confidence": 0.94,
                    "rationale": "Large model checkpoint best stored as a versioned S3 object with multipart streaming.",
                    "tuning": {"multipart_threshold_mb": 100}
                }

        return {
            "is_ambiguous": False,
            "recommended_workflow": None,
            "confidence": 0.0,
            "rationale": "",
            "tuning": {}
        }
