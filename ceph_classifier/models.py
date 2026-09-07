"""
models.py
Data models and contracts for Ceph payload feature extraction and classification.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Literal

WorkflowType = Literal["RBD", "RGW", "CephFS", "RADOS"]
ItemCategory = Literal[
    "virtual_disk_image",
    "hierarchical_directory",
    "flat_media_object",
    "columnar_dataset",
    "archive_bundle",
    "relational_database",
    "raw_key_value_shard",
    "generic_stream"
]

@dataclass
class PayloadMetadata:
    """Structured feature extraction profile of an input payload."""
    path: str
    is_directory: bool
    size_bytes: int
    size_mb: float
    mime_type: str
    extension: str
    magic_signature: str  # e.g., 'QCOW2', 'ISO9660', 'SQLite3', 'Parquet'
    file_count: int
    max_depth: int
    has_project_markers: bool
    detected_markers: List[str] = field(default_factory=list)
    sample_header_hex: str = ""

@dataclass
class ClassificationResult:
    """Authoritative workflow routing decision with confidence and tuning parameters."""
    item_path: str
    item_type: ItemCategory
    target_workflow: WorkflowType  # RBD | RGW | CephFS | RADOS
    confidence: float  # 0.0 to 1.0
    rationale: str
    target_destination: str  # e.g. 'joel', 's3://default-bucket/', '/mnt/cephfs/'
    tuning_parameters: Dict[str, Any] = field(default_factory=dict)
    decision_tier: Literal["tier1_deterministic", "tier2_llm_fallback"] = "tier1_deterministic"
    metadata: Optional[PayloadMetadata] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_path": self.item_path,
            "item_type": self.item_type,
            "target_workflow": self.target_workflow,
            "confidence": self.confidence,
            "rationale": self.rationale,
            "target_destination": self.target_destination,
            "tuning_parameters": self.tuning_parameters,
            "decision_tier": self.decision_tier,
            "metadata": {
                "size_mb": self.metadata.size_mb if self.metadata else 0.0,
                "is_directory": self.metadata.is_directory if self.metadata else False,
                "mime_type": self.metadata.mime_type if self.metadata else "",
                "magic_signature": self.metadata.magic_signature if self.metadata else "",
                "file_count": self.metadata.file_count if self.metadata else 1,
                "max_depth": self.metadata.max_depth if self.metadata else 0,
            } if self.metadata else None
        }
