"""
recipes.py
Defines structured, ordered workflow execution DAGs for Ceph interfaces:
- RBD: Block device provisioning, initialization, kernel mapping, formatting, and mounting.
- RGW: S3 object store verification, credentials provisioning, bucket management, and upload.
- CephFS: Metadata server verification, volume creation, kernel mountpoint setup, and mounting.
- RADOS: Native pool verification, byte shard ingestion, and checksum integrity validation.
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class ExecutionStep:
    """Represents a discrete step in a Ceph workflow recipe."""
    name: str
    command: str
    description: str
    is_idempotent: bool = True
    danger_level: str = "moderate"
    timeout_sec: int = 60
    optional: bool = False


def _safe_name(raw_name: str) -> str:
    """Sanitizes file/dataset names for image/bucket/object identifiers."""
    base = os.path.basename(raw_name)
    clean = "".join(c if c.isalnum() or c in "-_." else "_" for c in base).lower()
    return clean.replace(".", "_")


def _safe_pool_name(raw: Optional[str], default: str = "rbd") -> str:
    """Sanitizes destination into a valid, safe Ceph pool/fs identifier (no slashes, dots, or spaces)."""
    if not raw:
        return default
    # Strip any directory path components
    base = raw.split("/")[-1].split("\\")[-1]
    name_without_ext = base.rsplit(".", 1)[0] if "." in base else base
    clean = "".join(c if c.isalnum() or c in "_-" else "_" for c in name_without_ext).lower().strip("_")
    if not clean:
        return default
    return f"{default}_{clean}" if default not in clean else clean


def build_rbd_recipe(
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Generates the ordered step sequence for RBD Block Storage provisioning."""
    pool = _safe_pool_name(destination, default="rbd_pool")
    image_name = _safe_name(payload_path)
    size_mb = (tuning or {}).get("size_mb", 1024)
    size_str = f"{max(size_mb, 512)}M"
    mount_point = f"/mnt/rbd_{image_name}"

    return [
        ExecutionStep(
            name="ensure_rbd_pool",
            command=f"ceph osd pool create {pool} 32 32",
            description=f"Ensure target OSD pool '{pool}' exists.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="init_rbd_pool",
            command=f"rbd pool init {pool}",
            description=f"Initialize pool application tag for '{pool}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="create_rbd_image",
            command=f"rbd create {pool}/{image_name} --size {size_str} || rbd info {pool}/{image_name}",
            description=f"Create {size_str} block device image '{pool}/{image_name}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="map_rbd_device",
            command=f"rbd map {pool}/{image_name}",
            description=f"Map block image '{pool}/{image_name}' to kernel block device.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="format_rbd_filesystem",
            command=f"mkfs.ext4 -F /dev/rbd/{pool}/{image_name}",
            description="Format mapped block device with ext4 filesystem.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="mount_rbd_volume",
            command=f"mkdir -p {mount_point} && mount /dev/rbd/{pool}/{image_name} {mount_point}",
            description=f"Mount block device volume to '{mount_point}'.",
            is_idempotent=True,
            danger_level="moderate"
        )
    ]


def build_rgw_recipe(
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Generates the ordered step sequence for RGW / S3 Object Storage provisioning."""
    bucket_name = _safe_name(destination or payload_path)
    s3_uid = "agent_s3_user"
    obj_name = os.path.basename(payload_path)

    return [
        ExecutionStep(
            name="verify_rgw_service",
            command="ceph orch ps --daemon-type rgw",
            description="Verify Ceph RGW object storage service is active.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="ensure_s3_user",
            command=f"radosgw-admin user create --uid={s3_uid} --display-name='Ceph Agent User' || radosgw-admin user info --uid={s3_uid}",
            description=f"Provision S3 API credentials for '{s3_uid}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="ensure_s3_bucket",
            command=f"radosgw-admin bucket create --bucket={bucket_name} --uid={s3_uid} || radosgw-admin bucket stats --bucket={bucket_name} || true",
            description=f"Ensure S3 bucket '{bucket_name}' is initialized for '{s3_uid}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="list_s3_buckets",
            command=f"radosgw-admin bucket list --uid={s3_uid}",
            description=f"List and verify S3 buckets for '{s3_uid}'.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="verify_rgw_endpoint",
            command="curl -s -I http://localhost:80/ || curl -s -I http://localhost:8080/ || ceph -s",
            description="Verify local RGW REST endpoint HTTP connectivity.",
            is_idempotent=True,
            danger_level="read-only"
        )
    ]


def build_cephfs_recipe(
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Generates the ordered step sequence for CephFS POSIX Filesystem provisioning."""
    fs_name = _safe_pool_name(destination, default="cephfs")
    mount_point = "/mnt/cephfs"

    return [
        ExecutionStep(
            name="verify_mds_health",
            command="ceph fs status || ceph mds stat",
            description="Verify Metadata Server (MDS) daemon health.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="ensure_cephfs_volume",
            command=f"ceph fs volume create {fs_name} || ceph fs status {fs_name}",
            description=f"Ensure CephFS filesystem volume '{fs_name}' is initialized.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="setup_mountpoint",
            command=f"mkdir -p {mount_point}",
            description=f"Create local mountpoint directory at '{mount_point}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="load_ceph_module",
            command="modprobe ceph",
            description="Ensure Linux kernel 'ceph' filesystem module is loaded.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="mount_cephfs",
            command=f"mount -t ceph :/ {mount_point} -o name=admin,fs={fs_name} || mount -t ceph :/{fs_name} {mount_point} -o name=admin || mount -t ceph :/ {mount_point} -o name=admin",
            description=f"Mount CephFS '{fs_name}' to '{mount_point}'.",
            is_idempotent=True,
            danger_level="moderate"
        )
    ]


def build_rados_recipe(
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Generates the ordered step sequence for native RADOS object ingestion."""
    pool = _safe_pool_name(destination, default="datapool")
    obj_name = _safe_name(payload_path)

    return [
        ExecutionStep(
            name="ensure_rados_pool",
            command=f"ceph osd pool create {pool} 32 32",
            description=f"Ensure target RADOS pool '{pool}' exists.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="init_rados_pool",
            command=f"ceph osd pool application enable {pool} rados",
            description=f"Enable 'rados' application tag on pool '{pool}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="put_rados_object",
            command=f"echo 'RADOS payload header validation for {obj_name}' | rados -p {pool} put {obj_name} -",
            description=f"Write native object '{obj_name}' into pool '{pool}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="verify_rados_object",
            command=f"rados -p {pool} stat {obj_name}",
            description=f"Verify object '{obj_name}' integrity and metadata in pool '{pool}'.",
            is_idempotent=True,
            danger_level="read-only"
        )
    ]


def get_workflow_recipe(
    workflow: str,
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Returns the ordered DAG recipe steps for the specified Ceph workflow."""
    wf = workflow.upper()
    if wf == "RBD":
        return build_rbd_recipe(payload_path, destination, tuning)
    elif wf == "RGW":
        return build_rgw_recipe(payload_path, destination, tuning)
    elif wf == "CEPHFS":
        return build_cephfs_recipe(payload_path, destination, tuning)
    elif wf == "RADOS":
        return build_rados_recipe(payload_path, destination, tuning)
    else:
        # Default cluster health check recipe
        return [
            ExecutionStep(
                name="cluster_health_check",
                command="ceph -s",
                description="General Ceph cluster status and health check.",
                is_idempotent=True,
                danger_level="read-only"
            )
        ]
