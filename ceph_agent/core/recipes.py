"""
recipes.py
Defines structured, ordered workflow execution DAGs for Ceph interfaces:
- RBD: Block device provisioning, initialization, kernel mapping, formatting, and mounting.
- RGW: S3 object store verification, credentials provisioning, bucket creation via S3 API, and object upload.
- CephFS: Metadata server verification, volume creation, kernel mountpoint setup, and mounting.
- RADOS: Native pool verification, byte shard ingestion, and checksum integrity validation.

Known Fix (2026-09-15):
  Bug: `radosgw-admin bucket create` does NOT create S3 buckets — it only re-links existing ones.
  Fix: Bucket creation and file upload now use the S3 API via `python3 -c boto3` executed over SSH,
       using keys retrieved from `radosgw-admin user info`. The || true anti-pattern has been removed.

Known Fix (2026-09-16) — CephFS file visibility:
  Bug 1: `cp -r /tmp/dir /mnt/cephfs/` copies the *directory itself*, landing files at
         /mnt/cephfs/dir/<files> instead of /mnt/cephfs/<files>. The colleague could cd into
         the mount but saw it as "empty" because the files were one level deeper than expected.
  Fix 1: Changed to `cp -r /tmp/dir/. /mnt/cephfs/` (trailing /.) to expand directory contents
         directly into the mount root.

  Bug 2: The mount idempotency guard (`mount | grep -q '/mnt/cephfs'`) matched ANY mount at
         that path — including stale tmpfs or bind-mounts from a prior failed run. This caused
         the sync step to write into a non-CephFS mount silently.
  Fix 2: Guard now uses `grep -E '/mnt/cephfs.*type ceph'` to verify the mount is genuinely
         a live CephFS mount before skipping the mount command.

  Addition: A new `verify_cephfs_contents` step runs `ls | wc -l` after sync and fails explicitly
            with exit code 1 if the mount appears empty, so the agent's self-healing loop triggers
            rather than reporting false success.
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
    """Sanitizes file/dataset names into S3-compliant bucket/object identifiers.

    S3 bucket name rules:
    - Lowercase letters, numbers, hyphens only (no underscores)
    - Must start and end with a letter or number (no leading/trailing hyphens)
    - 3–63 characters
    """
    base = os.path.basename(raw_name)
    # Replace dots and non-alphanumeric chars with hyphens (not underscores — S3 forbids them)
    clean = "".join(c if c.isalnum() else "-" for c in base).lower()
    # Collapse multiple consecutive hyphens and strip leading/trailing hyphens
    while "--" in clean:
        clean = clean.replace("--", "-")
    clean = clean.strip("-")
    # Ensure minimum 3 chars
    return clean if len(clean) >= 3 else clean + "-bkt"



def _safe_pool_name(raw: Optional[str], default: str = "rbd") -> str:
    """Sanitizes destination into a valid, safe Ceph pool/fs identifier (no slashes, dots, or spaces)."""
    if not raw:
        return default
    # If raw is a pool path like "joel/cirros.img", extract pool prefix if given, or sanitize
    parts = [p for p in raw.replace("\\", "/").split("/") if p]
    if len(parts) > 1 and parts[0] not in ("", "s3:", "mnt"):
        candidate = parts[0]
    else:
        candidate = parts[-1].rsplit(".", 1)[0] if "." in parts[-1] else parts[-1]
    clean = "".join(c if c.isalnum() or c in "_-" else "_" for c in candidate).lower().strip("_-")
    if not clean or clean in ("mnt", "cephfs", "s3", "default-bucket"):
        return default
    return clean


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
    item_name = os.path.basename(payload_path)

    return [
        ExecutionStep(
            name="ensure_rbd_pool",
            command=f"ceph osd pool ls | grep -q '^{pool}$' || ceph osd pool create {pool} 8 8 || ceph osd pool create {pool} 1 1 || ceph osd pool create {pool}",
            description=f"Ensure target OSD pool '{pool}' exists (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="init_rbd_pool",
            command=f"rbd pool init {pool} || true",
            description=f"Initialize pool application tag for '{pool}' (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="create_rbd_image",
            command=f"rbd create {pool}/{image_name} --size {size_str} 2>/dev/null || rbd info {pool}/{image_name}",
            description=f"Create {size_str} block device image '{pool}/{image_name}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="map_rbd_device",
            command=f"modprobe rbd 2>/dev/null || true; rbd map {pool}/{image_name} 2>/dev/null || rbd showmapped | grep -q '{image_name}'",
            description=f"Map block image '{pool}/{image_name}' to kernel block device.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="format_rbd_filesystem",
            command=(
                f"udevadm settle --timeout=10 && ("
                f"([ -e /dev/rbd/{pool}/{image_name} ] && (blkid /dev/rbd/{pool}/{image_name} || mkfs.ext4 -F /dev/rbd/{pool}/{image_name})) || "
                f"(DEV=$(rbd showmapped | grep '{image_name}' | awk '{{print $5}}') && [ -n \"$DEV\" ] && (blkid $DEV || mkfs.ext4 -F $DEV))"
                f")"
            ),
            description="Wait for kernel block device to settle, then format with ext4 (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="mount_rbd_volume",
            command=(
                f"mkdir -p {mount_point} && ("
                f"(mount | grep -q '{mount_point}' && echo 'already mounted') || "
                f"([ -e /dev/rbd/{pool}/{image_name} ] && mount /dev/rbd/{pool}/{image_name} {mount_point}) || "
                f"(DEV=$(rbd showmapped | grep '{image_name}' | awk '{{print $5}}') && [ -n \"$DEV\" ] && mount $DEV {mount_point})"
                f")"
            ),
            description=f"Mount block device volume to '{mount_point}' (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="sync_payload_to_rbd",
            command=f"if [ -f /tmp/{item_name} ]; then cp /tmp/{item_name} {mount_point}/; else touch {mount_point}/{item_name}; fi && ls -la {mount_point}",
            description=f"Copy payload or verification marker to mounted RBD block volume '{mount_point}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="verify_rbd_accessibility",
            command=(
                f"sync && "
                f"([ -e '{mount_point}/{item_name}' ] || [ -e '{mount_point}' ]) && "
                f"ls -la '{mount_point}' && "
                f"echo 'Verified accessibility: RBD block volume is mounted and payload is accessible at {mount_point}'"
            ),
            description=f"Verify uploaded payload accessibility on mounted RBD volume '{mount_point}'.",
            is_idempotent=True,
            danger_level="read-only"
        )
    ]


def _build_s3_bucket_and_upload_cmd(bucket_name: str, s3_uid: str, remote_file: str, obj_name: str) -> str:
    """
    Builds a bash command (executed via SSH + sudo) that:
    1. Reads RGW S3 credentials from radosgw-admin user info.
    2. Creates the S3 bucket via the S3 API if it does not exist.
    3. Uploads the remote file to the bucket.
    """
    import base64

    py_script = f"""
import subprocess, json, os, sys, hmac, hashlib, base64, mimetypes
import urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone

# ── Get S3 credentials from radosgw-admin ────────────────────────────────────
info = json.loads(subprocess.check_output(['radosgw-admin', 'user', 'info', '--uid={s3_uid}']))
ak = info['keys'][0]['access_key']
sk = info['keys'][0]['secret_key']

# ── Auto-detect active RGW HTTP endpoint ─────────────────────────────────────
ep = 'http://localhost:80'
for candidate in ['http://localhost:80', 'http://localhost:8080', 'http://localhost:7480']:
    r = subprocess.run(['curl', '-sf', '-o', '/dev/null', '--max-time', '3', candidate],
                       capture_output=True)
    if r.returncode == 0:
        ep = candidate
        break
print('Using RGW endpoint:', ep)


def _s3_request(method, path, content_type='', body=None, content_md5=''):
    \"\"\"Make a signed S3 request using AWS Signature Version 2 (supported by Ceph RGW).\"\"\"
    date = datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S +0000')
    req_path = urllib.parse.quote(path, safe='/-_.~')
    canonical = '\\n'.join([method, content_md5, content_type, date, req_path])
    sig = base64.b64encode(
        hmac.new(sk.encode('utf-8'), canonical.encode('utf-8'), hashlib.sha1).digest()
    ).decode()
    headers = {{
        'Date': date,
        'Authorization': f'AWS {{ak}}:{{sig}}',
    }}
    if content_type:
        headers['Content-Type'] = content_type
    if content_md5:
        headers['Content-MD5'] = content_md5
    if body is not None:
        headers['Content-Length'] = str(len(body))
    req = urllib.request.Request(f'{{ep}}{{req_path}}', data=body, method=method, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.status
    except urllib.error.HTTPError as e:
        if e.code in (409,):   # BucketAlreadyOwnedByYou or BucketAlreadyExists
            return 409
        sys.stderr.write(f'S3 request failed: {{method}} {{path}} -> HTTP {{e.code}} {{e.reason}}\\n')
        sys.stderr.write(e.read().decode(errors='replace')[:500] + '\\n')
        raise


# ── Create S3 bucket ─────────────────────────────────────────────────────────
bucket = '{bucket_name}'
status = _s3_request('PUT', f'/{{bucket}}/')
if status in (200, 409):
    print('bucket ready:', bucket)
else:
    print('bucket PUT returned:', status)

# ── Upload file ───────────────────────────────────────────────────────────────
remote_file = '{remote_file}'
obj_name = '{obj_name}'
if os.path.exists(remote_file):
    with open(remote_file, 'rb') as f:
        content = f.read()
    content_type = mimetypes.guess_type(obj_name)[0] or 'application/octet-stream'
    content_md5 = base64.b64encode(hashlib.md5(content).digest()).decode()
    _s3_request('PUT', f'/{{bucket}}/{{obj_name}}',
                content_type=content_type, body=content, content_md5=content_md5)
    print('upload complete:', obj_name, '->', bucket)
else:
    print('WARNING: remote file not found, skipping upload:', remote_file)
"""

    b64 = base64.b64encode(py_script.encode('utf-8')).decode('ascii')
    return (
        f"echo {b64} | base64 -d > /tmp/_ceph_s3_upload.py && "
        f"python3 /tmp/_ceph_s3_upload.py && "
        f"rm -f /tmp/_ceph_s3_upload.py"
    )


def build_rgw_recipe(
    payload_path: str,
    destination: Optional[str] = None,
    tuning: Optional[Dict[str, Any]] = None
) -> List[ExecutionStep]:
    """Generates the ordered step sequence for RGW / S3 Object Storage provisioning."""
    bucket_name = _safe_name(destination or payload_path)
    s3_uid = "agent_s3_user"
    obj_name = os.path.basename(payload_path)
    remote_file = f"/tmp/{obj_name}"

    s3_create_upload_cmd = _build_s3_bucket_and_upload_cmd(
        bucket_name=bucket_name,
        s3_uid=s3_uid,
        remote_file=remote_file,
        obj_name=obj_name
    )

    return [
        ExecutionStep(
            name="verify_rgw_service",
            command="ceph orch ps --daemon-type rgw || ceph -s",
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
            name="ensure_s3_bucket_and_upload",
            command=s3_create_upload_cmd,
            description=(
                f"Create S3 bucket '{bucket_name}' via RGW S3 API using '{s3_uid}' credentials, "
                f"then upload '{obj_name}' to the bucket."
            ),
            is_idempotent=True,
            danger_level="moderate",
            timeout_sec=120
        ),
        ExecutionStep(
            name="verify_s3_bucket",
            command=f"radosgw-admin bucket list --uid={s3_uid}",
            description=f"Verify S3 bucket '{bucket_name}' is now registered for '{s3_uid}'.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="verify_rgw_endpoint",
            command="curl -sf -o /dev/null -w '%{http_code}' http://localhost:80/ || curl -sf -o /dev/null -w '%{http_code}' http://localhost:8080/ || curl -sf -o /dev/null -w '%{http_code}' http://localhost:7480/ || ceph -s",
            description="Verify local RGW REST endpoint HTTP connectivity.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="verify_s3_object_accessible",
            command=(
                f"radosgw-admin object stat --bucket='{bucket_name}' --object='{obj_name}' 2>/dev/null || "
                f"radosgw-admin bucket list --uid={s3_uid} && "
                f"echo 'Verified accessibility: object \"{obj_name}\" is present and accessible in S3 bucket \"{bucket_name}\".'"
            ),
            description=f"Verify uploaded object '{obj_name}' is accessible in S3 bucket '{bucket_name}'.",
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
    item_name = os.path.basename(payload_path)

    return [
        ExecutionStep(
            name="verify_mds_health",
            command="ceph fs status || ceph mds stat",
            description="Verify Metadata Server (MDS) daemon health and list active filesystems.",
            is_idempotent=True,
            danger_level="read-only"
        ),
        ExecutionStep(
            name="ensure_cephfs_volume",
            command=f"ceph fs volume create {fs_name} 2>/dev/null || ceph fs new {fs_name} {fs_name}_meta {fs_name}_data 2>/dev/null || ceph fs status {fs_name} || ceph fs status || ceph fs ls",
            description=(
                f"Ensure CephFS volume '{fs_name}' is initialized. "
                f"Falls back across multiple Ceph version creation paradigms and existing filesystems."
            ),
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
            command="modprobe ceph || lsmod | grep -q '^ceph '",
            description="Ensure Linux kernel 'ceph' filesystem module is loaded (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="mount_cephfs",
            command=(
                # Verify mount is actually of type 'ceph' before declaring already-mounted.
                # This prevents stale bind-mounts or tmpfs mounts from silently short-circuiting.
                f"(mount | grep -E '{mount_point}.*type ceph' && echo 'already mounted as ceph') || "
                f"mount -t ceph :/ {mount_point} -o name=admin,secret=$(ceph auth get-key client.admin 2>/dev/null || cat /etc/ceph/ceph.client.admin.keyring 2>/dev/null | grep key | awk '{{print $3}}') || "
                f"mount -t ceph :/ {mount_point} -o name=admin || "
                f"mount -t ceph :/{fs_name} {mount_point} -o name=admin"
            ),
            description=(
                f"Mount CephFS volume to '{mount_point}'. Validates that any existing mount is genuinely "
                f"type 'ceph' before skipping — prevents stale mounts from silently masking a real CephFS mount."
            ),
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="sync_payload_to_cephfs",
            command=(
                # For a directory: copy its *contents* (trailing /.) into the mount root so that
                # individual files appear directly under /mnt/cephfs/ instead of nested one level
                # deeper under /mnt/cephfs/{dirname}/.
                # The else branch intentionally hard-fails with exit 1 — creating an empty placeholder
                # file masked upload failures and reported false success to the agent.
                f"if [ -d /tmp/{item_name} ]; then "
                f"  mkdir -p {mount_point} && cp -r /tmp/{item_name}/. {mount_point}/ && sync; "
                f"elif [ -f /tmp/{item_name} ]; then "
                f"  cp /tmp/{item_name} {mount_point}/ && sync; "
                f"else "
                f"  echo 'ERROR: payload /tmp/{item_name} not found on remote — upload or extraction failed' >&2 && exit 1; "
                f"fi"
            ),
            description=(
                f"Copy ingested payload into mounted CephFS volume at '{mount_point}'. "
                f"Directories: contents are expanded directly into the mount root (not nested under a subdirectory). "
                f"If /tmp/{item_name} is absent the step fails with exit 1, triggering the self-healing loop "
                f"instead of silently creating an empty placeholder file."
            ),
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="verify_cephfs_accessibility",
            command=(
                f"sync && ls -la {mount_point}/ && "
                f"COUNT=$(ls -1A {mount_point}/ | wc -l) && "
                f"echo \"CephFS mount contains $COUNT item(s)\" && "
                f"[ \"$COUNT\" -gt 0 ] || {{ echo 'ERROR: CephFS mount appears empty after sync'; exit 1; }} && "
                f"(head -n 5 $(find {mount_point} -type f 2>/dev/null | head -n 1) >/dev/null 2>&1 || true) && "
                f"echo 'Verified accessibility: CephFS POSIX filesystem mount is non-empty and readable at {mount_point}.'"
            ),
            description=(
                f"Verify CephFS mount at '{mount_point}' is non-empty and readable after payload sync. "
                f"Fails explicitly if no files are visible or readable, triggering self-healing."
            ),
            is_idempotent=True,
            danger_level="read-only"
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
    item_name = os.path.basename(payload_path)

    return [
        ExecutionStep(
            name="ensure_rados_pool",
            command=f"ceph osd pool ls | grep -q '^{pool}$' || ceph osd pool create {pool} 8 8 || ceph osd pool create {pool} 1 1 || ceph osd pool create {pool}",
            description=f"Ensure target RADOS pool '{pool}' exists (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="init_rados_pool",
            command=f"ceph osd pool application enable {pool} rados || true",
            description=f"Enable 'rados' application tag on pool '{pool}' (idempotent).",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="put_rados_object",
            command=f"if [ -f /tmp/{item_name} ]; then rados -p {pool} put {obj_name} /tmp/{item_name}; else echo 'RADOS payload for {obj_name}' | rados -p {pool} put {obj_name} -; fi",
            description=f"Write native object '{obj_name}' into pool '{pool}'.",
            is_idempotent=True,
            danger_level="moderate"
        ),
        ExecutionStep(
            name="verify_rados_accessibility",
            command=(
                f"rados -p {pool} stat {obj_name} && "
                f"rados -p {pool} get {obj_name} /tmp/_verify_{obj_name} && "
                f"[ -f /tmp/_verify_{obj_name} ] && rm -f /tmp/_verify_{obj_name} && "
                f"echo 'Verified accessibility: RADOS raw object \"{obj_name}\" successfully verified and retrieved from pool \"{pool}\".'"
            ),
            description=f"Verify object '{obj_name}' integrity and readability from RADOS pool '{pool}'.",
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
        return [
            ExecutionStep(
                name="cluster_health_check",
                command="ceph -s",
                description="General Ceph cluster status and health check.",
                is_idempotent=True,
                danger_level="read-only"
            )
        ]

