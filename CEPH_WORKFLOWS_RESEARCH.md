# Ceph Storage Workflows & Architecture Reference

This document summarizes the core storage workflows in Ceph (RBD, RGW/S3, CephFS, and RADOS), their architectural components, practical operational commands, benchmarking methods, and current state on the VM.

---

## 1. High-Level Architecture Overview

At the heart of Ceph is **RADOS** (Reliable Autonomic Distributed Object Store), an object store managed by:
- **OSD (Object Storage Daemon)**: Stores data chunks via BlueStore directly on raw block devices.
- **MON (Monitor)**: Maintains cluster maps (OSD, Mon, PG, CRUSH) with Paxos consensus.
- **MGR (Manager)**: Collects metrics (Prometheus module on `:9283`), Ceph Dashboard (`:8443`), and orchestrates container daemons (`cephadm`).
- **CRUSH**: Computes data placement deterministically using pseudo-random hashing without central lookup tables.

Three primary interfaces sit on top of RADOS:

```
                      +-----------------------------+
                      |     Client Applications     |
                      +--------------+--------------+
                                     |
    +--------------------------------+--------------------------------+
    |                                |                                |
+---+-------------------+    +-------+---------------+    +-----------+-----------+
|      Block (RBD)      |    |  Object Storage (RGW) |    |   File System (CephFS)|
|  - VM virtual disks   |    |  - S3 / Swift REST    |    |   - POSIX filesystem  |
|  - Kubernetes PVs     |    |  - Buckets & Objects  |    |   - Shared dirs / HPC |
|  - /dev/rbd* & librbd |    |  - radosgw daemon     |    |   - MDS metadata srv  |
+-----------+-----------+    +-------+---------------+    +-----------+-----------+
            |                        |                                |
            +------------------------+--------------------------------+
                                     |
                          +----------+----------+
                          |   RADOS (librados)  |
                          |   OSDs / MONs / MGRs|
                          +---------------------+
```

---

## 2. The 4 Ceph Workflows in Detail

### A. RADOS Block Device (RBD)
- **Concept**: Stripes a single virtual disk block device across thousands of 4MB RADOS objects.
- **Key Features**: Thin-provisioning, copy-on-write (COW) clones, instantaneous snapshots, exclusive locking, asynchronous block mirroring.
- **Access Modes**:
  1. *Kernel Client*: `rbd map <img_name>` $\rightarrow$ exposes `/dev/rbd0` $\rightarrow$ format (`mkfs.ext4`) $\rightarrow$ `mount /dev/rbd0 /mnt`.
  2. *Userspace (`librbd`)*: Direct integration with QEMU/KVM hypervisors and Kubernetes Ceph-CSI without kernel module overhead.
- **Key Commands**:
  ```bash
  # Create pool and initialize for RBD
  ceph osd pool create rbd_pool 32 32
  rbd pool init rbd_pool

  # Create a 20GB thin-provisioned disk image
  rbd create disk01 --size 20480 --pool rbd_pool --image-format 2

  # Snapshot & COW clone
  rbd snap create rbd_pool/disk01@snap1
  rbd snap protect rbd_pool/disk01@snap1
  rbd clone rbd_pool/disk01@snap1 rbd_pool/disk01_clone

  # Map & mount
  sudo rbd map disk01 --pool rbd_pool
  sudo mkfs.ext4 /dev/rbd0
  sudo mount /dev/rbd0 /mnt/disk
  ```
- **Workload / Benchmarking**:
  - `rbd bench --io-type write --io-size 4k --io-threads 16 --io-total 1G --pool rbd_pool disk01`
  - `fio --name=rbd_test --ioengine=rbd --pool=rbd_pool --rbdname=disk01 --rw=randrw --bs=4k --numjobs=4 --runtime=30`

---

### B. Ceph Object Gateway (RGW / S3 & Swift)
- **Concept**: HTTP/REST daemon (`radosgw`) translating Amazon S3 and OpenStack Swift REST calls into RADOS objects.
- **Internal Pool Splitting**:
  - `default.rgw.meta`: S3 user accounts and bucket configuration.
  - `default.rgw.buckets.index`: High-speed OMAP key-value index storing bucket dentries.
  - `default.rgw.buckets.data`: Actual binary object chunks.
- **Access Modes**: AWS CLI, `boto3` (Python), MinIO client `mc`, `s3cmd`, Cyberduck.
- **Key Commands**:
  ```bash
  # Create S3 user with access keys
  radosgw-admin user create --uid="tester" --display-name="Test User" --access-key="TESTKEY" --secret-key="TESTSECRET"

  # S3 Python integration (boto3)
  import boto3
  s3 = boto3.client('s3', endpoint_url='http://127.0.0.1:8000', aws_access_key_id='TESTKEY', aws_secret_access_key='TESTSECRET')
  s3.create_bucket(Bucket='test-bucket')
  s3.put_object(Bucket='test-bucket', Key='sample.dat', Body=b'Hello Ceph Object Store')
  ```
- **Workload / Benchmarking**:
  - *Small Objects (1KB–64KB)*: Tests bucket index locking (`OMAP`) and metadata operations.
  - *Large Objects (100MB–5GB Multipart)*: Tests raw bandwidth and OSD streaming.
  - *Tools*: MinIO `warp`, `cosbench`, automated `boto3` concurrent threads.

---

### C. Ceph File System (CephFS)
- **Concept**: POSIX-compliant distributed filesystem that strictly separates metadata operations from data storage.
- **Key Components**:
  - **MDS (Metadata Server)**: In-memory daemon managing POSIX directory hierarchy, permissions, open file handles, and leases.
  - **Metadata Pool**: Low-latency OSD pool storing directory inodes and the MDS journal.
  - **Data Pool**: High-capacity OSD pool storing raw file chunk objects.
- **Access Modes**:
  1. *Kernel Client*: Native Linux kernel driver (`mount -t ceph mon_ip:6789:/ /mnt/cephfs`).
  2. *FUSE Client*: Userspace driver (`ceph-fuse -m mon_ip:6789 /mnt/cephfs`).
  3. *NFS Gateway*: NFS-Ganesha exposing CephFS as standard NFS shares.
- **Key Commands**:
  ```bash
  # Create data and metadata pools
  ceph osd pool create cephfs_data 32 32
  ceph osd pool create cephfs_metadata 32 32

  # Create filesystem
  ceph fs new cephfs cephfs_metadata cephfs_data

  # Mount via kernel driver
  sudo mount -t ceph 127.0.0.1:6789:/ /mnt/cephfs -o name=admin,secret=AQ...==
  ```
- **Workload / Benchmarking**:
  - `fio` direct POSIX file I/O tests on `/mnt/cephfs`.
  - `mdtest` / `smallfile` for metadata-intensive workloads (creating/opening thousands of tiny files).

---

### D. Native RADOS (librados)
- **Concept**: Bypasses block, filesystem, and HTTP layers to interact directly with raw objects on OSDs.
- **Key Commands**:
  ```bash
  # Put / Get raw objects
  rados -p test_data_pool put obj1 /path/to/local/file
  rados -p test_data_pool get obj1 /tmp/out_file

  # Native RADOS stress benchmark
  rados bench -p test_data_pool 30 write -t 16 -b 4M --no-cleanup
  rados bench -p test_data_pool 30 seq -t 16
  rados -p test_data_pool cleanup
  ```

---

## 3. Comparison Matrix

| Feature | RBD (Block) | RGW (Object) | CephFS (Filesystem) | Native RADOS |
| :--- | :--- | :--- | :--- | :--- |
| **Protocol** | Block Device / `librbd` | S3 / Swift REST HTTP | POSIX (`mount -t ceph`) | C/C++/Python `librados` |
| **Typical Use Case** | VMs (KVM/QEMU), Databases | Media, Backups, Cloud Apps | Shared user dirs, HPC | Distributed DB engines |
| **Required Daemons** | MON, MGR, OSD | MON, MGR, OSD, **RGW** | MON, MGR, OSD, **MDS** | MON, MGR, OSD |
| **Data Unit** | 4KB–4MB disk blocks | 1B–5TB objects | Hierarchical files/folders | Key-Value & raw objects |
| **Benchmark Tool** | `rbd bench`, `fio (librbd)` | `warp`, `cosbench`, `boto3` | `fio`, `mdtest`, `smallfile` | `rados bench` |

---

## 4. Current VM Environment Status (`aikyastorvm`)

- **Ceph Release**: `17.2.8 Quincy (stable)` running via `cephadm` container orchestrator.
- **Cluster State**: `HEALTH_OK`, 1 MON, 1 MGR (Prometheus on 9283, Dashboard on 8443), 1 OSD.
- **Active Pools**:
  - `.mgr` (Application: mgr)
  - `.rgw.root` (Application: rgw)
  - `joel` (Application: rbd)
  - `test_data_pool` (Application: rados)
- **Readiness**:
  - **RBD**: Ready (`joel` pool is active; `rbd` CLI installed).
  - **RADOS**: Ready (`test_data_pool` active; `rados` CLI installed).
  - **RGW**: Can be deployed on-demand via `ceph orch apply rgw single-zone --placement="aikyastorvm"`.
  - **CephFS**: Can be deployed on-demand via `ceph fs volume create cephfs` or manual pool creation.
