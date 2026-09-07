"""
magic_probe.py
Deep binary signature and exact-offset magic byte inspector for storage classification.
Inspects headers at offset 0, POSIX Tar at offset 257, and ISO 9660 at offset 32769.
"""

import os
import mimetypes
from typing import Dict, Any, Tuple, Optional

class MagicProbe:
    """Inspects raw byte headers to identify exact storage container formats."""
    
    # Well-known signature map
    SIGNATURES = {
        # Virtual Disk Images -> RBD
        "QCOW2": {"offset": 0, "magic": b"QFI\xfb", "category": "virtual_disk_image", "workflow": "RBD"},
        "VMDK": {"offset": 0, "magic": b"KDMV", "category": "virtual_disk_image", "workflow": "RBD"},
        "VDI": {"offset": 0, "magic": b"<<< Oracle", "category": "virtual_disk_image", "workflow": "RBD"},
        "VHDX": {"offset": 0, "magic": b"vhdxfile", "category": "virtual_disk_image", "workflow": "RBD"},
        "VHD": {"offset": 0, "magic": b"conectix", "category": "virtual_disk_image", "workflow": "RBD"},
        
        # Databases -> Relational
        "SQLite3": {"offset": 0, "magic": b"SQLite format 3\x00", "category": "relational_database", "workflow": "RBD"},
        
        # Datasets & Columnar -> RGW (S3)
        "Parquet": {"offset": 0, "magic": b"PAR1", "category": "columnar_dataset", "workflow": "RGW"},
        
        # Archives -> RGW / S3
        "ZIP": {"offset": 0, "magic": b"PK\x03\x04", "category": "archive_bundle", "workflow": "RGW"},
        "GZIP": {"offset": 0, "magic": b"\x1f\x8b", "category": "archive_bundle", "workflow": "RGW"},
        "BZIP2": {"offset": 0, "magic": b"BZh", "category": "archive_bundle", "workflow": "RGW"},
        "XZ": {"offset": 0, "magic": b"\xfd7zXZ\x00", "category": "archive_bundle", "workflow": "RGW"},
        "7Z": {"offset": 0, "magic": b"7z\xbc\xaf\x27\x1c", "category": "archive_bundle", "workflow": "RGW"},
        
        # Media / Documents -> RGW (S3)
        "PDF": {"offset": 0, "magic": b"%PDF-", "category": "flat_media_object", "workflow": "RGW"},
        "PNG": {"offset": 0, "magic": b"\x89PNG\r\n\x1a\n", "category": "flat_media_object", "workflow": "RGW"},
        "JPEG": {"offset": 0, "magic": b"\xff\xd8\xff", "category": "flat_media_object", "workflow": "RGW"},
        "GIF": {"offset": 0, "magic": b"GIF8", "category": "flat_media_object", "workflow": "RGW"},
        "ELF": {"offset": 0, "magic": b"\x7fELF", "category": "generic_stream", "workflow": "RADOS"},
    }

    def inspect(self, file_path: str) -> Dict[str, Any]:
        """
        Inspects binary headers of a file.
        Returns a dict containing:
        { 'magic_name', 'category', 'target_workflow', 'mime_type', 'sample_hex' }
        """
        if not os.path.isfile(file_path):
            return {
                "magic_name": "UNKNOWN",
                "category": "generic_stream",
                "target_workflow": "RGW",
                "mime_type": "application/octet-stream",
                "sample_hex": ""
            }

        size = os.path.getsize(file_path)
        mime, _ = mimetypes.guess_type(file_path)
        mime = mime or "application/octet-stream"

        header_512 = b""
        iso_header = b""
        tar_header = b""

        try:
            with open(file_path, "rb") as f:
                header_512 = f.read(512)
                
                # Check POSIX Tar at offset 257
                if size >= 512:
                    f.seek(257)
                    tar_header = f.read(8)
                    
                # Check ISO 9660 signature at offset 32769 (0x8001)
                if size >= 32774:
                    f.seek(32768)
                    iso_header = f.read(6)
        except Exception:
            pass

        sample_hex = header_512[:16].hex()

        # 1. Check ISO 9660 at offset 32768/32769
        if iso_header and (b"CD001" in iso_header or iso_header.startswith(b"\x01CD001")):
            return {
                "magic_name": "ISO9660",
                "category": "virtual_disk_image",
                "target_workflow": "RBD",
                "mime_type": "application/x-iso9660-image",
                "sample_hex": sample_hex
            }

        # 2. Check POSIX Tar at offset 257
        if tar_header and tar_header.startswith(b"ustar"):
            return {
                "magic_name": "POSIX_TAR",
                "category": "archive_bundle",
                "target_workflow": "RGW",
                "mime_type": "application/x-tar",
                "sample_hex": sample_hex
            }

        # 3. Check ISOBMFF Containers (ftyp box at offset 4: AVIF, MP4, QuickTime)
        if len(header_512) >= 12 and header_512[4:8] == b"ftyp":
            major_brand = header_512[8:12]
            if major_brand in (b"avif", b"avis"):
                return {
                    "magic_name": "AVIF_IMAGE",
                    "category": "flat_media_object",
                    "target_workflow": "RGW",
                    "mime_type": "image/avif",
                    "sample_hex": sample_hex
                }
            return {
                "magic_name": "MP4_CONTAINER",
                "category": "flat_media_object",
                "target_workflow": "RGW",
                "mime_type": "video/mp4",
                "sample_hex": sample_hex
            }

        # 4. Check Offset 0 Signatures
        for name, sig in self.SIGNATURES.items():
            magic_bytes = sig["magic"]
            if header_512.startswith(magic_bytes):
                return {
                    "magic_name": name,
                    "category": sig["category"],
                    "target_workflow": sig["workflow"],
                    "mime_type": mime,
                    "sample_hex": sample_hex
                }

        # 5. Extension-based fallback if binary is unrecognised
        ext = os.path.splitext(file_path)[1].lower()
        if ext in [".iso", ".img", ".raw", ".qcow2", ".vmdk", ".vdi", ".vhdx"]:
            return {
                "magic_name": f"DISK_EXT_{ext.upper().strip('.')}",
                "category": "virtual_disk_image",
                "target_workflow": "RBD",
                "mime_type": "application/x-raw-disk-image",
                "sample_hex": sample_hex
            }
        elif ext in [".avif", ".heic", ".webp", ".png", ".jpg", ".jpeg", ".gif", ".mp4", ".mov", ".mkv", ".pdf", ".csv", ".parquet"]:
            return {
                "magic_name": f"MEDIA_EXT_{ext.upper().strip('.')}",
                "category": "flat_media_object",
                "target_workflow": "RGW",
                "mime_type": mime or "image/avif",
                "sample_hex": sample_hex
            }
        elif ext in [".omap", ".kv", ".rados"]:
            return {
                "magic_name": "RADOS_OMAP_PAYLOAD",
                "category": "raw_key_value_shard",
                "target_workflow": "RADOS",
                "mime_type": "application/x-ceph-rados",
                "sample_hex": sample_hex
            }

        return {
            "magic_name": "GENERIC_BINARY",
            "category": "generic_stream",
            "target_workflow": "RGW",
            "mime_type": mime,
            "sample_hex": sample_hex
        }
