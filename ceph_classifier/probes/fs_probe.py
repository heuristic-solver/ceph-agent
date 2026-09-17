"""
fs_probe.py
Filesystem structure probe for analyzing directory hierarchies, depth, and POSIX project markers.
"""

import os
import zipfile
import tarfile
from typing import Dict, Any, List, Optional

PROJECT_MARKERS = {
    ".git", ".svn", "Makefile", "package.json", "setup.py", "pyproject.toml",
    "Cargo.toml", "go.mod", "pom.xml", "CMakeLists.txt", "Dockerfile",
    "docker-compose.yml", "requirements.txt"
}

CODE_EXTENSIONS = {
    ".py", ".c", ".cpp", ".h", ".rs", ".go", ".java", ".js", ".ts", ".jsx", ".tsx",
    ".php", ".rb", ".sh", ".bash", ".html", ".css", ".json", ".yaml", ".yml", ".md"
}

class FSProbe:
    """Recursively inspects directory layout, file structures, and archive package topologies."""
    
    def _inspect_archive(self, target_path: str, max_depth_limit: int = 6) -> Optional[Dict[str, Any]]:
        """Peeks into ZIP or TAR archives to analyze packaged directory layout and markers."""
        size = os.path.getsize(target_path)
        ext = os.path.splitext(target_path)[1].lower()
        is_zip = False
        is_tar = False

        if ext == ".zip" or zipfile.is_zipfile(target_path):
            is_zip = True
        elif ext in [".tar", ".tgz"] or target_path.endswith((".tar.gz", ".tar.bz2", ".tar.xz")) or tarfile.is_tarfile(target_path):
            is_tar = True

        if not is_zip and not is_tar:
            return None

        file_count = 0
        max_depth = 0
        detected_markers = []
        code_file_count = 0
        subfolder_count = 0

        try:
            if is_zip:
                with zipfile.ZipFile(target_path, "r") as zf:
                    for info in zf.infolist():
                        filename = info.filename.rstrip("/\\")
                        if not filename:
                            continue
                        parts = [p for p in filename.replace("\\", "/").split("/") if p]
                        depth = max(0, len(parts) - 1)
                        if depth > max_depth:
                            max_depth = min(depth, max_depth_limit)

                        base_name = parts[-1] if parts else ""
                        if info.is_dir() or info.filename.endswith(("/", "\\")):
                            subfolder_count += 1
                        else:
                            file_count += 1
                            item_ext = os.path.splitext(base_name)[1].lower()
                            if item_ext in CODE_EXTENSIONS:
                                code_file_count += 1

                        if base_name in PROJECT_MARKERS and base_name not in detected_markers:
                            detected_markers.append(base_name)
            elif is_tar:
                with tarfile.open(target_path, "r:*") as tf:
                    for member in tf.getmembers():
                        filename = member.name.rstrip("/\\")
                        if not filename:
                            continue
                        parts = [p for p in filename.replace("\\", "/").split("/") if p]
                        depth = max(0, len(parts) - 1)
                        if depth > max_depth:
                            max_depth = min(depth, max_depth_limit)

                        base_name = parts[-1] if parts else ""
                        if member.isdir():
                            subfolder_count += 1
                        elif member.isfile():
                            file_count += 1
                            item_ext = os.path.splitext(base_name)[1].lower()
                            if item_ext in CODE_EXTENSIONS:
                                code_file_count += 1

                        if base_name in PROJECT_MARKERS and base_name not in detected_markers:
                            detected_markers.append(base_name)

            code_ratio = code_file_count / file_count if file_count > 0 else 0.0
            is_packaged_dir = (file_count > 1 or max_depth > 0 or len(detected_markers) > 0 or subfolder_count > 0)

            return {
                "is_directory": False,
                "is_archive": True,
                "is_packaged_directory": is_packaged_dir,
                "size_bytes": size,
                "size_mb": round(size / (1024 * 1024), 2),
                "file_count": file_count,
                "max_depth": max_depth,
                "has_project_markers": len(detected_markers) > 0,
                "detected_markers": detected_markers,
                "code_file_ratio": round(code_ratio, 2),
                "is_flat": (max_depth <= 1 and subfolder_count == 0)
            }
        except Exception:
            return None

    def inspect(self, target_path: str, max_depth_limit: int = 6) -> Dict[str, Any]:
        if not os.path.exists(target_path):
            raise FileNotFoundError(f"Target path does not exist: {target_path}")

        if not os.path.isdir(target_path):
            # Check if this file is an archive container
            archive_info = self._inspect_archive(target_path, max_depth_limit=max_depth_limit)
            if archive_info is not None:
                return archive_info

            size = os.path.getsize(target_path)
            ext = os.path.splitext(target_path)[1].lower()
            return {
                "is_directory": False,
                "is_archive": False,
                "is_packaged_directory": False,
                "size_bytes": size,
                "size_mb": round(size / (1024 * 1024), 2),
                "file_count": 1,
                "max_depth": 0,
                "has_project_markers": False,
                "detected_markers": [],
                "code_file_ratio": 1.0 if ext in CODE_EXTENSIONS else 0.0,
                "is_flat": True
            }

        # Directory traversal
        total_size = 0
        file_count = 0
        max_depth = 0
        detected_markers = []
        code_file_count = 0
        subfolder_count = 0

        target_norm = os.path.normpath(os.path.abspath(target_path))
        base_depth = len(target_norm.split(os.sep))

        for root, dirs, files in os.walk(target_norm):
            root_norm = os.path.normpath(root)
            curr_depth = len(root_norm.split(os.sep)) - base_depth
            if curr_depth > max_depth:
                max_depth = curr_depth
                
            subfolder_count += len(dirs)
            
            for item in dirs + files:
                if item in PROJECT_MARKERS and item not in detected_markers:
                    detected_markers.append(item)
                    
            for f in files:
                file_count += 1
                f_path = os.path.join(root, f)
                try:
                    f_size = os.path.getsize(f_path)
                    total_size += f_size
                except Exception:
                    pass
                    
                ext = os.path.splitext(f)[1].lower()
                if ext in CODE_EXTENSIONS:
                    code_file_count += 1
                    
            if curr_depth >= max_depth_limit:
                del dirs[:]

        code_ratio = code_file_count / file_count if file_count > 0 else 0.0
        is_flat = (max_depth <= 1 and subfolder_count == 0)

        return {
            "is_directory": True,
            "size_bytes": total_size,
            "size_mb": round(total_size / (1024 * 1024), 2),
            "file_count": file_count,
            "max_depth": max_depth,
            "has_project_markers": len(detected_markers) > 0,
            "detected_markers": detected_markers,
            "code_file_ratio": round(code_ratio, 2),
            "is_flat": is_flat
        }
