"""
llm_oracle.py
Tier 2 Local LLM Fallback Engine (Ollama / Gemma).
Invoked only when Tier 1 deterministic confidence is < 0.85 or when an ambiguous payload
requires natural language semantic disambiguation.
Includes strict JSON parsing and graceful offline degradation.
"""

import os
import json
import requests
from typing import Dict, Any, Optional
from ceph_classifier.models import ClassificationResult, PayloadMetadata

OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma"

SYSTEM_PROMPT = """You are a Principal Ceph Storage Architect.
Given metadata about an uploaded payload, select the optimal Ceph storage interface:
- 'RBD': For virtual machine disks (.iso, .qcow2, .raw, .vmdk, .vdi), database block volumes needing low-latency 4K random I/O.
- 'RGW': For S3 object storage (media files, .parquet datasets, backup archives, static assets, WORM files).
- 'CephFS': For hierarchical directory trees, source code projects with .git/code, shared multi-client POSIX workspaces.
- 'RADOS': For raw byte shards, .omap key-values, and custom binary buffers.

Output valid JSON ONLY with these exact keys:
{
  "target_workflow": "RBD" | "RGW" | "CephFS" | "RADOS",
  "confidence": float (0.0 to 1.0),
  "rationale": "One concise sentence explaining the architectural reason.",
  "target_destination": "string (pool name or bucket or mount path)",
  "tuning_parameters": {"key": "value"}
}"""

class LLMOracle:
    def __init__(self, endpoint: str = OLLAMA_ENDPOINT, model: str = OLLAMA_MODEL):
        self.endpoint = endpoint
        self.model = model

    def query(self, metadata: PayloadMetadata, user_intent: Optional[str] = None, timeout: int = 5) -> Optional[Dict[str, Any]]:
        prompt_data = {
            "payload_name": os.path.basename(metadata.path),
            "is_directory": metadata.is_directory,
            "size_mb": metadata.size_mb,
            "mime_type": metadata.mime_type,
            "magic_detected": metadata.magic_signature,
            "file_count": metadata.file_count,
            "max_depth": metadata.max_depth,
            "has_project_markers": metadata.has_project_markers,
            "detected_markers": metadata.detected_markers,
            "user_intent": user_intent or "None provided"
        }
        
        user_prompt = f"Analyze this payload metadata and select the optimal Ceph storage interface:\n{json.dumps(prompt_data, indent=2)}"
        
        try:
            payload = {
                "model": self.model,
                "system": SYSTEM_PROMPT,
                "prompt": user_prompt,
                "stream": False,
                "options": {
                    "temperature": 0.1,
                    "top_p": 0.9
                }
            }
            resp = requests.post(self.endpoint, json=payload, timeout=timeout)
            if resp.status_code == 200:
                raw_text = resp.json().get("response", "").strip()
                if raw_text.startswith("```"):
                    raw_text = raw_text.strip("`").replace("json\n", "", 1).strip()
                parsed = json.loads(raw_text)
                return parsed
        except Exception:
            pass
            
        return None
