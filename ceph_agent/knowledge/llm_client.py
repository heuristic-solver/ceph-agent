"""
llm_client.py
Local LLM interface for Ceph Knowledge Retrieval, Error Diagnosis, and Remediation Synthesis.
Uses Ollama (Gemma / Qwen) with structured JSON generation and fast-fail fallback.
"""

import os
import json
import logging
import requests
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

DEFAULT_OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
DEFAULT_TAGS_ENDPOINT = "http://localhost:11434/api/tags"
DEFAULT_MODEL = "gemma"

REMEDIATION_SYSTEM_PROMPT = """You are a Principal Ceph Storage Site Reliability Engineer (SRE).
Your mission is to analyze a Ceph cluster error, diagnose the root cause from the provided documentation & command specifications, and formulate the exact remediation command.

Guidelines:
1. Identify the exact failure mode (e.g., missing pool application, daemon offline, kernel module unmapped, user not found, quorum loss).
2. Select the optimal CLI fix command from the provided candidate specifications.
3. If placeholders exist in the canonical command (e.g., <pool>, <image>, <id>, <fs-name>), substitute them using the runtime context.
4. Assess the danger level:
   - 'read-only': Diagnostic commands (ceph status, rbd ls, etc.)
   - 'moderate': State recovery, pool init, enabling daemons, creating missing users
   - 'destructive': Removing pools, deleting snapshots, forced crush changes
5. Provide a clear, technical rationale citing the Ceph architecture reason.

Output valid JSON ONLY with these exact keys:
{
  "fix_command": "exact CLI command string",
  "danger_level": "read-only" | "moderate" | "destructive",
  "rationale": "Clear 1-2 sentence architectural explanation of the fix.",
  "doc_citation": "Health code, doc section, or command reference cited",
  "confidence": 0.0 to 1.0,
  "applicable_workflow": "RBD" | "RGW" | "CephFS" | "RADOS" | "CLUSTER_OPS",
  "health_code": "Optional health check code (e.g. OSD_DOWN, POOL_APP_NOT_ENABLED) or null",
  "idempotent": true | false,
  "llm_reasoning": "Step-by-step diagnostic breakdown"
}"""


class LLMRemediationClient:
    """Interfaces with local LLM for diagnosis and remediation synthesis."""

    def __init__(
        self,
        endpoint: str = DEFAULT_OLLAMA_ENDPOINT,
        model: str = DEFAULT_MODEL,
        timeout: float = 3.0
    ):
        self.endpoint = endpoint
        self.model = model
        self.timeout = timeout
        self._is_server_available: Optional[bool] = None

    def check_availability(self) -> bool:
        """Quick 500ms probe to check if Ollama server is alive."""
        try:
            resp = requests.get(DEFAULT_TAGS_ENDPOINT, timeout=0.5)
            self._is_server_available = (resp.status_code == 200)
        except Exception:
            self._is_server_available = False
        return self._is_server_available

    def diagnose_and_remediate(
        self,
        stderr: str,
        failed_command: Optional[str],
        workflow: Optional[str],
        target_destination: Optional[str],
        cluster_health: Optional[str],
        candidate_commands: List[Dict[str, Any]],
        candidate_health_checks: List[Dict[str, Any]],
        candidate_docs: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """Queries local LLM to synthesize diagnosis and structured remediation proposal."""
        # Fast-fail if Ollama is not active
        if self._is_server_available is False:
            return None

        prompt_payload = {
            "runtime_error": {
                "stderr": stderr,
                "failed_command": failed_command or "N/A",
                "target_workflow": workflow or "GENERAL",
                "target_destination": target_destination or "N/A",
                "cluster_health_summary": cluster_health or "N/A"
            },
            "candidate_commands": [
                {
                    "id": c.get("id"),
                    "command": c.get("command"),
                    "subsystem": c.get("subsystem"),
                    "intent": c.get("operational_intent"),
                    "danger": c.get("danger_level"),
                    "resolves": c.get("resolves_errors_or_states", [])[:4]
                }
                for c in candidate_commands[:6]
            ],
            "candidate_health_checks": [
                {
                    "code": h.get("code"),
                    "severity": h.get("severity"),
                    "summary": h.get("summary", "")[:180],
                    "resolution_commands": h.get("resolution_commands", [])[:3]
                }
                for h in candidate_health_checks[:4]
            ],
            "relevant_documentation": [
                {
                    "title": d.get("title"),
                    "section": d.get("section_heading"),
                    "summary": d.get("summary", "")[:200],
                    "error_context": d.get("error_context", "")
                }
                for d in candidate_docs[:3]
            ]
        }

        user_prompt = (
            f"Analyze this Ceph operational error and generate the optimal remediation proposal:\n"
            f"{json.dumps(prompt_payload, indent=2)}"
        )

        return self._call_ollama(self.model, user_prompt)

    def _call_ollama(self, model: str, prompt: str) -> Optional[Dict[str, Any]]:
        payload = {
            "model": model,
            "system": REMEDIATION_SYSTEM_PROMPT,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,
                "top_p": 0.9
            }
        }
        try:
            resp = requests.post(self.endpoint, json=payload, timeout=self.timeout)
            if resp.status_code == 200:
                raw_text = resp.json().get("response", "").strip()
                if raw_text.startswith("```"):
                    raw_text = raw_text.strip("`").replace("json\n", "", 1).strip()
                parsed = json.loads(raw_text)
                if "fix_command" in parsed and "rationale" in parsed:
                    return parsed
        except Exception as e:
            logger.debug("Ollama call failed: %s", e)
        return None
