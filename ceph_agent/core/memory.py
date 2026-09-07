"""
memory.py
Two-Layer Cognitive Memory Architecture for Ceph Autonomous Agent.
Layer 1: WorkingMemory — In-task blackboard tracking live belief state, inferred facts,
         entity slot bindings ({pool}, {user}, {daemon}), and past attempt-observation pairs.
Layer 2: EpisodicMemory — Cross-task long-term recall querying 'agent_traces.db' for
         past proven cluster remediations.
"""

import re
import sqlite3
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set

from ceph_agent.knowledge.schema import RemediationProposal

logger = logging.getLogger(__name__)
DEFAULT_TRACES_DB = Path("agent_traces.db")


@dataclass
class AttemptRecord:
    """Represents a single command execution attempt within a task."""
    step_name: str
    attempt_idx: int
    command: str
    exit_code: int
    stdout: str
    stderr: str
    timestamp: str


class WorkingMemory:
    """
    In-Task Working Memory Blackboard.
    Maintains dynamic cluster belief state, entity slot bindings, and attempt history
    to prevent repetitive execution cycles ('hamster wheel') and ground remediation.
    """

    def __init__(self):
        # Dynamic facts inferred from cluster observations (e.g. {'s3_user_exists': True, 'osd_0_down': False})
        self.inferred_facts: Dict[str, Any] = {}

        # Entity slot bindings: {slot_name: value} (e.g. {'pool': 'rbd_pool_data', 'user': 'agent_s3_user'})
        self.slot_bindings: Dict[str, str] = {}

        # Chronological log of all attempts in the current task
        self.action_history: List[AttemptRecord] = []

    def set_slot(self, slot: str, value: str):
        """Registers an entity slot binding."""
        if slot and value:
            clean_slot = slot.strip("{}<> ")
            self.slot_bindings[clean_slot] = str(value).strip()

    def update_slots(self, bindings: Dict[str, str]):
        """Batch registers entity slot bindings."""
        for k, v in bindings.items():
            self.set_slot(k, v)

    def set_fact(self, key: str, value: Any):
        """Records an inferred cluster fact."""
        self.inferred_facts[key] = value

    def get_fact(self, key: str, default: Any = None) -> Any:
        return self.inferred_facts.get(key, default)

    def record_attempt(
        self,
        step_name: str,
        attempt_idx: int,
        command: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        timestamp: str = ""
    ) -> AttemptRecord:
        """Records an execution attempt into the action history and updates inferred facts."""
        record = AttemptRecord(
            step_name=step_name,
            attempt_idx=attempt_idx,
            command=command,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            timestamp=timestamp
        )
        self.action_history.append(record)

        # Infer facts from stderr / output patterns
        combined_text = f"{stdout} {stderr}".lower()
        if "user exists" in combined_text or "already exists" in combined_text:
            self.set_fact("user_already_exists", True)
        if "osd_down" in combined_text or "osds down" in combined_text:
            self.set_fact("osd_down", True)
        elif "active+clean" in combined_text:
            self.set_fact("osd_down", False)
        if "disk quota exceeded" in combined_text or "edquot" in combined_text:
            self.set_fact("quota_exhausted", True)
        if "permission denied" in combined_text or "accessdenied" in combined_text:
            self.set_fact("auth_caps_denied", True)

        # Extract dynamic slots from stderr if present
        pool_match = re.search(r"pool\s+['\"]?([a-zA-Z0-9_\-\.]+)['\"]?", stderr, re.IGNORECASE)
        if pool_match:
            self.set_slot("pool", pool_match.group(1))

        return record

    def get_failed_commands(self, step_name: str) -> Set[str]:
        """Returns set of commands that previously failed for this specific step."""
        return {
            rec.command.strip()
            for rec in self.action_history
            if rec.step_name == step_name and rec.exit_code != 0
        }

    def has_failed_previously(self, step_name: str, command: str) -> bool:
        """Checks if a command was already attempted and failed in the current step."""
        return command.strip() in self.get_failed_commands(step_name)

    def bind_slots(self, template: str) -> str:
        """
        Substitutes entity slot tokens in command templates with live values.
        Supports patterns like {pool}, <pool-name>, <pool>, {user}, <user-id>, {daemon}.
        """
        if not template:
            return template

        result = template
        for slot_name, slot_val in self.slot_bindings.items():
            patterns = [
                f"{{{slot_name}}}",
                f"<{slot_name}>",
                f"<{slot_name}-name>",
                f"<{slot_name}name>",
                f"<{slot_name}-id>",
                f"<{slot_name}id>",
                f"[{slot_name}]"
            ]
            for pat in patterns:
                result = result.replace(pat, slot_val)

        # Fallback heuristic replacements
        if "{pool}" in result or "<pool>" in result or "<pool-name>" in result or "<poolname>" in result:
            default_pool = self.slot_bindings.get("pool", "rbd_pool")
            result = re.sub(r"\{pool\}|<pool>|<pool-name>|<poolname>", default_pool, result)

        if "{daemon}" in result or "<daemon>" in result or "<daemon-name>" in result or "<daemonname>" in result:
            default_daemon = self.slot_bindings.get("daemon", "osd.0")
            result = re.sub(r"\{daemon\}|<daemon>|<daemon-name>|<daemonname>", default_daemon, result)

        if "{volume}" in result or "<volume>" in result or "{fs_name}" in result or "<fs-name>" in result or "{fs}" in result or "<fs>" in result:
            default_fs = self.slot_bindings.get("fs_name") or self.slot_bindings.get("volume") or self.slot_bindings.get("fs") or "cephfs"
            result = re.sub(r"\{volume\}|<volume>|\{fs_name\}|<fs-name>|\{fs\}|<fs>", default_fs, result)

        return result


class EpisodicMemory:
    """
    Cross-Task Long-Term Episodic Memory.
    Mines 'agent_traces.db' to recall past verified remediation commands that
    successfully resolved similar cluster faults on this VM.
    """

    def __init__(self, db_path: Path = DEFAULT_TRACES_DB):
        self.db_path = db_path

    def _get_connection(self) -> Optional[sqlite3.Connection]:
        if not self.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.warning("Could not connect to traces db for episodic memory: %s", e)
            return None

    def recall_winning_remediation(
        self,
        error_signature: str,
        workflow: str = "",
        step_name: str = ""
    ) -> Optional[RemediationProposal]:
        """
        Searches historical healing traces for past successful resolutions to similar errors.
        Returns a high-confidence RemediationProposal if an exact or strong pattern match is found.
        """
        conn = self._get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()

            # Stop words that cause false positive episodic associations
            stop_words = {
                "error", "could", "failed", "cannot", "unable", "with", "from", "that", "this",
                "have", "been", "command", "return", "returned", "status", "exit", "code", "line",
                "admin", "sudo", "info", "data", "file", "directory"
            }

            # Clean error text to extract core signature tokens
            tokens = [
                re.escape(t.strip().lower())
                for t in re.sub(r"[^\w\s]", " ", error_signature).split()
                if len(t.strip()) > 3 and not t.isdigit() and t.strip().lower() not in stop_words
            ][:5]

            if not tokens:
                return None

            # Look for past successful heals where applied=1 and task was SUCCESS
            query = """
                SELECT h.remediation_command, h.danger_level, h.rationale, h.error_context, h.confidence, COUNT(*) as hit_count
                FROM healing_traces h
                JOIN tasks t ON h.task_id = t.task_id
                WHERE t.state = 'SUCCESS'
                  AND h.applied = 1
                  AND (
            """
            conditions = []
            params = []
            for token in tokens:
                conditions.append("LOWER(h.error_context) LIKE ?")
                params.append(f"%{token}%")

            # Require multi-token co-occurrence if multiple distinctive tokens exist
            if len(conditions) >= 2:
                query += " AND ".join(conditions[:2]) + ")"
                params = params[:2]
            else:
                query += " OR ".join(conditions) + ")"

            query += """
                GROUP BY h.remediation_command
                ORDER BY hit_count DESC, h.id DESC
                LIMIT 5;
            """

            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            sig_lower = error_signature.lower()
            is_missing_user = ("not find user" in sig_lower or "not found" in sig_lower or "nosuchuser" in sig_lower or "no user info" in sig_lower)
            is_existing_user = ("already exists" in sig_lower or "user exists" in sig_lower)

            for row in rows:
                err_ctx = (row["error_context"] or "").lower()
                rat_ctx = (row["rationale"] or "").lower()
                
                # Polarity guard: Don't treat "user missing" as "user already exists"
                if is_missing_user and ("already exists" in err_ctx or "already exists" in rat_ctx):
                    continue
                if is_existing_user and ("not found" in err_ctx or "nosuchuser" in err_ctx):
                    continue

                remed_cmd = row["remediation_command"].strip()
                logger.info(
                    "Episodic memory recall: Matched winning remediation '%s' (hits: %d)",
                    remed_cmd, row["hit_count"]
                )
                return RemediationProposal(
                    fix_command=remed_cmd,
                    danger_level=row["danger_level"] or "moderate",
                    rationale=f"Episodic Memory: Recalled past verified cluster fix ({row['hit_count']} successful past uses): {row['rationale']}",
                    doc_citation="SQLite Episodic Memory (agent_traces.db)",
                    confidence=0.95,
                    applicable_workflow=workflow or "CLUSTER_OPS",
                    idempotent=True,
                    llm_reasoning="Recalled from previous successful cluster remediation in episodic traces."
                )

        except Exception as e:
            logger.warning("Error querying episodic memory: %s", e)
        finally:
            conn.close()

        return None
