"""
retriever.py
LLM-based Self-Healing Knowledge Retrieval and Remediation Engine.
Queries SQLite FTS5 multi-version Ceph knowledge base, performs semantic
ranking and candidate filtering, and invokes the local LLM to synthesize
precise, context-aware remediation proposals with safety guardrails.
"""

import re
import json
import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from ceph_agent.knowledge.schema import (
    QueryContext,
    RemediationProposal,
    KnowledgeChunk,
    HealthCheckEntry,
    CommandSpec
)
from ceph_agent.knowledge.llm_client import LLMRemediationClient

logger = logging.getLogger(__name__)
DEFAULT_DB_PATH = Path("agent_knowledge.db")


class RemediationRetriever:
    """Retrieves Ceph knowledge and synthesizes remediation actions using local LLM reasoning."""

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        llm_client: Optional[LLMRemediationClient] = None
    ):
        self.db_path = db_path
        self.llm_client = llm_client or LLMRemediationClient()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _sanitize_fts_query(self, query: str) -> str:
        """Converts raw error text into safe FTS5 query tokens."""
        cleaned = re.sub(r'[^\w\s]', ' ', query)
        tokens = [t.strip() for t in cleaned.split() if len(t.strip()) > 2]
        if not tokens:
            return "ceph"
        # Take the most informative tokens (limit to 8)
        return " OR ".join(tokens[:8])

    def search_health_checks(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Searches health check catalog via FTS5 and code matching."""
        conn = self._get_connection()
        cursor = conn.cursor()
        sanitized = self._sanitize_fts_query(query)

        # 1. Check for exact health code in text (e.g. OSD_DOWN, MDS_ALL_DOWN, POOL_APP_NOT_ENABLED)
        code_match = re.findall(r'\b[A-Z][A-Z0-9_]{3,}\b', query)
        exact_results = []
        if code_match:
            placeholders = ",".join("?" * len(code_match))
            cursor.execute(f"""
                SELECT code, heading, severity, summary, description, resolution_commands, source_url, ceph_version
                FROM health_checks
                WHERE code IN ({placeholders})
                LIMIT ?;
            """, (*code_match, limit))
            for row in cursor.fetchall():
                exact_results.append({
                    "code": row["code"],
                    "heading": row["heading"],
                    "severity": row["severity"],
                    "summary": row["summary"],
                    "description": row["description"],
                    "resolution_commands": json.loads(row["resolution_commands"]),
                    "source_url": row["source_url"],
                    "ceph_version": row["ceph_version"],
                    "score": 10.0
                })

        # 2. FTS5 Search
        cursor.execute("""
            SELECT hc.code, hc.heading, hc.severity, hc.summary, hc.description,
                   hc.resolution_commands, hc.source_url, hc.ceph_version,
                   bm25(health_checks_fts) as rank
            FROM health_checks_fts
            JOIN health_checks hc ON hc.code = health_checks_fts.code
            WHERE health_checks_fts MATCH ?
            ORDER BY rank
            LIMIT ?;
        """, (sanitized, limit))

        fts_results = []
        seen_codes = {r["code"] for r in exact_results}
        for row in cursor.fetchall():
            if row["code"] not in seen_codes:
                fts_results.append({
                    "code": row["code"],
                    "heading": row["heading"],
                    "severity": row["severity"],
                    "summary": row["summary"],
                    "description": row["description"],
                    "resolution_commands": json.loads(row["resolution_commands"]),
                    "source_url": row["source_url"],
                    "ceph_version": row["ceph_version"],
                    "score": abs(float(row["rank"]))
                })
                seen_codes.add(row["code"])

        conn.close()
        return exact_results + fts_results

    def search_command_specs(
        self,
        query: str,
        workflow: Optional[str] = None,
        limit: int = 8
    ) -> List[Dict[str, Any]]:
        """Searches command specs using FTS5 and workflow affinity."""
        conn = self._get_connection()
        cursor = conn.cursor()
        sanitized = self._sanitize_fts_query(query)

        cursor.execute("""
            SELECT cs.id, cs.command, cs.subsystem, cs.applicable_workflows,
                   cs.operational_intent, cs.resolves_errors_or_states, cs.preconditions,
                   cs.is_idempotent, cs.danger_level, cs.canonical_example,
                   cs.raw_syntax, cs.version_compat, cs.source,
                   bm25(command_specs_fts) as rank
            FROM command_specs_fts
            JOIN command_specs cs ON cs.id = command_specs_fts.id
            WHERE command_specs_fts MATCH ?
            ORDER BY rank
            LIMIT ?;
        """, (sanitized, limit * 2))

        results = []
        for row in cursor.fetchall():
            wfs = json.loads(row["applicable_workflows"])
            resolves = json.loads(row["resolves_errors_or_states"])
            score = abs(float(row["rank"]))

            # Boost score if matching target workflow; penalize incompatible cross-workflow matches
            if workflow and (workflow in wfs or "CLUSTER_OPS" in wfs or "RADOS" in wfs):
                score += 5.0
            elif workflow:
                score -= 10.0

            results.append({
                "id": row["id"],
                "command": row["command"],
                "subsystem": row["subsystem"],
                "applicable_workflows": wfs,
                "operational_intent": row["operational_intent"],
                "resolves_errors_or_states": resolves,
                "preconditions": row["preconditions"],
                "is_idempotent": bool(row["is_idempotent"]),
                "danger_level": row["danger_level"],
                "canonical_example": row["canonical_example"],
                "raw_syntax": row["raw_syntax"],
                "version_compat": row["version_compat"],
                "source": row["source"],
                "score": score
            })

        # Sort by boosted score
        results.sort(key=lambda x: x["score"], reverse=True)
        conn.close()
        return results[:limit]

    def search_doc_chunks(
        self,
        query: str,
        release: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Searches multi-version documentation chunks via FTS5."""
        conn = self._get_connection()
        cursor = conn.cursor()
        sanitized = self._sanitize_fts_query(query)

        query_sql = """
            SELECT dc.chunk_id, dc.ceph_release, dc.ceph_version, dc.title,
                   dc.section_heading, dc.subsystem, dc.applicable_workflows,
                   dc.topic_intent, dc.summary, dc.key_concepts,
                   dc.health_codes_discussed, dc.error_context, dc.source_url,
                   dc.content, dc.commands_mentioned,
                   bm25(doc_chunks_fts) as rank
            FROM doc_chunks_fts
            JOIN doc_chunks dc ON dc.chunk_id = doc_chunks_fts.chunk_id
            WHERE doc_chunks_fts MATCH ?
        """
        params = [sanitized]
        if release:
            query_sql += " AND dc.ceph_release = ?"
            params.append(release.lower())

        query_sql += " ORDER BY rank LIMIT ?;"
        params.append(limit)

        cursor.execute(query_sql, params)
        results = []
        for row in cursor.fetchall():
            results.append({
                "chunk_id": row["chunk_id"],
                "ceph_release": row["ceph_release"],
                "ceph_version": row["ceph_version"],
                "title": row["title"],
                "section_heading": row["section_heading"],
                "subsystem": row["subsystem"],
                "applicable_workflows": json.loads(row["applicable_workflows"]),
                "topic_intent": row["topic_intent"],
                "summary": row["summary"],
                "key_concepts": json.loads(row["key_concepts"]),
                "health_codes_discussed": json.loads(row["health_codes_discussed"]),
                "error_context": row["error_context"],
                "source_url": row["source_url"],
                "content": row["content"],
                "commands_mentioned": json.loads(row["commands_mentioned"]),
                "score": abs(float(row["rank"]))
            })

        conn.close()
        return results

    def query(
        self,
        context: QueryContext,
        working_memory: Optional[Any] = None,
        episodic_memory: Optional[Any] = None
    ) -> RemediationProposal:
        """
        Primary entry point for self-healing diagnosis.
        1. Queries Layer 2 Episodic Memory (past verified cluster fixes).
        2. Retrieves relevant Ceph knowledge from catalog and FTS5.
        3. Synthesizes an actionable RemediationProposal grounded by Working Memory slots.
        """
        # Tier 0: Check Episodic Memory for verified winning solutions from past runs
        if episodic_memory:
            recalled = episodic_memory.recall_winning_remediation(
                error_signature=context.stderr,
                workflow=context.workflow or "",
                step_name=context.failed_command or ""
            )
            if recalled:
                # Ensure this exact command hasn't already failed in this step
                step_identifier = context.failed_command or context.stderr[:40]
                if not (working_memory and working_memory.has_failed_previously(step_identifier, recalled.fix_command)):
                    if working_memory:
                        recalled.fix_command = working_memory.bind_slots(recalled.fix_command)
                    return recalled

        query_text = f"{context.stderr} {context.failed_command or ''} {context.cluster_health or ''}"

        # 1. Retrieve candidate assets from knowledge base
        health_checks = self.search_health_checks(query_text, limit=5)
        command_specs = self.search_command_specs(query_text, workflow=context.workflow, limit=8)
        doc_chunks = self.search_doc_chunks(query_text, limit=4)

        # 2. Invoke local LLM for diagnosis and parameter substitution
        llm_response = self.llm_client.diagnose_and_remediate(
            stderr=context.stderr,
            failed_command=context.failed_command,
            workflow=context.workflow,
            target_destination=context.target_destination,
            cluster_health=context.cluster_health,
            candidate_commands=command_specs,
            candidate_health_checks=health_checks,
            candidate_docs=doc_chunks
        )

        proposal = None
        if llm_response:
            proposal = RemediationProposal(
                fix_command=llm_response.get("fix_command", "ceph status"),
                danger_level=llm_response.get("danger_level", "read-only"),
                rationale=llm_response.get("rationale", "Diagnosed by local LLM Ceph expert."),
                doc_citation=llm_response.get("doc_citation", "Ceph Knowledge Base"),
                confidence=float(llm_response.get("confidence", 0.90)),
                applicable_workflow=llm_response.get("applicable_workflow", context.workflow or "CLUSTER_OPS"),
                health_code=llm_response.get("health_code"),
                idempotent=bool(llm_response.get("idempotent", True)),
                suggested_retries=1,
                llm_reasoning=llm_response.get("llm_reasoning")
            )
        else:
            # 3. Deterministic semantic synthesis fallback (guarantees offline resilience)
            proposal = self._synthesize_fallback_proposal(
                context, health_checks, command_specs, doc_chunks, working_memory=working_memory
            )

        # Apply Working Memory Slot-Filling to bind parameters
        if working_memory and proposal:
            proposal.fix_command = working_memory.bind_slots(proposal.fix_command)

        return proposal

    def _synthesize_fallback_proposal(
        self,
        context: QueryContext,
        health_checks: List[Dict[str, Any]],
        command_specs: List[Dict[str, Any]],
        doc_chunks: List[Dict[str, Any]],
        working_memory: Optional[Any] = None
    ) -> RemediationProposal:
        """Synthesizes high-confidence proposal when LLM is offline or cold-starting."""
        stderr_lower = context.stderr.lower()
        step_id = context.failed_command or context.stderr[:40]
        failed_cmds = working_memory.get_failed_commands(step_id) if working_memory else set()

        def is_cmd_failed(raw_candidate: str) -> bool:
            if not failed_cmds:
                return False
            bound = working_memory.bind_slots(raw_candidate) if working_memory else raw_candidate
            return (raw_candidate.strip() in failed_cmds) or (bound.strip() in failed_cmds)

        # Domain Pattern 1: OSD Down / Storage Offline
        if "osd_down" in stderr_lower or "1 osds down" in stderr_lower or "osd.0 down" in stderr_lower or "error opening pool" in stderr_lower and "timed out" in stderr_lower:
            osd_cmd = "ceph osd in 0 || systemctl restart ceph*@osd.0 || ceph orch daemon restart osd.0"
            if not is_cmd_failed(osd_cmd):
                return RemediationProposal(
                    fix_command=osd_cmd,
                    danger_level="moderate",
                    rationale="OSD daemon is down or marked out; triggering re-inclusion and service restart.",
                    doc_citation="Ceph OSD Troubleshooting Reference",
                    confidence=0.94,
                    applicable_workflow=context.workflow or "RBD",
                    health_code="OSD_DOWN",
                    idempotent=True,
                    llm_reasoning="Synthesized direct OSD recovery action."
                )

        # Domain Pattern 2: Disk Quota Exceeded (EDQUOT / [errno 122])
        if "disk quota exceeded" in stderr_lower or "edquot" in stderr_lower or "errno 122" in stderr_lower:
            quota_100m = "ceph osd pool set-quota {pool} max_bytes 104857600"
            quota_1g = "ceph osd pool set-quota {pool} max_bytes 1073741824"
            if not is_cmd_failed(quota_100m):
                chosen_quota = quota_100m
                quota_str = "100MB"
            elif not is_cmd_failed(quota_1g):
                chosen_quota = quota_1g
                quota_str = "1GB"
            else:
                chosen_quota = "ceph osd pool set-quota {pool} max_bytes 0"
                quota_str = "unlimited"

            return RemediationProposal(
                fix_command=chosen_quota,
                danger_level="moderate",
                rationale=f"Pool storage byte quota exhausted; expanding pool quota to {quota_str}.",
                doc_citation="Ceph Pool Quota Management Spec",
                confidence=0.95,
                applicable_workflow=context.workflow or "RADOS",
                health_code="POOL_FULL",
                idempotent=True,
                llm_reasoning="Synthesized dynamic pool quota expansion with escalation."
            )

        # Domain Pattern 2b: PG Limit Exceeded (ERANGE / mon_max_pg_per_osd)
        if "erange" in stderr_lower or "mon_max_pg_per_osd" in stderr_lower or "exceeds the mon_max_pg_per_osd" in stderr_lower or "cumulative pgs per osd" in stderr_lower:
            pg_limit_cmd = "ceph config set global mon_max_pg_per_osd 1000 && ceph config set global mon_pg_warn_max_per_osd 1000"
            if not is_cmd_failed(pg_limit_cmd):
                return RemediationProposal(
                    fix_command=pg_limit_cmd,
                    danger_level="moderate",
                    rationale="Cluster PG ceiling exceeded (mon_max_pg_per_osd); dynamically raising global PG threshold to 1000.",
                    doc_citation="Ceph PG and Pool Configuration Reference",
                    confidence=0.96,
                    applicable_workflow=context.workflow or "CLUSTER_OPS",
                    health_code="POOL_PG_NUM_NOT_POWER_OF_TWO",
                    idempotent=True,
                    llm_reasoning="Synthesized dynamic mon_max_pg_per_osd threshold expansion."
                )

        # Domain Pattern 3: CephX Caps / Permission Denied (EACCES / [errno 13])
        if ("permission denied" in stderr_lower or "errno 13" in stderr_lower or "auth_bad_caps" in stderr_lower) and "nosuchuser" not in stderr_lower:
            auth_cmd = "ceph auth caps client.admin mon 'allow *' osd 'allow *' mds 'allow *' mgr 'allow *'"
            if not is_cmd_failed(auth_cmd):
                return RemediationProposal(
                    fix_command=auth_cmd,
                    danger_level="moderate",
                    rationale="Client CephX capabilities missing read/write permissions; restoring administrative caps.",
                    doc_citation="CephX Authentication Reference",
                    confidence=0.93,
                    applicable_workflow=context.workflow or "CLUSTER_OPS",
                    health_code="AUTH_BAD_CAPS",
                    idempotent=True,
                    llm_reasoning="Synthesized CephX capability recovery."
                )

        # Domain Pattern 3b: RGW NoSuchUser / S3 Missing User
        if "nosuchuser" in stderr_lower or ("user" in stderr_lower and ("not found" in stderr_lower or "not find" in stderr_lower or "no user info" in stderr_lower)) or "could not fetch user info" in stderr_lower or "could not find user" in stderr_lower:
            user_cmd = "radosgw-admin user create --uid={user} --display-name='Ceph S3 User' || radosgw-admin user info --uid={user}"
            if not is_cmd_failed(user_cmd):
                return RemediationProposal(
                    fix_command=user_cmd,
                    danger_level="moderate",
                    rationale="S3 user is missing on RGW gateway; creating S3 user and access keys.",
                    doc_citation="Ceph RGW User Administration Reference",
                    confidence=0.94,
                    applicable_workflow="RGW",
                    health_code=None,
                    idempotent=True,
                    llm_reasoning="Synthesized RGW user provisioning."
                )

        # Domain Pattern 4: RGW HTTP Service Down / Connection Refused on port 80
        if "connection refused" in stderr_lower or "failed to connect to localhost port 80" in stderr_lower:
            rgw_restart_cmd = "ceph orch daemon restart rgw.single-zone.aikyastorvm.bsuutr || systemctl restart ceph*@rgw* || true"
            if not is_cmd_failed(rgw_restart_cmd):
                return RemediationProposal(
                    fix_command=rgw_restart_cmd,
                    danger_level="moderate",
                    rationale="RGW REST endpoint connection refused; restarting RGW gateway container.",
                    doc_citation="Ceph RGW Gateway Operational Reference",
                    confidence=0.92,
                    applicable_workflow="RGW",
                    health_code="RGW_DOWN",
                    idempotent=True,
                    llm_reasoning="Synthesized RGW service restart."
                )

        # Domain Pattern 5: CephFS MDS Failover / Offline
        if "no mds" in stderr_lower or "metadata server" in stderr_lower or "fs_with_failed_mds" in stderr_lower or "mds daemon" in stderr_lower or "joinable" in stderr_lower:
            mds_cmd = "ceph fs set {volume} joinable true"
            if not is_cmd_failed(mds_cmd):
                return RemediationProposal(
                    fix_command=mds_cmd,
                    danger_level="moderate",
                    rationale="CephFS joinable flag is disabled or MDS daemon offline; re-enabling filesystem joinability.",
                    doc_citation="CephFS MDS Recovery Reference",
                    confidence=0.93,
                    applicable_workflow="CephFS",
                    health_code="MDS_ALL_DOWN",
                    idempotent=True,
                    llm_reasoning="Synthesized CephFS joinable recovery."
                )

        # Domain Pattern 6: Payload Missing / Remote Ingestion Diagnostic
        if "payload" in stderr_lower and ("not found on remote" in stderr_lower or "upload or extraction failed" in stderr_lower):
            return RemediationProposal(
                fix_command="ls -la /tmp/",
                danger_level="read-only",
                rationale="Payload file or directory is missing from /tmp on the Ceph node; inspecting /tmp contents.",
                doc_citation="Ceph Agent Ingestion Protocol",
                confidence=0.90,
                applicable_workflow=context.workflow or "CephFS",
                health_code=None,
                idempotent=True,
                llm_reasoning="Synthesized payload diagnostic inspection."
            )

        # Check for explicit health code mentioned in query
        explicit_codes = set(re.findall(r'\b[A-Z][A-Z0-9_]{3,}\b', f"{context.stderr} {context.cluster_health or ''}"))
        matched_hc = next((hc for hc in health_checks if hc.get("code") in explicit_codes and hc.get("resolution_commands")), None)

        if matched_hc:
            for cmd in matched_hc["resolution_commands"]:
                if not is_cmd_failed(cmd):
                    if working_memory:
                        cmd = working_memory.bind_slots(cmd)
                    if context.target_destination:
                        pool_part = context.target_destination.split("/")[0] if "/" in context.target_destination else context.target_destination
                        cmd = cmd.replace("<pool>", pool_part)
                        cmd = cmd.replace("<poolname>", pool_part)
                        cmd = cmd.replace("<fs-name>", context.target_destination.split("/")[-1])
                    return RemediationProposal(
                        fix_command=cmd,
                        danger_level="moderate" if any(w in cmd for w in ["set", "init", "restart", "apply", "create"]) else "read-only",
                        rationale=f"Addresses {matched_hc['code']}: {matched_hc['summary'][:150]}",
                        doc_citation=matched_hc.get("source_url", matched_hc["code"]),
                        confidence=0.92,
                        applicable_workflow=context.workflow or "CLUSTER_OPS",
                        health_code=matched_hc["code"],
                        idempotent=True,
                        llm_reasoning="Synthesized from verified health check resolution catalog."
                    )

        # Check for command specs matching specific error text in stderr
        direct_spec = None
        for s in command_specs:
            for err in s.get("resolves_errors_or_states", []):
                if err.lower() in stderr_lower:
                    direct_spec = s
                    break
            if direct_spec:
                break

        # Filter command specs by workflow compatibility
        compatible_specs = [
            s for s in command_specs
            if not context.workflow or context.workflow in s.get("applicable_workflows", [])
            or "CLUSTER_OPS" in s.get("applicable_workflows", [])
            or "RADOS" in s.get("applicable_workflows", [])
        ]

        chosen_spec = direct_spec or (compatible_specs[0] if compatible_specs else (command_specs[0] if command_specs else None))

        if chosen_spec:
            cmd = chosen_spec.get("canonical_example", chosen_spec.get("command", "ceph status"))
            if not is_cmd_failed(cmd):
                if working_memory:
                    cmd = working_memory.bind_slots(cmd)
                if context.target_destination:
                    pool_part = context.target_destination.split("/")[0] if "/" in context.target_destination else context.target_destination
                    cmd = cmd.replace("<pool>", pool_part)
                    cmd = cmd.replace("<poolname>", pool_part)
                    cmd = cmd.replace("<image-name>", context.target_destination.split("/")[-1])
                    cmd = cmd.replace("<uid>", "s3user")
                cmd_lower = cmd.lower()
                if any(w in cmd_lower for w in ["delete", "purge", "rm", "destroy", "erase", "format", "zap"]):
                    inferred_danger = "destructive"
                elif any(w in cmd_lower for w in ["dump", "status", "stat", "ls", "list", "get", "show", "tree", "df", "version", "info"]):
                    inferred_danger = "read-only"
                elif any(w in cmd_lower for w in ["set", "init", "create", "restart", "apply", "enable", "disable"]):
                    inferred_danger = "moderate"
                else:
                    inferred_danger = chosen_spec.get("danger_level", "read-only")

                return RemediationProposal(
                    fix_command=cmd,
                    danger_level=inferred_danger,
                    rationale=f"Selected {chosen_spec['id']} to resolve: {chosen_spec['operational_intent']}",
                    doc_citation=f"Command Spec: {chosen_spec['id']}",
                    confidence=0.88 if direct_spec else 0.78,
                    applicable_workflow=context.workflow or "CLUSTER_OPS",
                    health_code=None,
                    idempotent=chosen_spec.get("is_idempotent", True),
                    llm_reasoning="Synthesized from command specification operational intent."
                )

        # Default cluster diagnostics
        return RemediationProposal(
            fix_command="ceph health detail",
            danger_level="read-only",
            rationale="No specific error signature matched; querying cluster health detail.",
            doc_citation="Ceph Diagnostics Reference",
            confidence=0.60,
            applicable_workflow=context.workflow or "CLUSTER_OPS",
            health_code=None,
            idempotent=True,
            llm_reasoning="Default safe fallback diagnostics."
        )
