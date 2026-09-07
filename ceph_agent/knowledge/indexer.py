"""
indexer.py
Ingests 1,610 Ceph documentation chunks, 99 health checks, and 284 command specs
into a high-performance SQLite Knowledge Database with FTS5 full-text indexing,
structured JSON metadata, and fast query indexes.
"""

import os
import json
import sqlite3
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from ceph_agent.knowledge.schema import KnowledgeChunk, HealthCheckEntry, CommandSpec

logger = logging.getLogger(__name__)

DB_PATH = Path("agent_knowledge.db")
CHUNKS_FILE = Path("corpus/agent_knowledge_chunks.jsonl")
HEALTH_CHECKS_FILE = Path("corpus/health_checks_catalog.json")
COMMAND_SPECS_FILE = Path("command_specs/agent_command_catalog.json")


class KnowledgeIndexer:
    """Manages the creation and population of the SQLite Knowledge Database."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path

    def init_database(self, reset: bool = False) -> sqlite3.Connection:
        """Initializes SQLite schema with tables, FTS5 virtual tables, and indexes."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        cursor = conn.cursor()

        if reset:
            cursor.execute("DROP TABLE IF EXISTS doc_chunks;")
            cursor.execute("DROP TABLE IF EXISTS doc_chunks_fts;")
            cursor.execute("DROP TABLE IF EXISTS health_checks;")
            cursor.execute("DROP TABLE IF EXISTS health_checks_fts;")
            cursor.execute("DROP TABLE IF EXISTS command_specs;")
            cursor.execute("DROP TABLE IF EXISTS command_specs_fts;")

        # ── 1. Document Chunks Table ─────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS doc_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id TEXT,
                ceph_release TEXT,
                ceph_version TEXT,
                title TEXT,
                section_heading TEXT,
                subsystem TEXT,
                applicable_workflows TEXT,
                topic_intent TEXT,
                summary TEXT,
                key_concepts TEXT,
                health_codes_discussed TEXT,
                error_context TEXT,
                source_url TEXT,
                content TEXT,
                commands_mentioned TEXT
            );
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_chunk_id ON doc_chunks(chunk_id);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_subsystem ON doc_chunks(subsystem);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_doc_release ON doc_chunks(ceph_release);
        """)

        # FTS5 for Document Chunks
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS doc_chunks_fts USING fts5(
                chunk_id UNINDEXED,
                title,
                section_heading,
                subsystem,
                topic_intent,
                summary,
                error_context,
                content,
                tokenize='porter unicode61'
            );
        """)

        # ── 2. Health Checks Table ───────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS health_checks (
                code TEXT PRIMARY KEY,
                heading TEXT,
                severity TEXT,
                summary TEXT,
                description TEXT,
                resolution_commands TEXT,
                source_url TEXT,
                ceph_version TEXT
            );
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_health_severity ON health_checks(severity);
        """)

        # FTS5 for Health Checks
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS health_checks_fts USING fts5(
                code,
                heading,
                summary,
                description,
                resolution_commands,
                tokenize='porter unicode61'
            );
        """)

        # ── 3. Command Specs Table ───────────────────────────────────────────
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS command_specs (
                id TEXT PRIMARY KEY,
                command TEXT,
                subsystem TEXT,
                applicable_workflows TEXT,
                operational_intent TEXT,
                resolves_errors_or_states TEXT,
                preconditions TEXT,
                is_idempotent INTEGER,
                danger_level TEXT,
                canonical_example TEXT,
                raw_syntax TEXT,
                version_compat TEXT,
                source TEXT
            );
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cmd_subsystem ON command_specs(subsystem);
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_cmd_danger ON command_specs(danger_level);
        """)

        # FTS5 for Command Specs
        cursor.execute("""
            CREATE VIRTUAL TABLE IF NOT EXISTS command_specs_fts USING fts5(
                id,
                command,
                subsystem,
                operational_intent,
                resolves_errors_or_states,
                preconditions,
                tokenize='porter unicode61'
            );
        """)

        conn.commit()
        return conn

    def index_all(
        self,
        chunks_path: Path = CHUNKS_FILE,
        health_path: Path = HEALTH_CHECKS_FILE,
        specs_path: Path = COMMAND_SPECS_FILE
    ) -> Dict[str, int]:
        """Indexes all knowledge assets into SQLite and populates FTS5 indices."""
        conn = self.init_database(reset=True)
        cursor = conn.cursor()

        stats = {"chunks": 0, "health_checks": 0, "command_specs": 0}

        # Clear existing data for fresh ingestion
        cursor.execute("DELETE FROM doc_chunks;")
        cursor.execute("DELETE FROM doc_chunks_fts;")
        cursor.execute("DELETE FROM health_checks;")
        cursor.execute("DELETE FROM health_checks_fts;")
        cursor.execute("DELETE FROM command_specs;")
        cursor.execute("DELETE FROM command_specs_fts;")

        # ── 1. Ingest Documentation Chunks ───────────────────────────────────
        if chunks_path.exists():
            with open(chunks_path, "r", encoding="utf-8") as f:
                chunk_rows = []
                chunk_fts_rows = []
                for line in f:
                    data = json.loads(line)
                    chunk_rows.append((
                        data["chunk_id"],
                        data.get("ceph_release", ""),
                        data.get("ceph_version", ""),
                        data.get("title", ""),
                        data.get("section_heading", ""),
                        data.get("subsystem", "general"),
                        json.dumps(data.get("applicable_workflows", [])),
                        data.get("topic_intent", ""),
                        data.get("summary", ""),
                        json.dumps(data.get("key_concepts", [])),
                        json.dumps(data.get("health_codes_discussed", [])),
                        data.get("error_context", ""),
                        data.get("source_url", ""),
                        data.get("content", ""),
                        json.dumps(data.get("commands_mentioned", []))
                    ))
                    chunk_fts_rows.append((
                        data["chunk_id"],
                        data.get("title", ""),
                        data.get("section_heading", ""),
                        data.get("subsystem", "general"),
                        data.get("topic_intent", ""),
                        data.get("summary", ""),
                        data.get("error_context", ""),
                        data.get("content", "")
                    ))

                cursor.executemany("""
                    INSERT INTO doc_chunks (
                        chunk_id, ceph_release, ceph_version, title, section_heading,
                        subsystem, applicable_workflows, topic_intent, summary,
                        key_concepts, health_codes_discussed, error_context,
                        source_url, content, commands_mentioned
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, chunk_rows)

                cursor.executemany("""
                    INSERT INTO doc_chunks_fts (chunk_id, title, section_heading, subsystem, topic_intent, summary, error_context, content)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, chunk_fts_rows)

                stats["chunks"] = len(chunk_rows)

        # ── 2. Ingest Health Checks Catalog ───────────────────────────────────
        if health_path.exists():
            with open(health_path, "r", encoding="utf-8") as f:
                health_data = json.load(f)
                health_rows = []
                health_fts_rows = []
                for code, item in health_data.items():
                    res_cmds = item.get("resolution_commands", [])
                    res_cmds_json = json.dumps(res_cmds)
                    res_cmds_str = " ".join(res_cmds)
                    health_rows.append((
                        code,
                        item.get("heading", code),
                        item.get("severity", "HEALTH_WARN"),
                        item.get("summary", ""),
                        item.get("description", ""),
                        res_cmds_json,
                        item.get("source_url", ""),
                        item.get("ceph_version", "17.2.8")
                    ))
                    health_fts_rows.append((
                        code,
                        item.get("heading", code),
                        item.get("summary", ""),
                        item.get("description", ""),
                        res_cmds_str
                    ))

                cursor.executemany("""
                    INSERT INTO health_checks VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """, health_rows)

                cursor.executemany("""
                    INSERT INTO health_checks_fts (code, heading, summary, description, resolution_commands)
                    VALUES (?, ?, ?, ?, ?);
                """, health_fts_rows)

                stats["health_checks"] = len(health_rows)

        # ── 3. Ingest Command Specs Catalog ───────────────────────────────────
        if specs_path.exists():
            with open(specs_path, "r", encoding="utf-8") as f:
                specs_data = json.load(f)
                spec_rows = []
                spec_fts_rows = []
                for s in specs_data:
                    resolves = s.get("resolves_errors_or_states", [])
                    resolves_json = json.dumps(resolves)
                    resolves_str = " ".join(resolves)
                    spec_rows.append((
                        s["id"],
                        s.get("command", ""),
                        s.get("subsystem", "general"),
                        json.dumps(s.get("applicable_workflows", [])),
                        s.get("operational_intent", ""),
                        resolves_json,
                        s.get("preconditions", ""),
                        1 if s.get("is_idempotent", True) else 0,
                        s.get("danger_level", "read-only"),
                        s.get("canonical_example", ""),
                        s.get("raw_syntax", ""),
                        s.get("version_compat", ""),
                        s.get("source", "ceph_vm_ground_truth")
                    ))
                    spec_fts_rows.append((
                        s["id"],
                        s.get("command", ""),
                        s.get("subsystem", "general"),
                        s.get("operational_intent", ""),
                        resolves_str,
                        s.get("preconditions", "")
                    ))

                cursor.executemany("""
                    INSERT INTO command_specs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, spec_rows)

                cursor.executemany("""
                    INSERT INTO command_specs_fts (id, command, subsystem, operational_intent, resolves_errors_or_states, preconditions)
                    VALUES (?, ?, ?, ?, ?, ?);
                """, spec_fts_rows)

                stats["command_specs"] = len(spec_rows)

        conn.commit()
        conn.close()
        return stats


def run_indexer():
    print("=" * 65)
    print("  CEPH AGENT: KNOWLEDGE BASE INGESTION (SQLite + FTS5)")
    print("=" * 65)
    indexer = KnowledgeIndexer()
    results = indexer.index_all()
    print(f"  [+] Ingested Document Chunks   : {results['chunks']:,}")
    print(f"  [+] Ingested Health Checks     : {results['health_checks']:,}")
    print(f"  [+] Ingested Command Specs     : {results['command_specs']:,}")
    print(f"  [+] SQLite Knowledge Base ready: {DB_PATH.resolve()}")
    print("=" * 65)
    return results


if __name__ == "__main__":
    run_indexer()
