"""
tracker.py
State Machine and SQLite Trace Logger for Autonomous Ceph Agent.
Persists task states, step-by-step telemetry, error diagnoses, remediation actions,
and iteration budgets to 'agent_traces.db'.
"""

import json
import sqlite3
import datetime
from enum import Enum
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

TRACES_DB_PATH = Path("agent_traces.db")


class TaskState(str, Enum):
    """Execution state machine states."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    ERROR = "ERROR"
    DIAGNOSING = "DIAGNOSING"
    REMEDIATING = "REMEDIATING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"


@dataclass
class StepTrace:
    step_name: str
    state: str
    command: str
    stdout: str
    stderr: str
    exit_code: int
    duration_ms: int
    timestamp: str


@dataclass
class HealingTrace:
    step_name: str
    error_context: str
    remediation_command: str
    danger_level: str
    rationale: str
    confidence: float
    applied: bool
    timestamp: str


@dataclass
class TaskSummary:
    task_id: str
    payload_path: str
    workflow: str
    state: str
    created_at: str
    updated_at: str
    iteration_count: int
    steps_executed: int
    healing_actions_count: int
    summary: str


class ExecutionTracker:
    """Manages the persistence of agent execution state and history."""

    def __init__(self, db_path: Path = TRACES_DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                payload_path TEXT,
                workflow TEXT,
                state TEXT,
                created_at TEXT,
                updated_at TEXT,
                iteration_count INTEGER DEFAULT 0,
                summary TEXT
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS step_traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                step_name TEXT,
                state TEXT,
                command TEXT,
                stdout TEXT,
                stderr TEXT,
                exit_code INTEGER,
                duration_ms INTEGER,
                timestamp TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS healing_traces (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT,
                step_name TEXT,
                error_context TEXT,
                remediation_command TEXT,
                danger_level TEXT,
                rationale TEXT,
                confidence REAL,
                applied INTEGER,
                timestamp TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(task_id)
            );
        """)

        conn.commit()
        conn.close()

    def create_task(self, task_id: str, payload_path: str, workflow: str) -> None:
        """Initializes a new task trace."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        conn = self._get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO tasks (task_id, payload_path, workflow, state, created_at, updated_at, iteration_count, summary)
            VALUES (?, ?, ?, ?, ?, ?, 0, 'Task initialized');
        """, (task_id, payload_path, workflow, TaskState.PENDING.value, now, now))
        conn.commit()
        conn.close()

    def update_task_state(
        self,
        task_id: str,
        state: TaskState,
        iteration_count: Optional[int] = None,
        summary: Optional[str] = None
    ) -> None:
        """Updates the state and summary of a task."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        state_val = state.value if hasattr(state, "value") else str(state)
        conn = self._get_connection()

        if iteration_count is not None and summary is not None:
            conn.execute("""
                UPDATE tasks
                SET state = ?, updated_at = ?, iteration_count = ?, summary = ?
                WHERE task_id = ?;
            """, (state_val, now, iteration_count, summary, task_id))
        elif summary is not None:
            conn.execute("""
                UPDATE tasks
                SET state = ?, updated_at = ?, summary = ?
                WHERE task_id = ?;
            """, (state_val, now, summary, task_id))
        elif iteration_count is not None:
            conn.execute("""
                UPDATE tasks
                SET state = ?, updated_at = ?, iteration_count = ?
                WHERE task_id = ?;
            """, (state_val, now, iteration_count, task_id))
        else:
            conn.execute("""
                UPDATE tasks
                SET state = ?, updated_at = ?
                WHERE task_id = ?;
            """, (state_val, now, task_id))

        conn.commit()
        conn.close()

    def record_step(
        self,
        task_id: str,
        step_name: str,
        state: TaskState,
        command: str,
        stdout: str,
        stderr: str,
        exit_code: int,
        duration_ms: int
    ) -> None:
        """Appends a step execution record to the trace."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        conn = self._get_connection()
        conn.execute("""
            INSERT INTO step_traces (task_id, step_name, state, command, stdout, stderr, exit_code, duration_ms, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (task_id, step_name, state.value, command, stdout, stderr, exit_code, duration_ms, now))
        conn.commit()
        conn.close()

    def record_healing(
        self,
        task_id: str,
        step_name: str,
        error_context: str,
        remediation_command: str,
        danger_level: str,
        rationale: str,
        confidence: float,
        applied: bool
    ) -> None:
        """Appends a self-healing action record to the trace."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        conn = self._get_connection()
        conn.execute("""
            INSERT INTO healing_traces (task_id, step_name, error_context, remediation_command, danger_level, rationale, confidence, applied, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, (task_id, step_name, error_context, remediation_command, danger_level, rationale, confidence, 1 if applied else 0, now))
        conn.commit()
        conn.close()

    def get_task_summary(self, task_id: str) -> Optional[TaskSummary]:
        """Fetches the aggregated summary for a task."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM tasks WHERE task_id = ?;", (task_id,))
        task_row = cursor.fetchone()
        if not task_row:
            conn.close()
            return None

        cursor.execute("SELECT count(*) FROM step_traces WHERE task_id = ?;", (task_id,))
        step_count = cursor.fetchone()[0]

        cursor.execute("SELECT count(*) FROM healing_traces WHERE task_id = ?;", (task_id,))
        healing_count = cursor.fetchone()[0]

        conn.close()
        return TaskSummary(
            task_id=task_row["task_id"],
            payload_path=task_row["payload_path"],
            workflow=task_row["workflow"],
            state=task_row["state"],
            created_at=task_row["created_at"],
            updated_at=task_row["updated_at"],
            iteration_count=task_row["iteration_count"],
            steps_executed=step_count,
            healing_actions_count=healing_count,
            summary=task_row["summary"]
        )
