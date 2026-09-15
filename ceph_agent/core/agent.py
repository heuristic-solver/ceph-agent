"""
agent.py
Autonomous Ceph Workload Orchestration and Self-Healing ReAct Agent.
Orchestrates:
1. Workload Perception & Classification (Magic headers, POSIX structure, LLM fallback)
2. Structured Workflow Execution DAGs (RBD, RGW, CephFS, RADOS)
3. SSH Execution on Ceph cluster / VM with sudo escalation
4. Autonomous LLM-based Error Diagnosis, Remediation Retrieval, and State Recovery
5. Persistent State Machine Tracking in SQLite with safety guardrails
"""

import os
import uuid
import time
import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from ceph_classifier.classifier import WorkflowClassifier
from ceph_classifier.models import ClassificationResult
from ceph_agent.core.recipes import get_workflow_recipe, ExecutionStep
from ceph_agent.core.ssh_executor import SSHExecutor, MockSSHExecutor, ExecutionResult
from ceph_agent.core.tracker import ExecutionTracker, TaskState, TaskSummary
from ceph_agent.knowledge.retriever import RemediationRetriever
from ceph_agent.knowledge.schema import QueryContext, RemediationProposal

logger = logging.getLogger(__name__)


@dataclass
class AgentExecutionSummary:
    """Final output report of the autonomous agent execution."""
    task_id: str
    payload_path: str
    target_workflow: str
    target_destination: str
    status: str  # 'SUCCESS' | 'FAILED' | 'AWAITING_CONFIRMATION'
    iteration_count: int
    steps_total: int
    steps_completed: int
    healing_actions_applied: List[Dict[str, Any]]
    final_message: str
    duration_ms: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "payload_path": self.payload_path,
            "target_workflow": self.target_workflow,
            "target_destination": self.target_destination,
            "status": self.status,
            "iteration_count": self.iteration_count,
            "steps_total": self.steps_total,
            "steps_completed": self.steps_completed,
            "healing_actions_applied": self.healing_actions_applied,
            "final_message": self.final_message,
            "duration_ms": self.duration_ms
        }


class CephSelfHealingAgent:
    """The central autonomous orchestrator for Ceph storage workflows and self-healing."""

    def __init__(
        self,
        classifier: Optional[WorkflowClassifier] = None,
        retriever: Optional[RemediationRetriever] = None,
        executor: Optional[SSHExecutor] = None,
        tracker: Optional[ExecutionTracker] = None
    ):
        self.classifier = classifier or WorkflowClassifier()
        self.retriever = retriever or RemediationRetriever()
        self.executor = executor or SSHExecutor()
        self.tracker = tracker or ExecutionTracker()

    def run(
        self,
        payload_path: str,
        user_intent: Optional[str] = None,
        max_iterations: int = 5
    ) -> AgentExecutionSummary:
        """
        Executes the autonomous closed-loop agent workflow on the provided payload.
        """
        start_time = time.time()
        task_id = f"task_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        healing_history = []
        iteration_count = 0

        print("\n" + "=" * 65)
        print(f"  CEPH AUTONOMOUS AGENT — TASK {task_id}")
        print(f"  Payload : {payload_path}")
        print(f"  Intent  : {user_intent or 'Auto-detect'}")
        print("=" * 65)

        # ── Stage 0: Cluster Preflight & Connectivity ────────────────────────
        print("\n[Stage 0: Cluster Preflight Check]")
        is_ready, preflight_msg, details = self.executor.test_connectivity(timeout=10)
        if not is_ready:
            print(f"  [!] Preflight Failure: {preflight_msg}")
            if not isinstance(self.executor, MockSSHExecutor):
                duration_ms = int((time.time() - start_time) * 1000)
                err_msg = f"SSH Preflight Failed: {preflight_msg}. Verify Ceph VM is running at {getattr(self.executor, 'host', '127.0.0.1')}:{getattr(self.executor, 'port', 2222)}."
                print(f"  [ERROR] {err_msg}")
                self.tracker.log_transition(
                    task_id=task_id,
                    from_state=TaskState.PENDING,
                    to_state=TaskState.FAILED,
                    reason=err_msg
                )
                return AgentExecutionSummary(
                    task_id=task_id,
                    payload_path=payload_path,
                    target_workflow="UNKNOWN",
                    target_destination="",
                    status="FAILED",
                    iteration_count=0,
                    steps_total=0,
                    steps_completed=0,
                    healing_actions_applied=[],
                    final_message=err_msg,
                    duration_ms=duration_ms
                )
        else:
            print(f"  --> Node       : {details.get('user')}@{details.get('host')}:{details.get('port')}")
            print(f"  --> Ceph Ver   : {details.get('ceph_version')}")
            print(f"  --> Cluster    : {details.get('cluster_health')}")

        # ── Step 1: Perception & Classification ──────────────────────────────
        print("\n[Stage 1: Workload Perception]")
        classification: ClassificationResult = self.classifier.classify(
            item_path=payload_path,
            user_intent=user_intent
        )
        print(f"  --> Target Workflow    : {classification.target_workflow}")
        print(f"  --> Confidence         : {classification.confidence * 100:.1f}% ({classification.decision_tier})")
        print(f"  --> Target Destination : {classification.target_destination}")
        print(f"  --> Architectural Plan : {classification.rationale}")

        self.tracker.create_task(
            task_id=task_id,
            payload_path=payload_path,
            workflow=classification.target_workflow
        )
        self.tracker.update_task_state(task_id, TaskState.RUNNING)

        # ── Step 2: Load Workflow Recipe DAG ─────────────────────────────────
        recipe: List[ExecutionStep] = get_workflow_recipe(
            workflow=classification.target_workflow,
            payload_path=payload_path,
            destination=classification.target_destination,
            tuning=classification.tuning_parameters
        )
        print(f"\n[Stage 2: Loaded Recipe DAG — {len(recipe)} Steps for {classification.target_workflow}]")
        for i, step in enumerate(recipe, 1):
            print(f"  {i}. [{step.name}] {step.description}")

        # Stream payload file/directory to remote VM if live connection
        if os.path.exists(payload_path) and not isinstance(self.executor, MockSSHExecutor):
            remote_tmp = f"/tmp/{os.path.basename(os.path.normpath(payload_path))}"
            if hasattr(self.executor, "upload_path"):
                if self.executor.upload_path(payload_path, remote_tmp):
                    print(f"  [+] Ingested Payload: {payload_path} -> {remote_tmp} on Ceph VM")
            elif hasattr(self.executor, "upload_file") and os.path.isfile(payload_path):
                if self.executor.upload_file(payload_path, remote_tmp):
                    print(f"  [+] Ingested Payload: {payload_path} -> {remote_tmp} on Ceph VM ({os.path.getsize(payload_path)} bytes)")

        # ── Step 3: Closed-Loop Execution with Self-Healing ──────────────────
        print("\n[Stage 3: Autonomous Execution & State Machine]")
        steps_completed = 0

        for step_idx, step in enumerate(recipe, 1):
            step_success = False

            while not step_success:
                print(f"\n  ({step_idx}/{len(recipe)}) Executing: {step.name}")
                print(f"      $ {step.command}")

                exec_result: ExecutionResult = self.executor.execute(
                    cmd=step.command,
                    timeout=step.timeout_sec
                )

                if exec_result.is_success:
                    print(f"      [OK] Completed ({exec_result.duration_ms}ms)")
                    self.tracker.record_step(
                        task_id=task_id,
                        step_name=step.name,
                        state=TaskState.RUNNING,
                        command=step.command,
                        stdout=exec_result.stdout,
                        stderr=exec_result.stderr,
                        exit_code=exec_result.exit_code,
                        duration_ms=exec_result.duration_ms
                    )
                    step_success = True
                    steps_completed += 1
                else:
                    iteration_count += 1
                    print(f"      [!] Step Failed (exit {exec_result.exit_code})")
                    if exec_result.stderr:
                        print(f"          stderr: {exec_result.stderr.splitlines()[-1]}")

                    self.tracker.record_step(
                        task_id=task_id,
                        step_name=step.name,
                        state=TaskState.ERROR,
                        command=step.command,
                        stdout=exec_result.stdout,
                        stderr=exec_result.stderr,
                        exit_code=exec_result.exit_code,
                        duration_ms=exec_result.duration_ms
                    )
                    self.tracker.update_task_state(task_id, TaskState.DIAGNOSING, iteration_count=iteration_count)

                    # ── Self-Healing Diagnosis via Knowledge Retriever ───────
                    print(f"      [*] Querying Ceph Self-Healing Knowledge Base (attempt {iteration_count}/{max_iterations})...")
                    query_ctx = QueryContext(
                        stderr=exec_result.stderr or "Command returned non-zero exit code",
                        exit_code=exec_result.exit_code,
                        workflow=classification.target_workflow,
                        target_destination=classification.target_destination,
                        failed_command=step.command
                    )

                    proposal: RemediationProposal = self.retriever.query(query_ctx)
                    print(f"          --> Diagnosis  : {proposal.rationale}")
                    print(f"          --> Fix Command: {proposal.fix_command}")
                    print(f"          --> Safety     : {proposal.danger_level.upper()} (Confidence: {proposal.confidence*100:.0f}%)")

                    # ── Safety Guardrails Check ──────────────────────────────
                    if proposal.danger_level == "destructive":
                        warning_msg = (
                            f"Execution paused for human confirmation. Proposed remediation '{proposal.fix_command}' "
                            f"is DESTRUCTIVE."
                        )
                        print(f"\n  [CAUTION] {warning_msg}")
                        self.tracker.record_healing(
                            task_id=task_id,
                            step_name=step.name,
                            error_context=exec_result.stderr,
                            remediation_command=proposal.fix_command,
                            danger_level=proposal.danger_level,
                            rationale=proposal.rationale,
                            confidence=proposal.confidence,
                            applied=False
                        )
                        self.tracker.update_task_state(task_id, TaskState.AWAITING_CONFIRMATION, summary=warning_msg)
                        return AgentExecutionSummary(
                            task_id=task_id,
                            payload_path=payload_path,
                            target_workflow=classification.target_workflow,
                            target_destination=classification.target_destination,
                            status=TaskState.AWAITING_CONFIRMATION.value,
                            iteration_count=iteration_count,
                            steps_total=len(recipe),
                            steps_completed=steps_completed,
                            healing_actions_applied=healing_history,
                            final_message=warning_msg,
                            duration_ms=int((time.time() - start_time) * 1000)
                        )

                    # ── Apply Remediation Action ─────────────────────────────
                    self.tracker.update_task_state(task_id, TaskState.REMEDIATING)
                    print(f"      [*] Applying remediation command on cluster: {proposal.fix_command}")

                    heal_result = self.executor.execute(proposal.fix_command, timeout=60)
                    healing_applied = heal_result.is_success
                    healing_history.append({
                        "step": step.name,
                        "failed_command": step.command,
                        "remediation": proposal.fix_command,
                        "danger_level": proposal.danger_level,
                        "rationale": proposal.rationale,
                        "applied_success": healing_applied
                    })

                    self.tracker.record_healing(
                        task_id=task_id,
                        step_name=step.name,
                        error_context=exec_result.stderr,
                        remediation_command=proposal.fix_command,
                        danger_level=proposal.danger_level,
                        rationale=proposal.rationale,
                        confidence=proposal.confidence,
                        applied=healing_applied
                    )

                    # ── Iteration Budget Guardrail Check ────────────────────
                    if iteration_count >= max_iterations:
                        fail_msg = f"Task failed: Exceeded maximum self-healing retry limit ({max_iterations}) at step '{step.name}'."
                        print(f"\n  [ERROR] {fail_msg}")
                        self.tracker.update_task_state(task_id, TaskState.FAILED, summary=fail_msg)
                        return AgentExecutionSummary(
                            task_id=task_id,
                            payload_path=payload_path,
                            target_workflow=classification.target_workflow,
                            target_destination=classification.target_destination,
                            status=TaskState.FAILED.value,
                            iteration_count=iteration_count,
                            steps_total=len(recipe),
                            steps_completed=steps_completed,
                            healing_actions_applied=healing_history,
                            final_message=fail_msg,
                            duration_ms=int((time.time() - start_time) * 1000)
                        )

                    # Transition to RETRYING and loop back
                    self.tracker.update_task_state(task_id, TaskState.RETRYING)
                    print(f"      [*] Retrying original step '{step.name}'...")

        # ── Step 4: Completion & Final Telemetry ─────────────────────────────
        total_duration = int((time.time() - start_time) * 1000)
        success_msg = f"Successfully provisioned {classification.target_workflow} workload across {steps_completed} steps."
        if healing_history:
            success_msg += f" (Autonomously resolved {len(healing_history)} fault{'s' if len(healing_history)>1 else ''})."

        print("\n" + "=" * 65)
        print("  TASK COMPLETED: SUCCESS")
        print(f"  {success_msg}")
        print(f"  Total Duration: {total_duration}ms | Healing Actions: {len(healing_history)}")
        print("=" * 65 + "\n")

        self.tracker.update_task_state(
            task_id=task_id,
            state=TaskState.SUCCESS,
            iteration_count=iteration_count,
            summary=success_msg
        )

        return AgentExecutionSummary(
            task_id=task_id,
            payload_path=payload_path,
            target_workflow=classification.target_workflow,
            target_destination=classification.target_destination,
            status=TaskState.SUCCESS.value,
            iteration_count=iteration_count,
            steps_total=len(recipe),
            steps_completed=steps_completed,
            healing_actions_applied=healing_history,
            final_message=success_msg,
            duration_ms=total_duration
        )
