"""
cli.py
Command-line interface for running the Ceph Autonomous Self-Healing Workload Agent.
Usage:
    python -m ceph_agent.cli <payload_path> [--intent <intent>] [--mock] [--retries 5] [--json]
"""

import sys
import json
import argparse
from pathlib import Path
from ceph_agent.core.agent import CephSelfHealingAgent
from ceph_agent.core.ssh_executor import SSHExecutor, MockSSHExecutor
from ceph_agent.core.tracker import ExecutionTracker
from ceph_agent.knowledge.retriever import RemediationRetriever
from ceph_classifier.classifier import WorkflowClassifier


def main():
    parser = argparse.ArgumentParser(
        description="Ceph Autonomous Workload Orchestration & Self-Healing Agent CLI"
    )
    parser.add_argument(
        "payload",
        type=str,
        help="Path to workload payload (e.g. disk image, dataset, project directory, archive)"
    )
    parser.add_argument(
        "--intent",
        type=str,
        default=None,
        help="Optional explicit user intent or operational constraints"
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Run using Mock SSH executor (dry-run simulation without live VM)"
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Maximum self-healing retry budget (default: 5)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output structured JSON summary to stdout"
    )

    args = parser.parse_args()

    # Configure executor
    if args.mock:
        executor = MockSSHExecutor()
    else:
        executor = SSHExecutor()

    agent = CephSelfHealingAgent(
        classifier=WorkflowClassifier(),
        retriever=RemediationRetriever(),
        executor=executor,
        tracker=ExecutionTracker()
    )

    summary = agent.run(
        payload_path=args.payload,
        user_intent=args.intent,
        max_iterations=args.retries
    )

    if args.json:
        print(json.dumps(summary.to_dict(), indent=2))

    sys.exit(0 if summary.status == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
