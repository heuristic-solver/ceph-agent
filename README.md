# Ceph Autonomous Workload & Self-Healing Agent

An autonomous system for classifying storage payloads and orchestrating Ceph workflows (RBD, RGW/S3, CephFS, and native RADOS) with closed-loop self-healing and cognitive memory.

## Architecture

The system consists of five primary layers:

1. **Workload Perception & Classification (`ceph_classifier`)**:
   - Tier 1: Deterministic binary magic byte matching and POSIX directory inspection (<2ms).
   - Tier 2: Local LLM fallback oracle (Ollama / Gemma) for ambiguous payloads or semantic user intent.

2. **Execution DAG Recipes (`ceph_agent/core/recipes.py`)**:
   - Structured, ordered provisioning steps for RBD, RGW, CephFS, and RADOS workflows.

3. **Cognitive Memory Subsystem (`ceph_agent/core/memory.py`)**:
   - WorkingMemory: In-task blackboard tracking live facts, slot bindings ({pool}, {user}, {volume}), and quarantine for failed commands.
   - EpisodicMemory: Cross-task long-term recall querying historical successful cluster remediations from SQLite (`agent_traces.db`).

4. **Self-Healing Knowledge Retriever (`ceph_agent/knowledge`)**:
   - BM25 full-text indexing + domain pattern heuristics on Ceph operational knowledge base (`agent_knowledge.db`).
   - Generates grounded, parameterized remediation commands when errors occur during execution.

5. **State Tracking & Safety Guardrails (`ceph_agent/core/tracker.py`)**:
   - Full state machine logging to SQLite.
   - Safety checks that halt destructive operations for human confirmation and enforce retry iteration budgets.

## Prerequisites

- Python 3.10+
- Reachable Ceph cluster node or VM with SSH and sudo access (for live execution)
- (Optional) Ollama running locally if using LLM fallback classification

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/heuristic-solver/ceph-agent.git
   cd ceph-agent
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On Linux/macOS:
   source venv/bin/activate
   # On Windows (PowerShell):
   .\venv\Scripts\Activate.ps1
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure environment variables:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` to configure your Ceph VM/host SSH credentials:
   ```ini
   VM_SSH_HOST=127.0.0.1
   VM_SSH_PORT=2222
   VM_SSH_USER=vboxuser
   VM_SSH_PASSWORD=admin
   VM_SUDO_PASSWORD=admin
   ```

## Running the Agent

### 1. Live Execution on Ceph Cluster
Run the orchestrator on a workload payload:
```bash
python -m ceph_agent.cli workload_samples/cirros-0.6.2-x86_64-disk.img
```

### 2. With User Intent Override
Pass operational constraints or target hints:
```bash
python -m ceph_agent.cli dataset.parquet --intent "store in S3 default bucket"
```

### 3. Dry-Run / Mock Mode
Simulate execution without requiring a live SSH cluster connection:
```bash
python -m ceph_agent.cli workload_samples/flask-main.zip --mock
```

### 4. Structured JSON Output
Output machine-readable execution summary:
```bash
python -m ceph_agent.cli workload_samples/Hierarchy.png --mock --json
```

## Running the Classifier Standalone

Classify any file or directory directly:
```bash
python -m ceph_classifier.cli workload_samples/cirros-0.6.2-x86_64-disk.img
```

With custom intent:
```bash
python -m ceph_classifier.cli data.db --intent "active transactional write database"
```

## Running Tests

Run the test suite with pytest:
```bash
pytest
```
