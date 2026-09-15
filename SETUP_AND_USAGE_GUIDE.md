# Ceph Autonomous Agent — Quickstart & Colleague Setup Guide

This guide walks you through setting up the agent on your machine, configuring the SSH connection to your Ceph Virtual Machine, and running storage workloads.

---

## 1. Prerequisites

- **Python 3.10+** installed
- **Git**
- A reachable Ceph cluster or VirtualBox VM with SSH and sudo access

---

## 2. Setup Instructions

### Step 1: Clone the Repository
```bash
git clone https://github.com/heuristic-solver/ceph-agent.git
cd ceph-agent
```

### Step 2: Create and Activate Virtual Environment

**On Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**On Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 3. Configure VM Connection (`.env`)

Copy the template file to `.env`:

**Windows (PowerShell):**
```powershell
Copy-Item .env.example .env
```
**Linux / macOS:**
```bash
cp .env.example .env
```

Open `.env` and configure your VM credentials:

```ini
# SSH configuration for your Ceph VM
VM_SSH_HOST=127.0.0.1
VM_SSH_PORT=2222
VM_SSH_USER=vboxuser
VM_SSH_PASSWORD=admin
VM_SUDO_PASSWORD=admin

# Optional: Local Ollama URL (defaults to localhost:11434)
OLLAMA_HOST=http://127.0.0.1:11434
```

> **VirtualBox Tip:** If running a VirtualBox VM with NAT networking:
> - Go to **VM Settings $\rightarrow$ Network $\rightarrow$ Advanced $\rightarrow$ Port Forwarding**.
> - Ensure **Host Port 2222** maps to **Guest Port 22**.
> - Set `VM_SSH_HOST=127.0.0.1` and `VM_SSH_PORT=2222`.
> - If using Bridged Networking, set `VM_SSH_HOST=<your-vm-ip>` and `VM_SSH_PORT=22`.

---

## 4. How to Run the Agent

The agent automatically inspects input files/folders and provisions them to the right Ceph backend (**RBD**, **RGW / S3**, **CephFS**, or **RADOS**).

### A. Live Execution (Connected to VM)
Pass any file or directory path to orchestrate:

```bash
# Provision a virtual disk image to RBD (Block Device):
python -m ceph_agent.cli workload_samples/cirros-0.6.2-x86_64-disk.img

# Provision a project directory to CephFS (Shared POSIX Filesystem):
python -m ceph_agent.cli workload_samples/flask-main

# Provision an archive or media file to RGW (S3 Object Storage):
python -m ceph_agent.cli workload_samples/Hierarchy.png
```

### B. Dry-Run / Mock Mode (No VM Required)
To test the agent's logic and self-healing engine without connecting to a live cluster:
```bash
python -m ceph_agent.cli workload_samples/cirros-0.6.2-x86_64-disk.img --mock
```

### C. With Custom User Intent
You can optionally pass operational constraints or hints:
```bash
python -m ceph_agent.cli database.db --intent "active transactional database"
python -m ceph_agent.cli backup.tar.gz --intent "cold archive in s3"
```

### D. Structured JSON Output
For programmatic consumption:
```bash
python -m ceph_agent.cli workload_samples/Hierarchy.png --json
```

---

## 5. Standalone Classifier

If you just want to see how the agent classifies a payload without executing any commands:

```bash
python -m ceph_classifier.cli workload_samples/cirros-0.6.2-x86_64-disk.img
```

---

## 6. Running Tests

To verify that all modules, knowledge retrievers, and memory components are working:
```bash
pytest
```

---

## 7. Common Troubleshooting

1. **`SSH Preflight Failed: Authentication failed`**
   - Double-check `VM_SSH_USER` and `VM_SSH_PASSWORD` in your `.env`.
   - Make sure you can SSH manually using `ssh -p 2222 vboxuser@127.0.0.1`.

2. **`sudo: a password is required`**
   - Ensure `VM_SUDO_PASSWORD` in `.env` matches your VM user's sudo password.

3. **`Connection refused on port 2222`**
   - Ensure your VirtualBox VM is booted up and running.
   - Verify VirtualBox port forwarding for port 2222.
