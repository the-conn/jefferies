# Jefferies: The Conn CI/CD Platform Backend

**The Conn** is a reactive, distributed, and event-driven CI/CD platform designed for high-performance orchestration within OpenShift and Kubernetes environments. It utilizes a "Shared-Nothing" execution model to ensure that every build step is isolated, resilient, and horizontally scalable.

---

## **Core Architecture**

The platform is built as a distributed state machine where the execution logic is decoupled from individual server instances. By externalizing state to **Redis** and signaling to **RabbitMQ**, the system achieves arbitrary scaling and self-healing capabilities.

### **System Modules**
* **`app_config`**: Manages layered configuration loading from TOML and environment variables (using `__` as a hierarchy separator). Covers Redis, RabbitMQ, GitHub, S3/NooBaa, and PostgreSQL credentials.
* **`providers`**: Handles GitHub-specific integrations, including HMAC-SHA256 webhook validation and repository content discovery.
* **`pipelines`**: Manages the parsing of `.jefferies/` YAML files and defines the shared state schema for execution tracking.
* **`state_store`**: Provides a `StateStore` trait backed by Redis. Persists `RunState` with optimistic concurrency (Lua CAS / version fencing) and manages distributed TTL leases per run for exactly-once coordination.
* **`backplane`**: Provides a `Backplane` trait backed by RabbitMQ. Replaces in-process MPSC channels with a cluster-wide topic exchange so any server node can route `NodeCompleted` and `Cancel` events to the appropriate coordinator.
* **`coordinator`**: The reactive engine that acquires and heartbeats a Redis lease, consumes backplane events, persists node-state transitions, and runs the "Reaper" task for reclaiming orphaned runs and sweeping stranded resources. Contains the **`SourceManager`**, which streams repository tarballs from GitHub directly to S3 and generates presigned URLs for worker access. The **`KubeDispatcher`** actuates each pipeline node by creating a ConfigMap with the user script and submitting a Kubernetes Job into a single namespace; every Pod runs under the kata-containers `RuntimeClass` and `jefferies-jobs-sa`, with per-node `privileged`, `cache_size`, `workspace_size`, `cpu`, and `memory` knobs from the YAML driving its security context, in-memory cache mount at `/tmp/cache`, in-memory workspace mount at `/workspace`, and deterministic resource requests/limits. A per-run **`PodWatcher`** observes Pod conditions through `kube::runtime::watcher`, surfacing infrastructure failures (`ImagePullBackOff`, `OOMKilled`, init-container errors, etc.) the moment Kubernetes reports them rather than waiting for the runtime timeout.
* **`run_history`**: Persists every pipeline run and its constituent node runs to PostgreSQL. Records outcome, timestamps, trigger, raw pipeline YAML, and captured output logs. Written by the coordinator immediately before S3 artifact cleanup, while node status files are still available.
* **`server`**: A stateless Axum-based interface that handles incoming webhooks and secure status callbacks from execution workers.

---

## **Key Features**

### **1. Reactive "Scan-and-Release" Execution**
Instead of following a rigid, linear path, the system maintains a "To-Run" queue in Redis. Whenever a step completes, the **coordinator** immediately identifies and dispatches all downstream nodes whose dependencies have been met.

### **2. Distributed Resiliency**
* **Leasing & Fencing:** Every active run is protected by a Redis-backed lease with a TTL. Monotonically increasing fencing tokens ensure that only the current, valid coordinator can write state, preventing race conditions from "zombie" servers.
* **Self-Healing (The Reaper):** A 60-second lease-reclaim pass detects expired leases in Redis and re-enqueues affected runs for adoption by a healthy node. A separate 5-minute resource sweep reconciles incomplete cleanup: terminal runs whose Redis state was not deleted, and stranded Kubernetes Jobs / ConfigMaps whose Redis state has already been removed.
* **Infrastructure Failure Detection:** A per-run `PodWatcher` reports structured failures (`ImagePullBackOff`, `OOMKilled`, `ContainerCreateError`, init-container failures, `PodStartTimeout`) the moment Kubernetes observes them. The reason is persisted to `node_runs.failure_reason`; the API exposes a curated `user_message` for actionable causes. Per-node timeouts are split into a startup phase (Job creation → `Running`, default 300s) and a runtime phase (default 600s) so a Pod stuck `Pending` due to cluster pressure does not consume the runtime budget.

### **3. Jefferies Tubes (Execution Wrapper)**
All user code runs inside a "Ghost Binary" wrapper that handles the lifecycle of a container:
1.  **Initialize**: Fresh clone of the repository.
2.  **Pull**: Fetch required artifacts from S3-compatible storage (Noobaa).
3.  **Execute**: Run user-defined shell steps.
4.  **Push**: Upload resulting artifacts back to S3.
5.  **Signal**: Securely notify the server of completion.

---

## **Deployment Configuration**

The platform requires the following infrastructure to be available in the cluster:
* **Redis**: For persistent state and distributed locking.
* **RabbitMQ**: For the cluster-wide event backplane.
* **S3 Storage (Noobaa)**: For artifact persistence between steps.
* **PostgreSQL**: For durable run history (pipeline and node outcomes, logs, timestamps).

### **Environment Variables**
```bash
# Redis
JEFFERIES__REDIS__URL=...
JEFFERIES__REDIS__PASSWORD=...

# RabbitMQ
JEFFERIES__RABBITMQ__URL=...
JEFFERIES__RABBITMQ__USER=...
JEFFERIES__RABBITMQ__PASSWORD=...

# GitHub Integration
JEFFERIES__GITHUB__APP_ID=...
JEFFERIES__GITHUB__WEBHOOK_SECRET=...
JEFFERIES__GITHUB__PRIVATE_KEY=...

# S3 / NooBaa Storage
JEFFERIES__S3__ENDPOINT=...
JEFFERIES__S3__BUCKET=the-conn-runs
JEFFERIES__S3__ACCESS_KEY=...
JEFFERIES__S3__SECRET_KEY=...

# PostgreSQL (run history)
JEFFERIES__POSTGRES__HOST=...
JEFFERIES__POSTGRES__PORT=5432
JEFFERIES__POSTGRES__DB=...
JEFFERIES__POSTGRES__USERNAME=...
JEFFERIES__POSTGRES__PASSWORD=...

# Kubernetes Dispatcher
JEFFERIES__KUBERNETES__NAMESPACE=jefferies-jobs
JEFFERIES__KUBERNETES__TUBE_IMAGE=quay.io/the-conn/tube:latest
JEFFERIES__KUBERNETES__DEFAULT_NODE_IMAGE=fedora:45
JEFFERIES__KUBERNETES__SERVICE_ACCOUNT=jefferies-jobs-sa
JEFFERIES__KUBERNETES__RUNTIME_CLASS=kata
JEFFERIES__KUBERNETES__DEFAULT_CPU=1
JEFFERIES__KUBERNETES__DEFAULT_MEMORY=2Gi
JEFFERIES__KUBERNETES__DEFAULT_WORKSPACE_SIZE=2Gi

# Pipeline Defaults (per-node overridable in YAML)
JEFFERIES__PIPELINE__DEFAULT_PIPELINE_TIMEOUT_SECS=3600
JEFFERIES__PIPELINE__DEFAULT_NODE_TIMEOUT_SECS=600
JEFFERIES__PIPELINE__DEFAULT_NODE_STARTUP_TIMEOUT_SECS=300
JEFFERIES__PIPELINE__FAIL_FAST=true
```

---

## **Development and Scaling**

Because the **server** and **coordinator** modules are stateless and rely on externalized infrastructure, you can scale the deployment to any number of replicas. The system automatically handles load distribution and ensures that no pipeline is lost during rolling updates or node failures.
