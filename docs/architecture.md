## **The Conn: Architecture Specification**

### **1. Core Philosophy**
* **Reactive & Distributed:** The system is a distributed state machine. Any server node in the cluster can progress a pipeline by interacting with the shared Redis state and RabbitMQ message backplane.
* **Stateless Ingress:** The **server** module is interchangeable. Traffic is balanced across replicas without losing track of active runs.
* **Resilient & Infrastructure-Blind:** The system assumes hardware and network failures. It self-heals via lease expiration and orphan reclamation, shielding the user from underlying instability.

---

### **2. Component & Module Breakdown**

#### **A. app_config (The Foundation)**
* **Layered Configuration:** Loads defaults from TOML and overrides them via environment variables using a double-underscore (`__`) separator for nested fields.
* **Secrets Management:** Handles sensitive credentials for **Redis**, **RabbitMQ**, and **GitHub** injected via OpenShift SecretKeyRefs.

#### **B. providers (The Gatekeeper)**
* **GitHub Logic:** Contains the HMAC-SHA256 signature verification logic to ensure webhooks are authentic.
* **Repository Access:** Manages GitHub App authentication (JWT and Installation Tokens) to read `.jefferies/` YAML files directly from the source repository via the Contents API.

#### **C. pipelines (The Logic)**
* **Discovery:** Parses YAML to match incoming events (e.g., `push`) to specific execution plans.
* **State Schema:** Defines the `NodeInfo` and `Pipeline` types that feed into the `RunState` stored in **Redis**.

#### **D. state_store (The Global Memory)**
* **Persistent State:** Stores durable `RunState` (node statuses, dependency graph, full pipeline definition) in **Redis** at `jefferies:run:{run_id}:state`.
* **Version Fencing:** All writes use a Lua CAS script; a save is rejected unless the caller's expected version matches the stored version, preventing split-brain writes from zombie servers.
* **Distributed Leases:** Manages per-run lease keys in Redis with configurable TTLs. Leases carry a monotonically increasing version number and must be renewed via heartbeat.

#### **E. backplane (The Cluster-Wide Signal Bus)**
* **Event Pub/Sub:** A RabbitMQ topic exchange (`jefferies.events`) with per-run auto-delete queues decouples event producers from consumers across server instances.
* **Events:** `NodeCompleted { node_name, success }` and `Cancel` — replacing all in-process MPSC channels.
* **Stateless Delivery:** Any server replica can publish an event; the coordinator holding the lease for that run will receive it regardless of which physical node it runs on.

#### **F. coordinator (The Reactor)**
* **Distributed Lifecycle:** Acquires a **Lease** in Redis before starting; renews it via heartbeat every 15 seconds. If renewal fails, the coordinator stops gracefully to yield to a new leader.
* **Version-Fenced Writes:** Persists `RunState` to Redis after every node transition using optimistic concurrency. A rejected write (version conflict) causes the coordinator to stop immediately.
* **Event Handling:** Consumes `NodeCompleted` and `Cancel` messages from the backplane, updates in-memory state, and dispatches newly unlocked nodes.
* **The Reaper:** A background task that identifies orphaned runs (Running nodes in Redis but no active lease) and reclaims them by re-acquiring the lease and resuming from persisted state.

#### **G. server (The Interface)**
* **Stateless Endpoints:** Axum handlers for GitHub webhooks and the secure status callbacks from **Tubes**.
* **Event Dispatch:** Validates requests and publishes the resulting state change to RabbitMQ, acting as the entry point for all external signals.

---

### **3. Data & Execution Flow**

1.  **Ingress:** A Webhook hits a **server** node. **providers** validates the signature and reads the pipeline YAML.
2.  **Initialization:** **coordinator** acquires a Redis lease and persists the initial `RunState`. All nodes with no dependencies are dispatched immediately.
3.  **Execution Loop:**
    * **Tubes** finishes a task and hits the **server** status endpoint.
    * The **server** publishes a `NodeCompleted` event to **RabbitMQ**.
    * The leasing **coordinator** reacts, updates the versioned Redis state, identifies "unlocked" nodes, and triggers the next Pods.
4.  **Cleanup:** Once all nodes reach a terminal state, the coordinator releases the lease and deletes the run state from Redis.

---

### **4. Resiliency & Error Handling**

| Scenario | System Response |
| :--- | :--- |
| **Server Instance Failure** | The Redis **Lease** TTL expires. The **Reaper** detects the orphaned run and re-acquires the lease on a healthy node. |
| **Network Partition / Zombie Server** | **Fencing Tokens** ensure that a zombie server cannot write outdated state to Redis once a new coordinator has established a higher-version lease. |
| **Worker Pod Failure** | **Tubes** reports a `Fail` status. The **coordinator** marks the node as failed and, if `fail_fast` is enabled, cancels the pipeline. |
| **Node Deletion** | A K8s Watcher in the background detects the missing Pod; the **coordinator** reconciles the state and marks it as a system error. |
| **Version Conflict** | The `state_store` rejects the write. The coordinator stops immediately to avoid split-brain execution. |

---

### **5. Key Operational Concepts**
* **Externalized State:** No local RAM or disk is used for run-time data. Redis and RabbitMQ provide the cluster's "shared brain."
* **Idempotent Dispatch:** Before starting a Pod, the system checks OpenShift to ensure a Pod for that `RunID/Step` doesn't already exist.
* **Rehydration:** Any **coordinator** can resume any pipeline by reading the current `RunState` snapshot from Redis, ensuring zero downtime during rolling updates of the backend.
