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
* **`coordinator`**: The reactive engine that acquires and heartbeats a Redis lease, consumes backplane events, persists node-state transitions, and runs the "Reaper" task for reclaiming orphaned runs and sweeping stranded resources. Contains the **`SourceManager`**, which streams repository tarballs from GitHub directly to S3 and generates presigned URLs for worker access. The **`KubeDispatcher`** actuates each pipeline node by creating a ConfigMap with the user script and submitting a Kubernetes Job into a single namespace. Per-node YAML knobs (`privileged`, `cache_size`, `cpu`, `memory`, `volumes`) drive the security context, in-memory cache mount at `/tmp/cache`, deterministic resource requests/limits, and any additional `emptyDir` mounts the user declares (e.g. `/var/lib/containers` for buildah overlays). The workspace at `/workspace` is a plain disk-backed `emptyDir`. Sandboxing is conditional: privileged nodes run with `runtimeClassName: kata` under `jefferies-jobs-sa` for VM-level isolation, while non-privileged nodes run on the cluster's default runtime and ServiceAccount for full performance. A per-run **`PodWatcher`** observes Pod conditions through `kube::runtime::watcher`, surfacing infrastructure failures (`ImagePullBackOff`, `OOMKilled`, init-container errors, etc.) the moment Kubernetes reports them rather than waiting for the runtime timeout.
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

# Tenancy (per-tenant GitHub App credentials are loaded from the YAML file
# below, mounted via Vault. See "Tenancy Configuration" for the schema.)
JEFFERIES__TENANCY__PATH=/etc/jefferies/tenancy/tenants.yaml

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

# Pipeline Defaults (per-node overridable in YAML)
JEFFERIES__PIPELINE__DEFAULT_PIPELINE_TIMEOUT_SECS=3600
JEFFERIES__PIPELINE__DEFAULT_NODE_TIMEOUT_SECS=600
JEFFERIES__PIPELINE__DEFAULT_NODE_STARTUP_TIMEOUT_SECS=300
JEFFERIES__PIPELINE__FAIL_FAST=true

# Dex (human authentication via OIDC)
JEFFERIES__DEX__CLIENT_ID=...
JEFFERIES__DEX__SECRET=...
JEFFERIES__DEX__ISSUER=https://dex.the-conn.com
JEFFERIES__DEX__REDIRECT_URI=https://the-conn.com/api/auth/callback
JEFFERIES__DEX__POST_LOGIN_REDIRECT=/
```

---

### **Tenancy Configuration**

A single backend deployment can serve multiple GitHub Apps (one per tenant/org). Per-tenant credentials live in a YAML file mounted into the pod via the Vault Agent Injector — the same pattern used for pipeline secrets. The default mount path is `/etc/jefferies/tenancy/tenants.yaml`; override with `JEFFERIES__TENANCY__PATH`.

**File schema:**

```yaml
tenants:
  - slug: the-conn          # required; lowercase a-z, 0-9, '-'; max 63 chars; starts alnum
    display_name: "..."     # optional; surfaced to operators / UI
    provider: github        # discriminator (only `github` today)
    app_id: "12345"         # GitHub App ID
    webhook_secret: "..."   # GitHub App webhook secret
    private_key: |
      -----BEGIN RSA PRIVATE KEY-----
      ...
      -----END RSA PRIVATE KEY-----
```

Each tenant's GitHub App points its webhook URL at `https://<backend>/webhooks/github`. The backend resolves the tenant by reading the owner login out of the event payload (`repository.owner.login`, `organization.login`, or `installation.account.login` for installation events) and matching it against `tenant.slug`, so the slug must equal the GitHub organization login. HMAC is then verified using that tenant's `webhook_secret`. Multiple tenants may share a single GitHub App — they will share the same `app_id`, `private_key`, and `webhook_secret` values across their `tenants.yaml` entries.

Only **organization** owners are allowed. A webhook whose `repository.owner.type` (or `installation.account.type`) is `User` is dropped before tenant lookup, even if the login matches a slug. Personal forks of a tenant repo, repos owned by a user account whose login was inadvertently added to `tenants.yaml`, and Apps installed on personal accounts are all rejected by this guard.

**Vault mount (deployment annotations):**

```yaml
metadata:
  annotations:
    vault.hashicorp.com/agent-inject: "true"
    vault.hashicorp.com/agent-pre-populate-only: "true"
    vault.hashicorp.com/role: "jefferies-backend"

    vault.hashicorp.com/agent-inject-secret-tenants: "secret/data/jefferies/tenancy"
    vault.hashicorp.com/agent-inject-template-tenants: |
      {{- with secret "secret/data/jefferies/tenancy" -}}
      {{ .Data.data.yaml }}
      {{- end -}}
    vault.hashicorp.com/agent-inject-file-tenants: "tenants.yaml"
    vault.hashicorp.com/secret-volume-path-tenants: "/etc/jefferies/tenancy"
```

The Vault role (`jefferies-backend` above) must be bound in Vault's Kubernetes auth method to the backend's ServiceAccount + namespace, with a policy granting `read` on `secret/data/jefferies/tenancy`. Adding or rotating a tenant requires updating the Vault secret and restarting the backend pod (no hot-reload yet).

---

### **Human Authentication (Dex)**

Operator/UI authentication is delegated to **Dex**, deployed separately and registered as the OIDC provider for Jefferies. The backend is a standard OIDC client.

**Flow:**
1. The browser hits `GET /api/auth/login` (optionally with `?return_to=/some/path` — relative paths only). The backend generates state + PKCE + nonce, stores them in a server-side session record, and 302-redirects to Dex's authorize URL with scopes `openid profile email groups`.
2. Dex authenticates the user against GitHub (using its own GitHub OAuth credentials), receives the user's org memberships from the GitHub API, and 302-redirects back to `GET /api/auth/callback` with `code` and `state`.
3. The backend verifies state, exchanges the code for tokens (with PKCE), validates the ID token (signature, issuer, audience, nonce, exp), and reads the `groups` claim — a list of GitHub org logins.
4. The backend intersects `groups` with the registered tenants in `tenants.yaml`. If empty → 403 with `{ "error": "no_authorized_tenants" }`. Otherwise the user gets a session with `authorized_slugs` populated and `active_tenant_context` defaulted to the first match. Browser is 302'd to `return_to` (validated relative path) or `JEFFERIES__DEX__POST_LOGIN_REDIRECT`.

**Endpoints:**
- `GET /api/auth/login` — initiates the OIDC flow.
- `GET /api/auth/callback` — Dex redirects here.
- `POST /api/auth/logout` — clears the session.
- `GET /api/auth/me` — returns the current `AuthSession` JSON (`user_id`, `email`, `name`, `authorized_slugs`, `active_tenant_context`); 401 without a session.
- `POST /api/auth/active-tenant` — body `{ "slug": "..." }`; updates `active_tenant_context`. 403 if slug not in `authorized_slugs`.

**Sessions** are server-side, stored in the same Redis instance the rest of the backend uses (a dedicated `tower-sessions/` keyspace via `fred` — separate pool from the `state_store` deadpool). Session cookies are HttpOnly, Secure, SameSite=Lax, with a 7-day inactivity expiry.

**Tenant-scoped routes** live under `/api/{slug}/...`. Every request to one of these is gated by an `AuthorizedTenant` extractor that checks the session's `authorized_slugs` against the URL slug. The handlers additionally scope data by slug (e.g. `GET /api/{slug}/runs` filters `run_history` by `owner == slug`; `GET /api/{slug}/runs/{run_id}` returns 404 if the run's `tenant_slug` does not match). The Tube callback `POST /api/v1/runs/{run_id}/nodes/{node_name}/poke` and webhooks at `/webhooks/github` are public — they do not flow through the session/auth layer.

The deployment must make `JEFFERIES__DEX__ISSUER` reachable from the backend pod (the URL is used for both OIDC discovery and JWKS fetches during ID token validation).

---

## **Development and Scaling**

Because the **server** and **coordinator** modules are stateless and rely on externalized infrastructure, you can scale the deployment to any number of replicas. The system automatically handles load distribution and ensures that no pipeline is lost during rolling updates or node failures.
