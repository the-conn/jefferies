# Observability & Operator Dashboards

## Goal

Give operators live visibility into Jefferies' system state and the ability to diagnose wedged pipelines without `kubectl exec`-ing into pods or running ad-hoc Postgres queries. Achieve this by leaning on the truth that already lives in our durable stores, not by encoding application-level metrics that duplicate it.

## Guiding Principle

**Postgres is the source of truth** for pipeline and node lifecycle (we established this during the resiliency work). Redis, RabbitMQ, and the Kubernetes API are operational layers. Each has a battle-tested Prometheus exporter that exposes everything the underlying system knows.

A live Grafana dashboard wired to those exporters plus log-based panels driven by our existing `tracing` events answers ~90% of operational questions with zero application code. We defer adding a `/metrics` endpoint to the backend until a concrete question emerges that none of these layers can answer — at which point we'll know exactly which counter or histogram is needed and why.

## Work Order

The tiers are independent; each delivers value on its own. Do them in priority order.

### Tier 1 — Postgres exporter (highest ROI)

`pipeline_runs` and `node_runs` are the canonical record of what happened. Most diagnostic questions reduce to a SQL query against these tables.

- [ ] Deploy `postgres_exporter` pointed at the Jefferies Postgres database
- [ ] Configure custom queries (`queries.yaml`) covering:
  - [ ] `pipeline_runs` count grouped by `status` (gauge)
  - [ ] `pipeline_runs` count where `status = 'in_progress' AND created_at < now() - interval '1 hour'` (wedged pipelines)
  - [ ] `pipeline_runs` count grouped by `tenant_slug, status` (per-tenant health)
  - [ ] `pipeline_runs` completion rate (`completed_at` rate over time, by status)
  - [ ] `node_runs` count grouped by `failure_reason` (which infra failures dominate)
  - [ ] `node_runs` median + p95 of `completed_at - started_at` per `node_definition->>'name'` (node-level latency)
  - [ ] `node_runs` count where `success IS NULL AND created_at < now() - interval '30 minutes'` (dispatched-but-unresolved)
- [ ] Build a "Pipeline Health" Grafana dashboard with the panels above
- [ ] Add a "Wedged Runs" alert rule firing when the in-progress >1h count is non-zero for >5min

### Tier 2 — Redis exporter

Operational health of our coordination cache. Lets us see when something Redis-side is the cause.

- [ ] Deploy `redis_exporter` against the Jefferies Redis instance
- [ ] Panels for:
  - [ ] Memory usage / fragmentation
  - [ ] Connected clients
  - [ ] Slow log entries
  - [ ] Key count by pattern: `jefferies:run:*:state` and `jefferies:run:*:lease`
  - [ ] Lease key TTL distribution (catches stale leases — the kind that would break orphan detection)
- [ ] No app changes needed

### Tier 3 — RabbitMQ exporter

Backplane health. Confirms the topic exchange + per-run queues are behaving.

- [ ] Enable RabbitMQ's built-in `rabbitmq_prometheus` plugin (a broker-side setting; no app change)
- [ ] Panels for:
  - [ ] Total queue count (we expect roughly one `jefferies.coordinator.{run_id}` per active coordinator)
  - [ ] Per-queue depth (catches a coordinator that's not draining)
  - [ ] Publish/deliver rates on the `jefferies.events` exchange
  - [ ] Connection churn (surfaces the auto-delete queue lifecycle)
- [ ] No app changes needed

### Tier 4 — Log-based panels (Loki / OpenShift logging)

The `tracing` events I added during the resiliency work are structured with consistent fields (`run_id`, `branch`, `stable_code`, sweep stats, etc.). LogQL panels turn them into time-series for free.

- [ ] Confirm Loki/OpenShift logging is collecting the `coordinator::reaper` and `coordinator::coordinator` log streams
- [ ] Panels for:
  - [ ] `count_over_time({app="jefferies"} |~ "Reaper: eager startup sweep beginning" [1h])` — reaper boots
  - [ ] `count_over_time({app="jefferies"} |~ "Postgres-driven sweep: examining" [5m])` — sweep cadence
  - [ ] Sweep stats trend: extract `reconciled`, `skipped_redis_state_present`, `errors` from the `Postgres-driven sweep: done` event
  - [ ] `count_over_time({app="jefferies"} |~ "Folding S3-observed completion via reconcile" [5m])` — S3 fold rate by `branch={"rehydrate"|"periodic"}`
  - [ ] `count_over_time({app="jefferies"} |~ "Reconstructing RunState from pipeline_definition" [1h])` — Postgres-driven recoveries
  - [ ] `count_over_time({app="jefferies"} |~ "Infrastructure failure detected" [5m])` grouped by `stable_code` — infra failure rate by reason
  - [ ] `count_over_time({app="jefferies"} |~ "Poke received with incomplete status.json; rejecting" [1h])` — Tube contract violations (W4)

### Tier 5 — Application `/metrics` endpoint (deferred; only on-demand)

Add this **only** if a concrete question emerges that Tiers 1–4 can't answer. Possible candidates:

- Per-tick S3 reconcile latency histogram (currently coarsely available via log timestamps)
- Internal coordinator channel depth
- Per-`RunStatus` transition timing

When the need arises:

- [ ] Add `metrics` + `metrics-exporter-prometheus` crates to the workspace
- [ ] Expose `/metrics` from the server (gated behind a config flag if we want to keep surface area minimal in dev)
- [ ] Add only the specific counter/histogram that answers the open question

Until that need exists, leave this tier alone.

## Diagnostic Playbook

Once Tiers 1–4 are in place, the playbook for "this pipeline looks stuck" becomes:

1. **Pipeline Health dashboard** — is the run in the wedged-runs panel?
2. **Postgres SQL panel** — what does `node_runs` show for it (success/failure_reason/timestamps)?
3. **Redis exporter** — does the run still have a state key? A lease key with a stale TTL?
4. **RabbitMQ exporter** — is there an active queue for the run? Backed up?
5. **Log panel** — what did the reaper say about it the last time it ran a sweep?

If those five answers don't pinpoint the issue, we have a real gap in observability — and that's the trigger to revisit Tier 5.

## Out of Scope (Today)

- Per-pipeline-definition cost accounting (would need pipeline metadata joined with K8s pod resource usage)
- Distributed tracing across coordinator → Tube → poke handler (OpenTelemetry; only if multi-pod handoff debugging gets harder)
- Alerting on tenant-specific SLOs (requires defining SLOs first)
