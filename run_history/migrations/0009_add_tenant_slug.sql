ALTER TABLE pipeline_runs
    ADD COLUMN tenant_slug TEXT NULL;

CREATE INDEX pipeline_runs_tenant_slug_idx
    ON pipeline_runs (tenant_slug)
    WHERE tenant_slug IS NOT NULL;
