CREATE TABLE pipeline_runs (
    run_id              UUID        PRIMARY KEY,
    pipeline_name       TEXT        NOT NULL,
    owner               TEXT        NOT NULL,
    repo                TEXT        NOT NULL,
    sha                 TEXT        NOT NULL,
    trigger             TEXT        NOT NULL,
    pipeline_definition TEXT        NOT NULL,
    success             BOOLEAN     NOT NULL,
    cancelled           BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE node_runs (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES pipeline_runs(run_id),
    node_name       TEXT        NOT NULL,
    node_definition TEXT        NOT NULL,
    success         BOOLEAN     NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    output_log      TEXT,
    UNIQUE (run_id, node_name)
);
