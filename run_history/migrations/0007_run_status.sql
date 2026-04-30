ALTER TABLE pipeline_runs
  ADD COLUMN status TEXT NOT NULL DEFAULT 'in_progress'
    CHECK (status IN ('in_progress', 'success', 'failure', 'cancelled'));

UPDATE pipeline_runs
  SET status = CASE
    WHEN completed_at IS NULL THEN 'in_progress'
    WHEN cancelled THEN 'cancelled'
    WHEN success IS TRUE THEN 'success'
    ELSE 'failure'
  END;

ALTER TABLE pipeline_runs
  DROP COLUMN success,
  DROP COLUMN cancelled;
