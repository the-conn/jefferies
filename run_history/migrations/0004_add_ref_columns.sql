ALTER TABLE pipeline_runs
  ADD COLUMN branch TEXT,
  ADD COLUMN target_branch TEXT,
  ADD COLUMN tag TEXT;
