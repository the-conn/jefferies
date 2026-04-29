ALTER TABLE pipeline_runs
  ADD COLUMN retry_of UUID REFERENCES pipeline_runs(run_id);
