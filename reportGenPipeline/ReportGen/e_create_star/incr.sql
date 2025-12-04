ALTER TABLE Quarter_year
  ADD COLUMN IF NOT EXISTS yoy_q_yq_id BIGINT;

ALTER TABLE Quarter_year
  ADD CONSTRAINT fk_quarter_year_yoy
  FOREIGN KEY (yoy_q_yq_id)
  REFERENCES Quarter_year (yq_id)
  ON DELETE SET NULL;