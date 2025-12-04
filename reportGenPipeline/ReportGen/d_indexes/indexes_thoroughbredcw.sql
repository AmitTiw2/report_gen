-- Indexes for new_car_wash_thoroughbredcw table 1

ANALYZE new_car_wash_thoroughbredcw;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_thoroughbredcw_created_ts
--   ON new_car_wash_thoroughbredcw USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_thoroughbredcw_client_site
--   ON new_car_wash_thoroughbredcw (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_thoroughbredcw_retail_or_mem
  ON new_car_wash_thoroughbredcw (retail_or_mem);

ANALYZE new_car_wash_thoroughbredcw;