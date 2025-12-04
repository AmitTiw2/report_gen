-- Indexes for new_car_wash_splash table 1

ANALYZE new_car_wash_splash;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_splash_created_ts
--   ON new_car_wash_splash USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_splash_client_site
--   ON new_car_wash_splash (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_splash_retail_or_mem
  ON new_car_wash_splash (retail_or_mem);

ANALYZE new_car_wash_splash;