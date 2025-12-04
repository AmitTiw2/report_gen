-- Indexes for new_car_wash_mrcleancw table 1

ANALYZE new_car_wash_mrcleancw;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_mrcleancw_created_ts
--   ON new_car_wash_mrcleancw USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_mrcleancw_client_site
--   ON new_car_wash_mrcleancw (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_mrcleancw_retail_or_mem
  ON new_car_wash_mrcleancw (retail_or_mem);

ANALYZE new_car_wash_mrcleancw;