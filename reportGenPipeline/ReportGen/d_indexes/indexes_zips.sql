-- Indexes for new_car_wash_zips table 1

ANALYZE new_car_wash_zips;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_zips_created_ts
--   ON new_car_wash_zips USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_zips_client_site
--   ON new_car_wash_zips (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_zips_retail_or_mem
  ON new_car_wash_zips (retail_or_mem);

ANALYZE new_car_wash_zips;