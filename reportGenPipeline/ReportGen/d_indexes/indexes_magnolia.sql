-- Indexes for new_car_wash_magnolia table 1

ANALYZE new_car_wash_magnolia;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_magnolia_created_ts
--   ON new_car_wash_magnolia USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_magnolia_client_site
--   ON new_car_wash_magnolia (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_magnolia_retail_or_mem
  ON new_car_wash_magnolia (retail_or_mem);

ANALYZE new_car_wash_magnolia;