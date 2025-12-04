-- Indexes for new_car_wash_riptideneuse table 1

ANALYZE new_car_wash_riptideneuse;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_riptideneuse_created_ts
--   ON new_car_wash_riptideneuse USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_riptideneuse_client_site
--   ON new_car_wash_riptideneuse (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_riptideneuse_retail_or_mem
  ON new_car_wash_riptideneuse (retail_or_mem);

ANALYZE new_car_wash_riptideneuse;