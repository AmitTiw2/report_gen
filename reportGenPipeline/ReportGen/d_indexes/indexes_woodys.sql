-- Indexes for new_car_wash_woodys table 1

ANALYZE new_car_wash_woodys;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_woodys_created_ts
--   ON new_car_wash_woodys USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_woodys_client_site
--   ON new_car_wash_woodys (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_woodys_retail_or_mem
  ON new_car_wash_woodys (retail_or_mem);

ANALYZE new_car_wash_woodys;