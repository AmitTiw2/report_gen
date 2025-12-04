-- Indexes for new_car_wash_gateexpress table 1

ANALYZE new_car_wash_gateexpress;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_gateexpress_created_ts
--   ON new_car_wash_gateexpress USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_gateexpress_client_site
--   ON new_car_wash_gateexpress (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_gateexpress_retail_or_mem
  ON new_car_wash_gateexpress (retail_or_mem);

ANALYZE new_car_wash_gateexpress;