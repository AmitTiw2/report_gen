-- Indexes for new_car_wash_bigdans table 1

ANALYZE new_car_wash_bigdans;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_bigdans_created_ts
--   ON new_car_wash_bigdans USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_bigdans_client_site
  ON new_car_wash_bigdans (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_bigdans_retail_or_mem
  ON new_car_wash_bigdans (retail_or_mem);

ANALYZE new_car_wash_bigdans;