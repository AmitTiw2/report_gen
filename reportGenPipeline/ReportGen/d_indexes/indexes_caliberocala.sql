-- Indexes for new_car_wash_caliberocala table 1

ANALYZE new_car_wash_caliberocala;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_caliberocala_created_ts
--   ON new_car_wash_caliberocala USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_caliberocala_client_site
--   ON new_car_wash_caliberocala (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_caliberocala_retail_or_mem
  ON new_car_wash_caliberocala (retail_or_mem);

ANALYZE new_car_wash_caliberocala;