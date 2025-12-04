-- Indexes for new_car_wash_tsunamicarwash table 1

ANALYZE new_car_wash_tsunamicarwash;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_tsunamicarwash_created_ts
--   ON new_car_wash_tsunamicarwash USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_tsunamicarwash_client_site
--   ON new_car_wash_tsunamicarwash (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_tsunamicarwash_retail_or_mem
  ON new_car_wash_tsunamicarwash (retail_or_mem);

ANALYZE new_car_wash_tsunamicarwash;