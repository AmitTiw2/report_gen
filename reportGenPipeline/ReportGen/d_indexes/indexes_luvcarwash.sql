-- Indexes for new_car_wash_luvcarwash table 1

ANALYZE new_car_wash_luvcarwash;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_luvcarwash_created_ts
--   ON new_car_wash_luvcarwash USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_luvcarwash_client_site
--   ON new_car_wash_luvcarwash (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_luvcarwash_retail_or_mem
  ON new_car_wash_luvcarwash (retail_or_mem);

ANALYZE new_car_wash_luvcarwash;