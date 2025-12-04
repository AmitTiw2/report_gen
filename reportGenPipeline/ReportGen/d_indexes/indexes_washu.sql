-- Indexes for new_car_wash_washu table 1

ANALYZE new_car_wash_washu;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_washu_created_ts
--   ON new_car_wash_washu USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_washu_client_site
--   ON new_car_wash_washu (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_washu_retail_or_mem
  ON new_car_wash_washu (retail_or_mem);

ANALYZE new_car_wash_washu;