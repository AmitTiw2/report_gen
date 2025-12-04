-- Indexes for new_car_wash_championxpresscw table 1

ANALYZE new_car_wash_championxpresscw;

-- CREATE INDEX IF NOT EXISTS brin_new_car_wash_championxpresscw_created_ts
--   ON new_car_wash_championxpresscw USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_new_car_wash_championxpresscw_client_site
  ON new_car_wash_championxpresscw (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_new_car_wash_championxpresscw_retail_or_mem
  ON new_car_wash_championxpresscw (retail_or_mem);

ANALYZE new_car_wash_championxpresscw;