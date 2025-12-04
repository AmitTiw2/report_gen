-- Indexes for car_wash_count table 1

ANALYZE car_wash_count;

-- CREATE INDEX IF NOT EXISTS brin_car_wash_count_created_ts
--   ON car_wash_count USING brin (created_ts);

-- CREATE INDEX IF NOT EXISTS idx_car_wash_count_client_site
  ON car_wash_count (client_id, site_id);

CREATE INDEX IF NOT EXISTS idx_car_wash_count_retail_or_mem
  ON car_wash_count (retail_or_mem);

ANALYZE car_wash_count;