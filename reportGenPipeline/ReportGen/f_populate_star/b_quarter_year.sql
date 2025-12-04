-- Create Quarter_year table if it doesn't exist
CREATE TABLE IF NOT EXISTS Quarter_year (
    yq_id BIGSERIAL PRIMARY KEY,
    year_num INT NOT NULL,
    quarter_num INT NOT NULL
);

-- Create unique constraint to prevent duplicate year/quarter combinations
CREATE UNIQUE INDEX IF NOT EXISTS idx_quarter_year_unique ON Quarter_year(year_num, quarter_num);

ANALYZE Quarter_year;
ANALYZE car_wash_count;

INSERT INTO Quarter_year (year_num, quarter_num)
SELECT DISTINCT
  EXTRACT(YEAR FROM carwash_data.created_ts)::int    AS year_num,
  EXTRACT(QUARTER FROM carwash_data.created_ts)::int AS quarter_num
FROM car_wash_count AS carwash_data
WHERE carwash_data.created_ts IS NOT NULL
ON CONFLICT (year_num, quarter_num) DO NOTHING;