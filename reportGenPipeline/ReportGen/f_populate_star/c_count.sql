

-- Create Fact1_Wash_count table if it doesn't exist
CREATE TABLE IF NOT EXISTS Fact1_Wash_count (
    fact_id BIGSERIAL PRIMARY KEY,
    site_client_id BIGINT NOT NULL,
    yq_id BIGINT NOT NULL,
    revenue_retail NUMERIC(18,2),
    revenue_membership NUMERIC(18,2),
    wash_count_retail INTEGER,
    wash_count_membership INTEGER
);

-- Create unique constraint to prevent duplicate combinations
CREATE UNIQUE INDEX IF NOT EXISTS idx_fact1_wash_count_unique ON Fact1_Wash_count(site_client_id, yq_id);

-- Create foreign key indexes for better join performance
CREATE INDEX IF NOT EXISTS idx_fact1_wash_count_site_client_id ON Fact1_Wash_count(site_client_id);
CREATE INDEX IF NOT EXISTS idx_fact1_wash_count_yq_id ON Fact1_Wash_count(yq_id);

-- Warm up stats
ANALYZE car_wash_count;
ANALYZE Site_Client;
ANALYZE Quarter_year;
ANALYZE Fact1_Wash_count;

-- JOIN version with proper Return handling
EXPLAIN (ANALYZE, BUFFERS)
WITH carwash_summary AS (
  SELECT
      carwash_data.client_id,
      carwash_data.site_id,
      EXTRACT(YEAR FROM carwash_data.created_ts)::int    AS year_num,
      EXTRACT(QUARTER FROM carwash_data.created_ts)::int AS quarter_num,
      -- Retail: +1 for Sale/Rewash, -1 for Return
      SUM(CASE 
          WHEN carwash_data.retail_or_mem = 'retail' THEN
              CASE WHEN carwash_data.trans_type_name = 'Return' THEN -1 ELSE 1 END
          ELSE 0 
      END) AS wash_count_retail,
      -- Membership: +1 for Sale/Rewash, -1 for Return
      SUM(CASE 
          WHEN carwash_data.retail_or_mem = 'membership' THEN
              CASE WHEN carwash_data.trans_type_name = 'Return' THEN -1 ELSE 1 END
          ELSE 0 
      END) AS wash_count_membership
  FROM car_wash_count AS carwash_data
  WHERE carwash_data.retail_or_mem IN ('retail','membership')
  GROUP BY carwash_data.client_id, carwash_data.site_id, year_num, quarter_num
)
INSERT INTO Fact1_Wash_count
  (site_client_id, yq_id, Wash_Count_Retail, Wash_Count_Membership)
SELECT
  site_mapping.site_client_id,
  quarter_mapping.yq_id,
  carwash_summary.wash_count_retail,
  carwash_summary.wash_count_membership
FROM carwash_summary
JOIN Site_Client  AS site_mapping
  ON site_mapping.client_id = carwash_summary.client_id
 AND site_mapping.site_id   = carwash_summary.site_id
JOIN Quarter_year AS quarter_mapping
  ON quarter_mapping.year_num    = carwash_summary.year_num
 AND quarter_mapping.quarter_num = carwash_summary.quarter_num
ON CONFLICT (site_client_id, yq_id) DO UPDATE
SET
  Wash_Count_Retail     = EXCLUDED.Wash_Count_Retail,
  Wash_Count_Membership = EXCLUDED.Wash_Count_Membership;
