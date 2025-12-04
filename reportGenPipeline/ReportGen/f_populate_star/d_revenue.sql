ANALYZE car_wash_count;
ANALYZE Site_Client;
ANALYZE Quarter_year;
ANALYZE Fact1_Wash_count;

WITH carwash_revenue_summary AS (
  SELECT
      carwash_data.client_id,
      carwash_data.site_id,
      EXTRACT(YEAR FROM carwash_data.created_ts)::int    AS year_num,
      EXTRACT(QUARTER FROM carwash_data.created_ts)::int AS quarter_num,
      SUM(CASE WHEN carwash_data.retail_or_mem = 'retail'     THEN carwash_data.gross_sales ELSE 0 END) AS revenue_retail,
      SUM(CASE WHEN carwash_data.retail_or_mem = 'membership' THEN carwash_data.gross_sales ELSE 0 END) AS revenue_membership
  FROM car_wash_count AS carwash_data
  WHERE carwash_data.retail_or_mem IN ('retail','membership')
  GROUP BY carwash_data.client_id, carwash_data.site_id, year_num, quarter_num
)
INSERT INTO Fact1_Wash_count
  (site_client_id, yq_id, Revenue_Retail, Revenue_Membership)
SELECT
  site_mapping.site_client_id,
  quarter_mapping.yq_id,
  carwash_revenue_summary.revenue_retail,
  carwash_revenue_summary.revenue_membership
FROM carwash_revenue_summary
JOIN Site_Client  AS site_mapping
  ON site_mapping.client_id = carwash_revenue_summary.client_id
 AND site_mapping.site_id   = carwash_revenue_summary.site_id
JOIN Quarter_year AS quarter_mapping
  ON quarter_mapping.year_num    = carwash_revenue_summary.year_num
 AND quarter_mapping.quarter_num = carwash_revenue_summary.quarter_num
ON CONFLICT (site_client_id, yq_id) DO UPDATE
SET
  Revenue_Retail     = EXCLUDED.Revenue_Retail,
  Revenue_Membership = EXCLUDED.Revenue_Membership;
