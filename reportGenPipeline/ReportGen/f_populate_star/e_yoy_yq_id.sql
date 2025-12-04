ANALYZE Quarter_year;

UPDATE Quarter_year AS current_quarter
SET yoy_q_yq_id = prior_quarter.yq_id
FROM Quarter_year AS prior_quarter
WHERE prior_quarter.year_num    = current_quarter.year_num - 1
  AND prior_quarter.quarter_num = current_quarter.quarter_num;
