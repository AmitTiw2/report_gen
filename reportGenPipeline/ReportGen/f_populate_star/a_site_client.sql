-- Create Site_Client table if it doesn't exist
CREATE TABLE IF NOT EXISTS Site_Client (
    site_client_id BIGSERIAL PRIMARY KEY,
    site_id TEXT NOT NULL,
    client_id TEXT NOT NULL
);

-- Create unique constraint to prevent duplicate site_id, client_id combinations
CREATE UNIQUE INDEX IF NOT EXISTS idx_site_client_unique ON Site_Client(site_id, client_id);

ANALYZE Site_Client;
ANALYZE car_wash_count;

INSERT INTO Site_Client (site_id, client_id)
SELECT DISTINCT
  carwash_data.site_id,
  carwash_data.client_id
FROM car_wash_count AS carwash_data
WHERE carwash_data.site_id  IS NOT NULL
  AND carwash_data.client_id IS NOT NULL
ON CONFLICT (site_id, client_id) DO NOTHING;
