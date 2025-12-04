ALTER TABLE public.car_wash_count
ADD COLUMN IF NOT EXISTS created_ts timestamptz;

UPDATE public.car_wash_count
SET created_ts = to_timestamp(created_date_utc, 'YYYY-MM-DD HH24:MI:SS') AT TIME ZONE 'UTC'
WHERE created_ts IS NULL
  AND created_date_utc ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$';

SELECT COUNT(*) AS null_count FROM public.car_wash_count
WHERE created_ts IS NULL;