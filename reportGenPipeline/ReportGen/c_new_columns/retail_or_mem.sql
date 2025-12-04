ALTER TABLE public.car_wash_count
ADD COLUMN retail_or_mem TEXT;

ANALYZE public.car_wash_count;

UPDATE public.car_wash_count
SET retail_or_mem = CASE
    WHEN trans_state_name = 'Completed'
     AND item_department_name IN ('Wash', 'Recurring Plan')
     AND trans_type_name IN ('Sale', 'Return', 'Rewash')
     AND trans_item_state_name IN ('Normal', 'Price Override', 'Return', 'Comp Sale')
    THEN
        CASE
            WHEN item_department_name = 'Recurring Plan' THEN 'membership'
            WHEN is_recurring_redemption = '1' THEN 'membership'
            ELSE 'retail'
        END
    ELSE NULL
END;
