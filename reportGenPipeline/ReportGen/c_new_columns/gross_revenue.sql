ALTER TABLE public.car_wash_count
ADD COLUMN gross_sales NUMERIC;

ANALYZE public.car_wash_count;

UPDATE public.car_wash_count
SET gross_sales = CASE
    WHEN trans_type_name IN ('Sale', 'Return', 'Rewash')
     AND trans_item_state_name IN ('Normal', 'Price Override', 'Comp Sale')
     AND item_department_name IN ('Wash', 'Recurring Plan', 'Vacuum Sales',
                                  'WashBook', 'GiftCard', 'Promotion/Coupon', 'Prepaid Wash')
    THEN
        CASE
            WHEN trans_state_name != 'Completed'
                THEN 0
            ELSE (item_quantity_int::NUMERIC * item_amount_currency::NUMERIC)
        END
    ELSE 0
END;
