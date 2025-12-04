ALTER TABLE public.car_wash_count
ADD COLUMN item_quantity_int INTEGER,
ADD COLUMN item_amount_currency NUMERIC(12,2);

UPDATE public.car_wash_count
SET item_quantity_int = CASE
        WHEN item_quantity ~ '^-?[0-9]+$' THEN item_quantity::INTEGER
        ELSE 0
    END,
    item_amount_currency = CASE
        WHEN item_amount ~ '^-?[0-9]+\.?[0-9]*$' THEN item_amount::NUMERIC(12,2)
        ELSE 0
    END;
