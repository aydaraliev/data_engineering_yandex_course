-- Add columns for SCD Type 2
ALTER TABLE public.products
    ADD COLUMN valid_from timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN valid_to   timestamptz,
    ADD COLUMN is_current boolean     NOT NULL DEFAULT true;
