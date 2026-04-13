ALTER TABLE cdm.dm_settlement_report
    DROP CONSTRAINT IF EXISTS dm_settlement_report_pkey;

ALTER TABLE cdm.dm_settlement_report
    ALTER COLUMN id SET NOT NULL,
    ALTER COLUMN id SET DEFAULT nextval('cdm.dm_settlement_report_id_seq'::regclass),
    ADD CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id);

SELECT n.nspname                         AS schema,
       t.relname                         AS table_name,
       c.conname                         AS constraint_name,
       pg_get_constraintdef(c.oid, true) AS constraint_def
FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE c.contype = 'c'
  AND n.nspname = 'cdm'
  AND t.relname = 'dm_settlement_report';

ALTER TABLE cdm.dm_settlement_report
    DROP CONSTRAINT IF EXISTS dm_settlement_report_settlement_date_check;

ALTER TABLE cdm.dm_settlement_report
    DROP CONSTRAINT IF EXISTS dm_settlement_report_year_check;

ALTER TABLE cdm.dm_settlement_report
    ADD CONSTRAINT dn_settlement_report_settlement_date_check
        CHECK (
            (settlement_date::date) >= DATE '2022-01-01'
                AND (settlement_date::date) < DATE '2500-01-01'
            );

ALTER TABLE cdm.dm_settlement_report
    ALTER COLUMN orders_count SET DEFAULT 0,
    ALTER COLUMN orders_total_sum SET DEFAULT 0,
    ALTER COLUMN orders_bonus_payment_sum SET DEFAULT 0,
    ALTER COLUMN orders_bonus_granted_sum SET DEFAULT 0,
    ALTER COLUMN order_processing_fee SET DEFAULT 0,
    ALTER COLUMN restaurant_reward_sum SET DEFAULT 0;

ALTER TABLE cdm.dm_settlement_report
    ADD CONSTRAINT chk_orders_count_nonnegative
        CHECK (orders_count >= 0),
    ADD CONSTRAINT chk_orders_total_sum_nonnegative
        CHECK (orders_total_sum >= 0),
    ADD CONSTRAINT chk_orders_bonus_payment_sum_nonnegative
        CHECK (orders_bonus_payment_sum >= 0),
    ADD CONSTRAINT chk_orders_bonus_granted_sum_nonnegative
        CHECK (orders_bonus_granted_sum >= 0),
    ADD CONSTRAINT chk_order_processing_fee_nonnegative
        CHECK (order_processing_fee >= 0),
    ADD CONSTRAINT chk_restaurant_reward_sum_nonnegative
        CHECK (restaurant_reward_sum >= 0);


ALTER TABLE cdm.dm_settlement_report
    ADD CONSTRAINT dm_settlement_report_restaurant_date_uniq
        UNIQUE (restaurant_id, settlement_date);
