DROP TABLE IF EXISTS cdm.dm_settlement_report;
CREATE TABLE cdm.dm_settlement_report
(
    id                       SERIAL PRIMARY KEY,
    restaurant_id            VARCHAR        NOT NULL,
    restaurant_name          VARCHAR        NOT NULL,
    settlement_date          DATE           NOT NULL,
    orders_count             INTEGER        NOT NULL,
    orders_total_sum         NUMERIC(14, 2) NOT NULL,
    orders_bonus_payment_sum NUMERIC(14, 2) NOT NULL,
    orders_bonus_granted_sum NUMERIC(14, 2) NOT NULL,
    order_processing_fee     NUMERIC(14, 2) NOT NULL,
    restaurant_reward_sum    NUMERIC(14, 2) NOT NULL
);
-- Diff expected -> actual
WITH diff1 AS (SELECT *
               FROM public_test.dm_settlement_report_expected
               EXCEPT ALL
               SELECT *
               FROM public_test.dm_settlement_report_actual),
-- Diff actual -> expected
     diff2 AS (SELECT *
               FROM public_test.dm_settlement_report_actual
               EXCEPT ALL
               SELECT *
               FROM public_test.dm_settlement_report_expected)
SELECT (SELECT COUNT(*) FROM diff1) AS missing_in_actual,
       (SELECT COUNT(*) FROM diff2) AS extra_in_actual;
