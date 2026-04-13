WITH diff AS (SELECT 1
              FROM public_test.dm_settlement_report_actual a
                       FULL OUTER JOIN public_test.dm_settlement_report_expected e
                                       ON a.restaurant_id = e.restaurant_id
                                           AND a.settlement_year = e.settlement_year
                                           AND a.settlement_month = e.settlement_month
              WHERE a.restaurant_id IS NULL
                 OR e.restaurant_id IS NULL
                 OR a.orders_count IS DISTINCT FROM e.orders_count
                 OR a.orders_total_sum IS DISTINCT FROM e.orders_total_sum
                 OR a.orders_bonus_payment_sum IS DISTINCT FROM e.orders_bonus_payment_sum
                 OR a.orders_bonus_granted_sum IS DISTINCT FROM e.orders_bonus_granted_sum
                 OR a.order_processing_fee IS DISTINCT FROM e.order_processing_fee
                 OR a.restaurant_reward_sum IS DISTINCT FROM e.restaurant_reward_sum)
SELECT now()                           AS test_date_time,
       'test_01'                       AS test_name,
       NOT EXISTS (SELECT 1 FROM diff) AS test_result;
