INSERT INTO cdm.dm_settlement_report (restaurant_id,
                                      restaurant_name,
                                      settlement_date,
                                      orders_count,
                                      orders_total_sum,
                                      orders_bonus_payment_sum,
                                      orders_bonus_granted_sum,
                                      order_processing_fee,
                                      restaurant_reward_sum)
SELECT r.restaurant_id,
       r.restaurant_name,
       ts.date                                                 AS settlement_date,
       COUNT(DISTINCT o.id)                                    AS orders_count,
       COALESCE(SUM(fs.total_sum), 0)::numeric(14, 2)          AS orders_total_sum,
       COALESCE(SUM(fs.bonus_payment), 0)::numeric(14, 2)      AS orders_bonus_payment_sum,
       COALESCE(SUM(fs.bonus_grant), 0)::numeric(14, 2)        AS orders_bonus_granted_sum,
       -- комиссия 25% от общей суммы заказа
       (COALESCE(SUM(fs.total_sum), 0) * 0.25)::numeric(14, 2) AS order_processing_fee,
       -- restaurant_reward = total_sum - commission - bonus_payment
       (COALESCE(SUM(fs.total_sum), 0)
           - (COALESCE(SUM(fs.total_sum), 0) * 0.25)
           - COALESCE(SUM(fs.bonus_payment), 0)
           )::numeric(14, 2)                                   AS restaurant_reward_sum
FROM dds.fct_product_sales fs
         JOIN dds.dm_orders o ON fs.order_id = o.id
         JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
         JOIN dds.dm_timestamps ts ON o.timestamp_id = ts.id
-- только закрытые заказы
WHERE o.order_status ILIKE 'closed'
GROUP BY r.restaurant_id, r.restaurant_name, ts.date
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
    SET orders_count             = EXCLUDED.orders_count,
        orders_total_sum         = EXCLUDED.orders_total_sum,
        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
        order_processing_fee     = EXCLUDED.order_processing_fee,
        restaurant_reward_sum    = EXCLUDED.restaurant_reward_sum;