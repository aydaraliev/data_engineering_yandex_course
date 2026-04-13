-- STG: сырой поток из API курьерской службы
CREATE SCHEMA IF NOT EXISTS stg;

-- Курьеры (сырые документы)
CREATE TABLE IF NOT EXISTS stg.couriers_raw
(
    object_id    text PRIMARY KEY,     -- business key (courier_id)
    object_value jsonb    NOT NULL,    -- полный JSON
    update_ts    timestamp NOT NULL,   -- метка обновления из источника
    load_ts      timestamp NOT NULL DEFAULT now()
);

-- Доставки / поездки (сырые документы)
CREATE TABLE IF NOT EXISTS stg.courier_deliveries_raw
(
    object_id    text PRIMARY KEY,     -- business key (delivery_id или order_id)
    object_value jsonb    NOT NULL,
    update_ts    timestamp NOT NULL,
    load_ts      timestamp NOT NULL DEFAULT now()
);

-- Рестораны (сырые документы)
CREATE TABLE IF NOT EXISTS stg.courier_restaurants_raw
(
    object_id    text PRIMARY KEY,     -- business key (restaurant_id)
    object_value jsonb    NOT NULL,
    update_ts    timestamp NOT NULL,
    load_ts      timestamp NOT NULL DEFAULT now()
);
