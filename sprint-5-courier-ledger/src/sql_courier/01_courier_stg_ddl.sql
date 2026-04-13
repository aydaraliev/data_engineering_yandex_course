-- STG: raw feed from the courier service API
CREATE SCHEMA IF NOT EXISTS stg;

-- Couriers (raw documents)
CREATE TABLE IF NOT EXISTS stg.couriers_raw
(
    object_id    text PRIMARY KEY,     -- business key (courier_id)
    object_value jsonb    NOT NULL,    -- full JSON payload
    update_ts    timestamp NOT NULL,   -- update timestamp from the source
    load_ts      timestamp NOT NULL DEFAULT now()
);

-- Deliveries / trips (raw documents)
CREATE TABLE IF NOT EXISTS stg.courier_deliveries_raw
(
    object_id    text PRIMARY KEY,     -- business key (delivery_id or order_id)
    object_value jsonb    NOT NULL,
    update_ts    timestamp NOT NULL,
    load_ts      timestamp NOT NULL DEFAULT now()
);

-- Restaurants (raw documents)
CREATE TABLE IF NOT EXISTS stg.courier_restaurants_raw
(
    object_id    text PRIMARY KEY,     -- business key (restaurant_id)
    object_value jsonb    NOT NULL,
    update_ts    timestamp NOT NULL,
    load_ts      timestamp NOT NULL DEFAULT now()
);
