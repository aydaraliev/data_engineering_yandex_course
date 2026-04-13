-- DDS: сущности курьеров и доставок
CREATE SCHEMA IF NOT EXISTS dds;

-- Справочник курьеров (SCD2)
CREATE TABLE IF NOT EXISTS dds.dm_couriers
(
    id           SERIAL PRIMARY KEY,
    courier_id   text      NOT NULL,
    courier_name text      NOT NULL,
    active_from  timestamp NOT NULL,
    active_to    timestamp NOT NULL DEFAULT timestamp '2099-12-31',
    CONSTRAINT ux_dm_couriers_bk_from UNIQUE (courier_id, active_from),
    CONSTRAINT ck_dm_couriers_period CHECK (active_from < active_to)
);

-- Таблица доставок/рейсов, связывающая заказ и курьера
CREATE TABLE IF NOT EXISTS dds.dm_deliveries
(
    id          SERIAL PRIMARY KEY,
    delivery_id text UNIQUE,  -- business key, если есть
    order_id    INTEGER        NOT NULL REFERENCES dds.dm_orders (id),
    courier_id  INTEGER        NOT NULL REFERENCES dds.dm_couriers (id),
    delivery_ts timestamp      NOT NULL,
    order_sum   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    tip_sum     NUMERIC(14, 2) NOT NULL DEFAULT 0,
    rate        NUMERIC(3, 2),
    CONSTRAINT ck_dm_deliveries_sums CHECK (order_sum >= 0 AND tip_sum >= 0),
    CONSTRAINT ck_dm_deliveries_rate CHECK (rate IS NULL OR (rate >= 0 AND rate <= 5))
);

-- Добавляем ссылку на курьера в заказ, если отсутствует
ALTER TABLE IF EXISTS dds.dm_orders
    ADD COLUMN IF NOT EXISTS courier_id INTEGER;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'dm_orders_courier_id_fkey'
          AND table_schema = 'dds'
          AND table_name = 'dm_orders'
    ) THEN
        ALTER TABLE dds.dm_orders
            ADD CONSTRAINT dm_orders_courier_id_fkey
                FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id);
    END IF;
END $$;
