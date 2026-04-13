-- Создание таблицы dm_products в схеме dds с поддержкой SCD 2
CREATE TABLE IF NOT EXISTS dds.dm_products
(
    id            SERIAL PRIMARY KEY,
    product_id    VARCHAR        NOT NULL,
    product_name  VARCHAR        NOT NULL,
    product_price NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (product_price >= 0),
    restaurant_id INTEGER        NOT NULL,
    active_from   TIMESTAMP      NOT NULL,
    active_to     TIMESTAMP      NOT NULL
);