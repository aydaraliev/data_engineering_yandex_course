-- ================
-- STAGING: RESTAURANTS
-- ================

DROP TABLE IF EXISTS stg.ordersystem_restaurants CASCADE;

CREATE TABLE stg.ordersystem_restaurants
(
    id           SERIAL PRIMARY KEY, -- integer, NOT NULL
    object_id    VARCHAR   NOT NULL, -- character varying, NOT NULL
    object_value TEXT      NOT NULL, -- text, NOT NULL
    update_ts    TIMESTAMP NOT NULL  -- timestamp without time zone, NOT NULL
);

-- ================
-- STAGING: USERS
-- ================

DROP TABLE IF EXISTS stg.ordersystem_users CASCADE;

CREATE TABLE stg.ordersystem_users
(
    id           SERIAL PRIMARY KEY, -- integer, NOT NULL
    object_id    VARCHAR   NOT NULL, -- character varying, NOT NULL
    object_value TEXT      NOT NULL, -- text, NOT NULL
    update_ts    TIMESTAMP NOT NULL  -- timestamp without time zone, NOT NULL
);

-- ================
-- STAGING: ORDERS
-- ================

DROP TABLE IF EXISTS stg.ordersystem_orders CASCADE;

CREATE TABLE stg.ordersystem_orders
(
    id           SERIAL PRIMARY KEY, -- integer, NOT NULL
    object_id    VARCHAR   NOT NULL, -- character varying, NOT NULL
    object_value TEXT      NOT NULL, -- text, NOT NULL
    update_ts    TIMESTAMP NOT NULL  -- timestamp without time zone, NOT NULL
);
