-- Создание таблицы bonussystem_users
DROP TABLE IF EXISTS stg.bonussystem_users CASCADE;
DROP TABLE IF EXISTS stg.bonussystem_ranks CASCADE;
DROP TABLE IF EXISTS stg.bonussystem_events CASCADE;
DROP INDEX IF EXISTS stg.idx_bonussystem_events__event_ts;

-- Создание таблицы bonussystem_users
CREATE TABLE stg.bonussystem_users
(
    id            integer NOT NULL,
    order_user_id text    NOT NULL,
    CONSTRAINT pk_bonussystem_users PRIMARY KEY (id)
);

-- Создание таблицы bonussystem_ranks
CREATE TABLE stg.bonussystem_ranks
(
    id                    integer        NOT NULL,
    name                  varchar(2048)  NOT NULL,
    bonus_percent         numeric(19, 5) NOT NULL,
    min_payment_threshold numeric(19, 5) NOT NULL,
    CONSTRAINT pk_bonussystem_ranks PRIMARY KEY (id)
);

-- Создание таблицы bonussystem_events
CREATE TABLE stg.bonussystem_events
(
    id          integer   NOT NULL,
    event_ts    timestamp NOT NULL,
    event_type  varchar   NOT NULL,
    event_value text      NOT NULL,
    CONSTRAINT pk_bonussystem_events PRIMARY KEY (id)
);

-- Создание индекса для таблицы bonussystem_events
CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events (event_ts);
