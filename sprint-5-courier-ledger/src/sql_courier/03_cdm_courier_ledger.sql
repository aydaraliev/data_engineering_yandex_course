-- CDM: courier payout mart
CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger
(
    id                  SERIAL PRIMARY KEY,
    courier_id          text        NOT NULL, -- courier business identifier
    courier_name        text        NOT NULL,
    settlement_year     INTEGER     NOT NULL,
    settlement_month    SMALLINT    NOT NULL,
    orders_count        INTEGER     NOT NULL DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum    NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
    rate_avg            NUMERIC(3,2),
    order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
    courier_order_sum   NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
    courier_tips_sum    NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
    courier_reward_sum  NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
    created_at          timestamp   NOT NULL DEFAULT now(),
    updated_at          timestamp   NOT NULL DEFAULT now(),
    CONSTRAINT ux_dm_courier_ledger_bk UNIQUE (courier_id, settlement_year, settlement_month),
    CONSTRAINT ck_dm_courier_ledger_month CHECK (settlement_month BETWEEN 1 AND 12)
);

CREATE INDEX IF NOT EXISTS idx_dm_courier_ledger_period
    ON cdm.dm_courier_ledger (settlement_year, settlement_month);
