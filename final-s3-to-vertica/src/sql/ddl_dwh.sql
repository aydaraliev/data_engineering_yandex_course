CREATE SCHEMA IF NOT EXISTS VT251126648744__DWH;

CREATE TABLE IF NOT EXISTS VT251126648744__DWH.global_metrics (
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC(18, 2),
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC(18, 2),
    cnt_accounts_make_transactions INT
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update;
