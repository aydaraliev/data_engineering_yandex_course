CREATE SCHEMA IF NOT EXISTS VT251126648744__STAGING;

CREATE TABLE IF NOT EXISTS VT251126648744__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(50),
    status VARCHAR(30),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
)
ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::DATE;

CREATE TABLE IF NOT EXISTS VT251126648744__STAGING.currencies (
    date_update TIMESTAMP,
    currency_code INT,
    currency_code_with INT,
    currency_code_div NUMERIC(10, 5)
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_code) ALL NODES
PARTITION BY date_update::DATE;
