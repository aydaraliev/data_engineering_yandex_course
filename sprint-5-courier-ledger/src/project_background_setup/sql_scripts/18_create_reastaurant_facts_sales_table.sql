DROP TABLE IF EXISTS dds.fct_product_sales;
-- Create the sales fact table fct_product_sales in the dds schema
CREATE TABLE IF NOT EXISTS dds.fct_product_sales
(
    id            SERIAL PRIMARY KEY,
    product_id    INTEGER        NOT NULL,
    order_id      INTEGER        NOT NULL,
    count         INTEGER        NOT NULL DEFAULT 0,
    price         NUMERIC(14, 2) NOT NULL DEFAULT 0,
    total_sum     NUMERIC(14, 2) NOT NULL DEFAULT 0,
    bonus_payment NUMERIC(14, 2) NOT NULL DEFAULT 0,
    bonus_grant   NUMERIC(14, 2) NOT NULL DEFAULT 0,
    CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
    CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
    CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
    CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
    CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0)
);
