CREATE TABLE clients
(
    client_id INTEGER NOT NULL
        CONSTRAINT clients_pk PRIMARY KEY,
    name      TEXT    NOT NULL,
    login     TEXT    NOT NULL
);

CREATE TABLE products
(
    product_id INTEGER        NOT NULL
        CONSTRAINT products_pk PRIMARY KEY,
    name       TEXT           NOT NULL,
    price      NUMERIC(14, 2) NOT NULL
);

CREATE TABLE sales
(
    client_id  INTEGER        NOT NULL
        CONSTRAINT sales_clients_client_id_fk REFERENCES clients,
    product_id INTEGER        NOT NULL
        CONSTRAINT sales_products_product_id_fk REFERENCES products,
    amount     INTEGER        NOT NULL,
    total_sum  NUMERIC(14, 2) NOT NULL,
    CONSTRAINT sales_pk PRIMARY KEY (client_id, product_id)
);
