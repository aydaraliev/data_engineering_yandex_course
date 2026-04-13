ALTER TABLE dds.dm_products
    ADD CONSTRAINT dm_products_restaurant_id_fkey
        FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);
