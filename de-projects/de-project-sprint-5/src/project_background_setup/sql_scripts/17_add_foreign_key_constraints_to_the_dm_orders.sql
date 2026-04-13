ALTER TABLE dds.dm_orders
    ADD CONSTRAINT dm_orders_user_id_fkey
        FOREIGN KEY (user_id) REFERENCES dds.dm_users (id);

ALTER TABLE dds.dm_orders
    ADD CONSTRAINT dm_orders_restaurant_id_fkey
        FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);

ALTER TABLE dds.dm_orders
    ADD CONSTRAINT dm_orders_timestamp_id_fkey
        FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id);