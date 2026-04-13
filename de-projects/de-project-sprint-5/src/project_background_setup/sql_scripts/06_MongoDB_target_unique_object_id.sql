ALTER TABLE stg.ordersystem_orders
    ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);

ALTER TABLE stg.ordersystem_restaurants
    ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);

ALTER TABLE stg.ordersystem_users
    ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);
