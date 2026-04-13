DROP TABLE IF EXISTS dds.dm_users;

-- Create the dm_users table in the dds schema
CREATE TABLE IF NOT EXISTS dds.dm_users
(
    id         SERIAL PRIMARY KEY,
    user_id    VARCHAR NOT NULL,
    user_name  VARCHAR NOT NULL,
    user_login VARCHAR NOT NULL
);

-- Load data from the staging layer
INSERT INTO dds.dm_users (user_id, user_name, user_login)
SELECT object_value::json ->> 'id'    AS user_id,
       object_value::json ->> 'name'  AS user_name,
       object_value::json ->> 'login' AS user_login
FROM stg.ordersystem_users
ON CONFLICT DO NOTHING;
