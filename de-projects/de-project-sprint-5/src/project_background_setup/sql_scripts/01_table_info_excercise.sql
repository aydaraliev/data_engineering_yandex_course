SELECT table_name,
       column_name,
       data_type,
       character_maximum_length,
       column_default,
       is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, column_name;


SELECT DISTINCT elem ->> 'product_name' AS product_name
FROM outbox,
     json_array_elements(event_value::json -> 'product_payments') AS elem
WHERE elem ->> 'product_name' IS NOT NULL;
