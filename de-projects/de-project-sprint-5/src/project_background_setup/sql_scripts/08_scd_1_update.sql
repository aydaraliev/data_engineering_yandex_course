-- Обновление измерения (SCD1) для пользователя id=42
UPDATE public.clients
SET login = 'arthur_dent'
WHERE client_id = 42
RETURNING client_id, name, login;
