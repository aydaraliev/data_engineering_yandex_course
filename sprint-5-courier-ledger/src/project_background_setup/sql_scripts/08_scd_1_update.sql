-- SCD1 dimension update for user id=42
UPDATE public.clients
SET login = 'arthur_dent'
WHERE client_id = 42
RETURNING client_id, name, login;
