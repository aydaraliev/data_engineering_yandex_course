CREATE TABLE public.outbox
(
    id        SERIAL PRIMARY KEY,
    object_id INTEGER      NOT NULL,
    record_ts TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    type      VARCHAR(255) NOT NULL,
    payload   TEXT         NOT NULL
);