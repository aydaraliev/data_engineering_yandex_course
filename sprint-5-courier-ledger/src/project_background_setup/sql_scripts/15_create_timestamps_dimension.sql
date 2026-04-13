-- Create the dm_timestamps table in the dds schema
CREATE TABLE IF NOT EXISTS dds.dm_timestamps
(
    id    SERIAL PRIMARY KEY,
    ts    TIMESTAMP NOT NULL,
    year  SMALLINT  NOT NULL CHECK (year >= 2022 AND year < 2500),
    month SMALLINT  NOT NULL CHECK (month >= 1 AND month <= 12),
    day   SMALLINT  NOT NULL CHECK (day >= 1 AND day <= 31),
    time  TIME      NOT NULL,
    date  DATE      NOT NULL
);
