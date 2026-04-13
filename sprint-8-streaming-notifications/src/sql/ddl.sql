-- DDL scripts for the Sprint 8 project
-- Restaurant Subscribe Streaming Service

-- ============================================
-- Restaurant subscribers table (SOURCE)
-- Database: rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de
-- Already exists and is populated with data
-- ============================================
CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);

-- ============================================
-- Subscribers feedback table (DESTINATION)
-- Database: localhost:5432/de (inside the Docker container)
-- Created empty, populated by the streaming job
-- ============================================
CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
    id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);

-- Unique constraint for idempotency when foreachBatch fails
-- If the PG write succeeded but Kafka failed, duplicates will be rejected on retry
CREATE UNIQUE INDEX IF NOT EXISTS idx_feedback_unique
    ON public.subscribers_feedback(restaurant_id, adv_campaign_id, client_id, datetime_created);
