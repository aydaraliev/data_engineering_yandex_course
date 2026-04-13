-- DDL скрипты для проекта Sprint 8
-- Сервис подписок на рестораны (Restaurant Subscribe Streaming Service)

-- ============================================
-- Таблица подписчиков ресторанов (ИСТОЧНИК)
-- База: rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de
-- Уже существует и заполнена данными
-- ============================================
CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);

-- ============================================
-- Таблица фидбэка подписчиков (НАЗНАЧЕНИЕ)
-- База: localhost:5432/de (внутри Docker контейнера)
-- Создаётся пустой, заполняется стримингом
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

-- Unique constraint для идемпотентности при сбоях foreachBatch
-- Если PG write прошёл, но Kafka упал - при retry дубли будут отклонены
CREATE UNIQUE INDEX IF NOT EXISTS idx_feedback_unique
    ON public.subscribers_feedback(restaurant_id, adv_campaign_id, client_id, datetime_created);
