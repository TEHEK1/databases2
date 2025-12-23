-- DDL для прототипа DWH MetroPulse (PostgreSQL/Greenplum)
-- Слои: staging, dwh

-- Схемы
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ========================================
-- STAGING (плоские сырые таблицы)
-- ========================================

CREATE TABLE IF NOT EXISTS staging.vehicle_positions_raw (
    event_ts        timestamp with time zone,
    vehicle_id      varchar(64),
    route_id        varchar(64),
    lat             numeric(10,7),
    lon             numeric(10,7),
    speed           numeric(10,3),
    heading         numeric(6,2),
    payload         jsonb,
    load_id         varchar(64),
    ingestion_ts    timestamp with time zone default now()
);

CREATE TABLE IF NOT EXISTS staging.users_raw (
    user_id       varchar(64),
    created_at    timestamp with time zone,
    city          varchar(64),
    lang          varchar(16),
    gender        varchar(16),
    birthdate     date,
    is_active     boolean,
    payload       jsonb,
    load_id       varchar(64),
    ingestion_ts  timestamp with time zone default now()
);

CREATE TABLE IF NOT EXISTS staging.routes_raw (
    route_id      varchar(64),
    name          varchar(256),
    transport_type varchar(32), -- bus/tram/metro
    base_fare     numeric(12,2),
    schema_json   jsonb,
    active_from   timestamp with time zone,
    active_to     timestamp with time zone,
    load_id       varchar(64),
    ingestion_ts  timestamp with time zone default now()
);

CREATE TABLE IF NOT EXISTS staging.trips_raw (
    trip_id        varchar(64),
    user_id        varchar(64),
    route_id       varchar(64),
    vehicle_id     varchar(64),
    start_ts       timestamp with time zone,
    end_ts         timestamp with time zone,
    start_stop_id  varchar(64),
    end_stop_id    varchar(64),
    distance_m     numeric(14,3),
    fare           numeric(12,2),
    status         varchar(32),
    load_id        varchar(64),
    ingestion_ts   timestamp with time zone default now()
);

CREATE TABLE IF NOT EXISTS staging.payments_raw (
    payment_id      varchar(64),
    user_id         varchar(64),
    route_id        varchar(64),
    trip_id         varchar(64),
    amount          numeric(12,2),
    currency        varchar(8),
    status          varchar(32),
    payment_method  varchar(32),
    paid_at         timestamp with time zone,
    load_id         varchar(64),
    ingestion_ts    timestamp with time zone default now()
);

-- ========================================
-- CORE DWH - ИЗМЕРЕНИЯ
-- ========================================

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_sk       integer primary key,
    full_date     date not null,
    day           smallint not null,
    week          smallint not null,
    month         smallint not null,
    quarter       smallint not null,
    year          integer not null,
    is_weekend    boolean not null
);

CREATE TABLE IF NOT EXISTS dwh.dim_time (
    time_sk     integer primary key,
    hh24mi      char(5) not null,
    hour        smallint not null,
    minute      smallint not null,
    second      smallint not null default 0
);

CREATE TABLE IF NOT EXISTS dwh.dim_user (
    user_sk       bigserial primary key,
    user_id       varchar(64) not null,
    city          varchar(64),
    lang          varchar(16),
    birthdate     date,
    gender        varchar(16),
    created_at    timestamp with time zone,
    is_active     boolean,
    effective_from timestamp with time zone not null default now(),
    effective_to   timestamp with time zone,
    is_current     boolean not null default true,
    constraint uq_dim_user_business unique (user_id, effective_from)
);

-- SCD2 измерение для маршрутов
CREATE TABLE IF NOT EXISTS dwh.dim_route (
    route_sk       bigserial primary key,
    route_id       varchar(64) not null,
    name           varchar(256),
    transport_type varchar(32),
    base_fare      numeric(12,2),
    schema_json    jsonb,
    active_flag    boolean default true,
    valid_from     timestamp with time zone not null default now(),
    valid_to       timestamp with time zone,
    is_current     boolean not null default true,
    constraint uq_dim_route_business unique (route_id, valid_from)
);

CREATE TABLE IF NOT EXISTS dwh.dim_vehicle (
    vehicle_sk     bigserial primary key,
    vehicle_id     varchar(64) not null,
    route_id       varchar(64),
    transport_type varchar(32),
    capacity       integer,
    vendor         varchar(64),
    commissioned_at timestamp with time zone,
    decommissioned_at timestamp with time zone,
    constraint uq_dim_vehicle unique (vehicle_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_stop (
    stop_sk    bigserial primary key,
    stop_id    varchar(64) not null,
    name       varchar(256),
    lat        numeric(10,7),
    lon        numeric(10,7),
    city       varchar(64),
    constraint uq_dim_stop unique (stop_id)
);

CREATE TABLE IF NOT EXISTS dwh.dim_payment_method (
    payment_method_sk bigserial primary key,
    code              varchar(32) not null,
    name              varchar(128),
    provider          varchar(64),
    constraint uq_dim_payment_method unique (code)
);

-- ========================================
-- CORE DWH - ФАКТЫ
-- ========================================

CREATE TABLE IF NOT EXISTS dwh.fact_trip (
    trip_sk         bigserial primary key,
    trip_id         varchar(64) not null,
    user_sk         bigint references dwh.dim_user(user_sk),
    route_sk        bigint references dwh.dim_route(route_sk),
    vehicle_sk      bigint references dwh.dim_vehicle(vehicle_sk),
    start_stop_sk   bigint references dwh.dim_stop(stop_sk),
    end_stop_sk     bigint references dwh.dim_stop(stop_sk),
    start_ts        timestamp with time zone not null,
    end_ts          timestamp with time zone,
    duration_sec    integer,
    distance_m      numeric(14,3),
    fare_amount     numeric(12,2),
    delay_start_sec integer,
    delay_end_sec   integer,
    status          varchar(32),
    load_id         varchar(64),
    inserted_at     timestamp with time zone default now(),
    constraint uq_fact_trip unique (trip_id)
);

CREATE TABLE IF NOT EXISTS dwh.fact_payment (
    payment_sk        bigserial primary key,
    payment_id        varchar(64) not null,
    user_sk           bigint references dwh.dim_user(user_sk),
    route_sk          bigint references dwh.dim_route(route_sk),
    trip_sk           bigint references dwh.fact_trip(trip_sk),
    payment_method_sk bigint references dwh.dim_payment_method(payment_method_sk),
    amount            numeric(12,2),
    currency          varchar(8),
    status            varchar(32),
    paid_at           timestamp with time zone,
    load_id           varchar(64),
    inserted_at       timestamp with time zone default now(),
    constraint uq_fact_payment unique (payment_id)
);

CREATE TABLE IF NOT EXISTS dwh.fact_vehicle_position (
    position_sk    bigserial primary key,
    event_ts       timestamp with time zone not null,
    event_date_sk  integer references dwh.dim_date(date_sk),
    event_time_sk  integer references dwh.dim_time(time_sk),
    vehicle_sk     bigint references dwh.dim_vehicle(vehicle_sk),
    route_sk       bigint references dwh.dim_route(route_sk),
    lat            numeric(10,7),
    lon            numeric(10,7),
    speed          numeric(10,3),
    heading        numeric(6,2),
    provider_ts    timestamp with time zone,
    load_id        varchar(64),
    inserted_at    timestamp with time zone default now()
);

-- Индексы для ускорения джойнов и партиций (BRIN по датам крупных фактов)
CREATE INDEX IF NOT EXISTS idx_fact_trip_start_ts ON dwh.fact_trip USING brin (start_ts);
CREATE INDEX IF NOT EXISTS idx_fact_payment_paid_at ON dwh.fact_payment USING brin (paid_at);
CREATE INDEX IF NOT EXISTS idx_fact_vehicle_position_event_ts ON dwh.fact_vehicle_position USING brin (event_ts);


