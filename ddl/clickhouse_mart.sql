CREATE TABLE IF NOT EXISTS mart_route_hourly
(
    route_id String,
    name String,
    transport_type String,
    hour_bucket DateTime,
    trip_cnt UInt64,
    avg_fare Float64,
    avg_duration_sec Float64,
    sum_distance_m Float64,
    avg_speed Float64,
    snapshot_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(hour_bucket)
ORDER BY (route_id, hour_bucket);

