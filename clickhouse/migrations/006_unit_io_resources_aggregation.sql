CREATE TABLE unit_io_resources_aggregation
(
    start_window_datetime DateTime('UTC'),
    end_window_datetime DateTime('UTC') MATERIALIZED start_window_datetime + INTERVAL 1 HOUR,
    unit_io_uuid UUID,
    total_requests UInt64,
    total_bytes UInt64,
    total_datapipe_bytes_increment UInt64,
    CONSTRAINT start_on_hour CHECK start_window_datetime = toStartOfHour(start_window_datetime)
)
ENGINE = SummingMergeTree((total_requests, total_bytes, total_datapipe_bytes_increment))
PARTITION BY toYYYYMM(start_window_datetime)
ORDER BY (unit_io_uuid, start_window_datetime)
TTL end_window_datetime + INTERVAL 90 DAY
SETTINGS non_replicated_deduplication_window = 1000;
