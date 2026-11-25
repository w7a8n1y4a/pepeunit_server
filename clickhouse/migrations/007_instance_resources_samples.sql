CREATE TABLE instance_resources_samples
(
    create_datetime DateTime('UTC'),
    cpu_percent Float64,
    ram_percent Float64,
    network_width_percent Float64,
    pu_instance_network_speed_limit UInt64,
    pg_disk_percent Float64,
    cc_disk_percent Float64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(create_datetime)
ORDER BY create_datetime
TTL create_datetime + INTERVAL 90 DAY;
