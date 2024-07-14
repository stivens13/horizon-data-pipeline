CREATE TABLE IF NOT EXISTS market_analytics.daily_market_volume_per_project
(
    id String,
    project_id String,
    date DATE,
    transaction_amount UInt64,
    total_volume_usd UInt64
) ENGINE = MergeTree()
    PRIMARY KEY (id, date);
