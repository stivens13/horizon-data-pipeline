CREATE TABLE IF NOT EXISTS market_analytics.daily_market_volume
(
    id String,
    date DATE,
    transactions_amount UInt64,
    total_volume_usd Float64,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
) ENGINE = MergeTree()
    PRIMARY KEY (id);
