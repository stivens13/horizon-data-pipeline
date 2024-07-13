-- CREATE ROLE IF NOT EXISTS 'analytics_role';
CREATE DATABASE IF NOT EXISTS market_analytics;
-- CREATE USER IF NOT EXISTS analytics_user IDENTIFIED WITH plaintext_password BY 'complex_password';
-- GRANT ALL ON *.* TO analytics_role WITH GRANT OPTION;
-- GRANT analytics_role to analytics_user;
CREATE TABLE IF NOT EXISTS market_analytics.daily_market_volume
(
    id String,
    project_id String,
    date DATE,
    transaction_amount UInt64,
    total_volume_usd UInt64
) ENGINE = MergeTree()
    PRIMARY KEY (id, date);
