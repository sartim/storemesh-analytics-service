CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_sales (
    sale_date DATE PRIMARY KEY,
    order_count BIGINT NOT NULL DEFAULT 0,
    gross_minor BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS inventory_event_counts (
    event_type TEXT PRIMARY KEY,
    event_count BIGINT NOT NULL DEFAULT 0
);
