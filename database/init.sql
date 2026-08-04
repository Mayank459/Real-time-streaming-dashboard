-- =============================================================
-- E-Commerce Streaming Analytics Platform
-- PostgreSQL Schema Initialization
-- Auto-runs when the postgres container first starts
-- =============================================================

-- ───────────────────────────────────────────────────────────────
-- RAW EVENTS TABLES
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    order_id        VARCHAR(36)     NOT NULL UNIQUE,
    user_id         VARCHAR(36)     NOT NULL,
    product_id      VARCHAR(36)     NOT NULL,
    product_name    VARCHAR(255)    NOT NULL,
    category        VARCHAR(100)    NOT NULL,
    price           DECIMAL(10, 2)  NOT NULL CHECK (price > 0),
    quantity        INT             NOT NULL DEFAULT 1 CHECK (quantity > 0),
    total_amount    DECIMAL(10, 2)  NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    device          VARCHAR(50)     NOT NULL,
    browser         VARCHAR(50)     NOT NULL,
    status          VARCHAR(20)     NOT NULL DEFAULT 'pending',
    event_timestamp TIMESTAMPTZ     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments (
    id              SERIAL PRIMARY KEY,
    payment_id      VARCHAR(36)     NOT NULL UNIQUE,
    order_id        VARCHAR(36)     NOT NULL,
    user_id         VARCHAR(36)     NOT NULL,
    payment_type    VARCHAR(50)     NOT NULL,  -- UPI, Credit Card, COD, Debit Card, Net Banking
    amount          DECIMAL(10, 2)  NOT NULL,
    status          VARCHAR(20)     NOT NULL,  -- success, failed, pending
    gateway         VARCHAR(50),
    event_timestamp TIMESTAMPTZ     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS clicks (
    id              SERIAL PRIMARY KEY,
    click_id        VARCHAR(36)     NOT NULL UNIQUE,
    user_id         VARCHAR(36)     NOT NULL,
    product_id      VARCHAR(36)     NOT NULL,
    session_id      VARCHAR(36)     NOT NULL,
    page            VARCHAR(100)    NOT NULL,  -- home, search, product, cart, checkout
    action          VARCHAR(50)     NOT NULL,  -- click, view, add_to_cart, remove_from_cart
    duration_sec    INT,
    device          VARCHAR(50),
    event_timestamp TIMESTAMPTZ     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reviews (
    id              SERIAL PRIMARY KEY,
    review_id       VARCHAR(36)     NOT NULL UNIQUE,
    user_id         VARCHAR(36)     NOT NULL,
    product_id      VARCHAR(36)     NOT NULL,
    product_name    VARCHAR(255),
    rating          INT             NOT NULL CHECK (rating BETWEEN 1 AND 5),
    sentiment       VARCHAR(20)     NOT NULL,  -- positive, neutral, negative
    verified        BOOLEAN         NOT NULL DEFAULT FALSE,
    event_timestamp TIMESTAMPTZ     NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- ANALYTICS AGGREGATION TABLES  (Grafana reads these)
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS analytics_per_minute (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ     NOT NULL,
    window_end      TIMESTAMPTZ     NOT NULL,
    total_orders    INT             NOT NULL DEFAULT 0,
    total_revenue   DECIMAL(12, 2)  NOT NULL DEFAULT 0,
    unique_users    INT             NOT NULL DEFAULT 0,
    successful_payments INT         NOT NULL DEFAULT 0,
    failed_payments INT             NOT NULL DEFAULT 0,
    top_product     VARCHAR(255),
    top_category    VARCHAR(100),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (window_start)
);

CREATE TABLE IF NOT EXISTS revenue_by_country (
    id              SERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    country         VARCHAR(100)    NOT NULL,
    total_revenue   DECIMAL(12, 2)  NOT NULL DEFAULT 0,
    total_orders    INT             NOT NULL DEFAULT 0,
    unique_users    INT             NOT NULL DEFAULT 0,
    UNIQUE (snapshot_time, country)
);

CREATE TABLE IF NOT EXISTS top_products (
    id              SERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    product_id      VARCHAR(36)     NOT NULL,
    product_name    VARCHAR(255)    NOT NULL,
    category        VARCHAR(100),
    total_orders    INT             NOT NULL DEFAULT 0,
    total_revenue   DECIMAL(12, 2)  NOT NULL DEFAULT 0,
    avg_rating      DECIMAL(3, 2),
    UNIQUE (snapshot_time, product_id)
);

CREATE TABLE IF NOT EXISTS payment_method_stats (
    id              SERIAL PRIMARY KEY,
    snapshot_time   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    payment_type    VARCHAR(50)     NOT NULL,
    total_count     INT             NOT NULL DEFAULT 0,
    total_amount    DECIMAL(12, 2)  NOT NULL DEFAULT 0,
    success_rate    DECIMAL(5, 2),
    UNIQUE (snapshot_time, payment_type)
);

-- ───────────────────────────────────────────────────────────────
-- DEAD LETTER QUEUE – Bad / invalid events
-- ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id              SERIAL PRIMARY KEY,
    topic           VARCHAR(50)     NOT NULL,
    raw_message     TEXT            NOT NULL,
    error_reason    TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- ───────────────────────────────────────────────────────────────
-- INDEXES for Grafana query performance
-- ───────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_orders_timestamp   ON orders   (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_orders_country     ON orders   (country);
CREATE INDEX IF NOT EXISTS idx_orders_category    ON orders   (category);
CREATE INDEX IF NOT EXISTS idx_payments_timestamp ON payments (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_payments_type      ON payments (payment_type);
CREATE INDEX IF NOT EXISTS idx_analytics_window   ON analytics_per_minute (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_revenue_country    ON revenue_by_country (snapshot_time DESC, country);
CREATE INDEX IF NOT EXISTS idx_top_products_time  ON top_products (snapshot_time DESC);

-- ───────────────────────────────────────────────────────────────
-- VIEWS – Convenience queries for Grafana
-- ───────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_realtime_summary AS
SELECT
    COUNT(*)                        AS total_orders,
    SUM(total_amount)               AS total_revenue,
    COUNT(DISTINCT user_id)         AS unique_users,
    AVG(total_amount)               AS avg_order_value,
    MAX(event_timestamp)            AS last_event_at
FROM orders
WHERE event_timestamp > NOW() - INTERVAL '1 hour';

CREATE OR REPLACE VIEW v_orders_per_minute AS
SELECT
    DATE_TRUNC('minute', event_timestamp) AS minute,
    COUNT(*)                               AS order_count,
    SUM(total_amount)                      AS revenue
FROM orders
WHERE event_timestamp > NOW() - INTERVAL '2 hours'
GROUP BY DATE_TRUNC('minute', event_timestamp)
ORDER BY minute DESC;

CREATE OR REPLACE VIEW v_payment_split AS
SELECT
    payment_type,
    COUNT(*)                AS total_transactions,
    SUM(amount)             AS total_amount,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*),
        2
    )                       AS success_rate_pct
FROM payments
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY payment_type
ORDER BY total_transactions DESC;

CREATE OR REPLACE VIEW v_top_10_products AS
SELECT
    product_name,
    category,
    COUNT(*)        AS order_count,
    SUM(total_amount) AS revenue
FROM orders
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY product_name, category
ORDER BY revenue DESC
LIMIT 10;

CREATE OR REPLACE VIEW v_revenue_by_country AS
SELECT
    country,
    COUNT(*)            AS order_count,
    SUM(total_amount)   AS revenue,
    COUNT(DISTINCT user_id) AS unique_users
FROM orders
WHERE event_timestamp > NOW() - INTERVAL '1 hour'
GROUP BY country
ORDER BY revenue DESC;

-- ───────────────────────────────────────────────────────────────
-- SEED – Insert a test row so Grafana shows something immediately
-- ───────────────────────────────────────────────────────────────

INSERT INTO analytics_per_minute (window_start, window_end, total_orders, total_revenue, unique_users)
VALUES (NOW() - INTERVAL '1 minute', NOW(), 0, 0, 0)
ON CONFLICT DO NOTHING;

SELECT 'Schema initialized successfully!' AS status;
