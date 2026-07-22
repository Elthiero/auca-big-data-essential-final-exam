-- Auto-executed by the mysql container on first startup only
-- (docker-entrypoint-initdb.d convention). If you change this after the
-- volume already exists, you must `docker compose down -v` on mysql first.

CREATE TABLE IF NOT EXISTS model_registry (
    id INT AUTO_INCREMENT PRIMARY KEY,
    hdfs_path VARCHAR(255) NOT NULL,       -- e.g. /models/champion or /models/candidates/20260714_120000
    version VARCHAR(50) NOT NULL,
    trained_on VARCHAR(50),                -- e.g. '2019-10' or '2019-10+11'
    auc_score FLOAT,               -- promotion decisions are made on this, not accuracy
    accuracy FLOAT,
    precision_score FLOAT,         -- weighted precision (dominated by majority class)
    recall_score FLOAT,            -- weighted recall (dominated by majority class)
    precision_purchase FLOAT,      -- precision for label=1 specifically
    recall_purchase FLOAT,         -- recall for label=1 specifically
    decision_threshold FLOAT,      -- classification threshold tuned on the validation set
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS predictions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_session VARCHAR(100),
    event_time TIMESTAMP,
    purchase_probability FLOAT,
    predicted_label BOOLEAN,
    model_version VARCHAR(50),
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user_session (user_session),
    INDEX idx_scored_at (scored_at)
);

CREATE TABLE IF NOT EXISTS running_metrics (
    metric_key VARCHAR(50) PRIMARY KEY,
    metric_value BIGINT DEFAULT 0
);

-- Initialize keys
INSERT INTO running_metrics (metric_key, metric_value) VALUES ('total_predictions', 0) ON DUPLICATE KEY UPDATE metric_key=metric_key;
INSERT INTO running_metrics (metric_key, metric_value) VALUES ('purchase_predicted', 0) ON DUPLICATE KEY UPDATE metric_key=metric_key;