CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id VARCHAR(50) PRIMARY KEY,
    transaction_id VARCHAR(50),
    anomaly_type VARCHAR(50),
    severity VARCHAR(20),
    confidence_score DECIMAL(5,4),
    description TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(50),
    amount DECIMAL(15,2),
    merchant VARCHAR(100),
    risk_factors TEXT[],
    recommended_actions TEXT[],
    llm_explanation TEXT
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    amount DECIMAL(15,2),
    merchant VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    location VARCHAR(100),
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id VARCHAR(50) PRIMARY KEY,
    anomaly_id VARCHAR(50),
    alert_type VARCHAR(50),
    severity VARCHAR(20),
    message TEXT,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20)
);
CREATE INDEX idx_anomalies_detected_at ON anomalies(detected_at);
CREATE INDEX idx_anomalies_severity ON anomalies(severity);
CREATE INDEX idx_transactions_timestamp ON transactions(timestamp);

GRANT ALL PRIVILEGES ON DATABASE fintech_monitoring TO fintech;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fintech;
EOF