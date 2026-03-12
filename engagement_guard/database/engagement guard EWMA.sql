-- ============================================================
--  EngagementGuard EWMA — MySQL Schema
--  Run this FIRST before touching any Python file
--  Your existing engagement_guard database is NOT affected
-- ============================================================

CREATE DATABASE IF NOT EXISTS engagement_guard_ewma;
USE engagement_guard_ewma;

-- ── 1. Companies ─────────────────────────────────────────────
--    One row per company being monitored
CREATE TABLE IF NOT EXISTS companies (
    company_id   INT          AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL UNIQUE,
    created_at   DATE         DEFAULT (CURRENT_DATE)
);

-- ── 2. Daily Feedback Counts ──────────────────────────────────
--    Raw input — one row per company per day
CREATE TABLE IF NOT EXISTS daily_feedback (
    id             INT  AUTO_INCREMENT PRIMARY KEY,
    company_id     INT  NOT NULL,
    feedback_date  DATE NOT NULL,
    feedback_count INT  NOT NULL DEFAULT 0,
    UNIQUE KEY uq_company_date (company_id, feedback_date),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- ── 3. Alerts raised by the pipeline ─────────────────────────
--    One row per company per day — updated on every pipeline run
CREATE TABLE IF NOT EXISTS engagement_alerts (
    alert_id      INT  AUTO_INCREMENT PRIMARY KEY,
    company_id    INT  NOT NULL,
    alert_date    DATE NOT NULL,
    alert_type    ENUM('Drop','Rising','Normal') NOT NULL,
    actual_count  INT,
    ewma_mean     FLOAT,
    ewma_std      FLOAT,
    deviation     FLOAT,
    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_alert (company_id, alert_date),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- ── 4. EWMA running state per company ────────────────────────
--    This is the "memory" of the system
--    Stores the 3 numbers EWMA needs to continue without replaying history
CREATE TABLE IF NOT EXISTS ewma_state (
    company_id   INT   PRIMARY KEY,
    ewma_mean    FLOAT NOT NULL,
    ewma_var     FLOAT NOT NULL DEFAULT 0,
    last_updated DATE  NOT NULL,
    n_days       INT   NOT NULL DEFAULT 0,
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);

-- ── 5. Pipeline run log ───────────────────────────────────────
--    Every time the pipeline runs it logs here
--    Replaces the model file versioning that Isolation Forest needs
CREATE TABLE IF NOT EXISTS model_registry (
    run_id           INT AUTO_INCREMENT PRIMARY KEY,
    run_date         DATE  DEFAULT (CURRENT_DATE),
    alpha            FLOAT NOT NULL,
    k_threshold      FLOAT NOT NULL,
    min_days         INT   NOT NULL,
    companies_scored INT,
    alerts_raised    INT,
    notes            TEXT
);

-- ── Quick verify — run this after to confirm all 5 tables exist
SHOW TABLES;