CREATE TABLE IF NOT EXISTS ewma_state (
    id           INT AUTO_INCREMENT PRIMARY KEY,
    company_id   INT NOT NULL UNIQUE,
    ewma_mean    FLOAT NOT NULL DEFAULT 0,
    ewma_var     FLOAT NOT NULL DEFAULT 0,
    n_days       INT NOT NULL DEFAULT 0,
    last_updated DATE,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);