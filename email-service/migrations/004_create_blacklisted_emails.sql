-- +migrate Up
CREATE TABLE IF NOT EXISTS blacklisted_emails (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    client_id  CHAR(36)        NOT NULL,
    email      VARCHAR(255)    NOT NULL,
    reason     VARCHAR(500)    NOT NULL DEFAULT '',
    created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_blacklist_client_email (client_id, email),
    KEY idx_blacklist_email (email),
    CONSTRAINT fk_blacklisted_emails_client FOREIGN KEY (client_id) REFERENCES clients (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- +migrate Down
DROP TABLE IF EXISTS blacklisted_emails;
