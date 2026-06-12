-- +migrate Up
CREATE TABLE IF NOT EXISTS email_logs (
    id             CHAR(36)     NOT NULL,
    client_id      CHAR(36)     NOT NULL,
    aws_message_id VARCHAR(255) DEFAULT NULL,
    from_email     VARCHAR(255) NOT NULL,
    to_email       VARCHAR(255) NOT NULL,
    subject        VARCHAR(998) NOT NULL DEFAULT '',
    status         VARCHAR(50)  NOT NULL DEFAULT 'queued',
    sent_at        DATETIME     DEFAULT NULL,
    delivered_at   DATETIME     DEFAULT NULL,
    opened_at      DATETIME     DEFAULT NULL,
    clicked_at     DATETIME     DEFAULT NULL,
    bounced_at     DATETIME     DEFAULT NULL,
    created_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_email_logs_client_id (client_id),
    KEY idx_email_logs_aws_message_id (aws_message_id),
    KEY idx_email_logs_status (status),
    KEY idx_email_logs_created_at (created_at),
    CONSTRAINT fk_email_logs_client FOREIGN KEY (client_id) REFERENCES clients (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- +migrate Down
DROP TABLE IF EXISTS email_logs;
