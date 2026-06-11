-- +migrate Up
CREATE TABLE IF NOT EXISTS clients (
    id            CHAR(36)     NOT NULL,
    name          VARCHAR(255) NOT NULL,
    smtp_username VARCHAR(255) NOT NULL,
    smtp_password_hash VARCHAR(255) NOT NULL,
    api_key       VARCHAR(255) NOT NULL,
    hourly_limit  INT          NOT NULL DEFAULT 500,
    monthly_limit INT          NOT NULL DEFAULT 10000,
    is_active     TINYINT(1)   NOT NULL DEFAULT 1,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_clients_smtp_username (smtp_username),
    UNIQUE KEY uq_clients_api_key (api_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- +migrate Down
DROP TABLE IF EXISTS clients;
