-- +migrate Up
CREATE TABLE IF NOT EXISTS email_daily_stats (
    client_id  CHAR(36) NOT NULL,
    date       DATE     NOT NULL,
    sent       INT      NOT NULL DEFAULT 0,
    delivered  INT      NOT NULL DEFAULT 0,
    opened     INT      NOT NULL DEFAULT 0,
    clicked    INT      NOT NULL DEFAULT 0,
    bounced    INT      NOT NULL DEFAULT 0,
    complained INT      NOT NULL DEFAULT 0,
    PRIMARY KEY (client_id, date),
    CONSTRAINT fk_email_daily_stats_client FOREIGN KEY (client_id) REFERENCES clients (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- +migrate Down
DROP TABLE IF EXISTS email_daily_stats;
