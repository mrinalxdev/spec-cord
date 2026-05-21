CREATE TABLE IF NOT EXISTS accounts (
    id          BIGINT       NOT NULL,
    name        VARCHAR(64)  NOT NULL,
    balance     DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    version     BIGINT       NOT NULL DEFAULT 0,     -- optimistic lock counter
    updated_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    INDEX idx_balance (balance)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS tx_history (
    tx_id       VARCHAR(36)  NOT NULL,
    account_id  BIGINT       NOT NULL,
    delta       DECIMAL(18,2) NOT NULL,
    state       ENUM('prepared','committed','aborted') NOT NULL,
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tx_id, account_id),
    INDEX idx_state (state),
    INDEX idx_created (created_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS xa_prepared (
    xid         VARCHAR(36)  NOT NULL,
    account_id  BIGINT       NOT NULL,
    delta       DECIMAL(18,2) NOT NULL,
    prepared_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (xid)
) ENGINE=InnoDB;

-- Seed some accounts for sysbench (range A shard: 1-10000, B: 10001-20000, C: 20001-30000)
-- The init.sql is identical on all shards; sysbench seeds only what the coordinator routes to each.
DELIMITER //
CREATE PROCEDURE seed_accounts(IN start_id BIGINT, IN end_id BIGINT)
BEGIN
    DECLARE i BIGINT DEFAULT start_id;
    WHILE i <= end_id DO
        INSERT IGNORE INTO accounts (id, name, balance)
        VALUES (i, CONCAT('account_', i), 1000.00);
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;
