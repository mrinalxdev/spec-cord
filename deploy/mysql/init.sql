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

  CREATE TABLE IF NOT EXISTS undo_log (
      undo_id       VARCHAR(36)  NOT NULL,
      tx_id         VARCHAR(36)  NOT NULL,
      account_id    BIGINT       NOT NULL,
      balance_before DECIMAL(18,2) NOT NULL,
      version_before BIGINT       NOT NULL,
      delta_applied DECIMAL(18,2) NOT NULL,
      state         ENUM('active', 'committed', 'rolled_back') NOT NULL DEFAULT 'active',
      created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
      expires_at    TIMESTAMP    NOT NULL,
      PRIMARY KEY (undo_id),
      INDEX idx_tx_account (tx_id, account_id),
      INDEX idx_state_expires (state, expires_at),
      INDEX idx_account (account_id)
  ) ENGINE=InnoDB
    DEFAULT CHARSET=utf8mb4
    COLLATE=utf8mb4_unicode_ci


DELIMITER //

CREATE PROCEDURE create_undo_and_apply_spec(
    IN p_undo_id VARCHAR(36),
    IN p_tx_id VARCHAR(36),
    IN p_account_id BIGINT,
    IN p_delta DECIMAL(18,2),
    IN p_expires_at TIMESTAMP
)
BEGIN
    DECLARE v_balance_before DECIMAL(18,2);
    DECLARE v_version_before BIGINT;
    START TRANSACTION;

    -- Lock the account row and capture pre-speculation state
    SELECT balance, version
    INTO v_balance_before, v_version_before
    FROM accounts
    WHERE id = p_account_id
    FOR UPDATE;
    INSERT INTO undo_log (
        undo_id, tx_id, account_id,
        balance_before, version_before, delta_applied,
        state, expires_at
    ) VALUES (
        p_undo_id, p_tx_id, p_account_id,
        v_balance_before, v_version_before, p_delta,
        'active', p_expires_at
    );
    UPDATE accounts
    SET balance = balance + p_delta,
        version = version + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE id = p_account_id;
    INSERT INTO tx_history (tx_id, account_id, delta, state)
    VALUES (p_tx_id, p_account_id, p_delta, 'spec_executed');

    COMMIT;
END //

CREATE PROCEDURE rollback_speculative_change(
    IN p_undo_id VARCHAR(36),
    IN p_tx_id VARCHAR(36)
)
BEGIN
    DECLARE v_account_id BIGINT;
    DECLARE v_balance_before DECIMAL(18,2);
    DECLARE v_version_before BIGINT;
    DECLARE v_current_version BIGINT;
    
    START TRANSACTION;
    
    SELECT account_id, balance_before, version_before
    INTO v_account_id, v_balance_before, v_version_before
    FROM undo_log
    WHERE undo_id = p_undo_id AND tx_id = p_tx_id AND state = 'active'
    FOR UPDATE;
    
    IF v_account_id IS NULL THEN
        COMMIT;
        SELECT 'already_processed' AS result;
    ELSE
        SELECT version INTO v_current_version
        FROM accounts WHERE id = v_account_id FOR UPDATE;
        
        IF v_current_version != v_version_before + 1 THEN
            ROLLBACK;
            SELECT 'version_conflict' AS result;
        ELSE
            UPDATE accounts 
            SET balance = v_balance_before,
                version = v_version_before,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = v_account_id;
            
            UPDATE undo_log 
            SET state = 'rolled_back' 
            WHERE undo_id = p_undo_id;
            
            INSERT INTO tx_history (tx_id, account_id, delta, state)
            VALUES (p_tx_id, v_account_id, 0, 'spec_rolled_back');
            
            COMMIT;
            SELECT 'rolled_back' AS result;
        END IF;
    END IF;
END //


CREATE PROCEDURE finalize_speculative_commit(
    IN p_undo_id VARCHAR(36),
    IN p_tx_id VARCHAR(36)
)
BEGIN
    START TRANSACTION; 
    UPDATE undo_log 
    SET state = 'committed' 
    WHERE undo_id = p_undo_id AND tx_id = p_tx_id AND state = 'active';
    UPDATE tx_history 
    SET state = 'committed' 
    WHERE tx_id = p_tx_id AND state = 'spec_executed';
    
    COMMIT;
    SELECT ROW_COUNT() AS finalized_count;
END //

DELIMITER ;

DELIMITER //

CREATE PROCEDURE gc_undo_log(IN p_batch_size INT)
BEGIN
    DELETE FROM undo_log 
    WHERE state IN ('committed', 'rolled_back') 
      AND expires_at < NOW()
    LIMIT p_batch_size;
    
    SELECT ROW_COUNT() AS deleted_count;
END //

DELIMITER ;


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
