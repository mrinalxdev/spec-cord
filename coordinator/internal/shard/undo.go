// coordinator/internal/shard/undo.go

package shard

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type SpeculativeOp struct {
	UndoID    string
	TxID      string
	AccountID int64
	Delta     float64
	ShardID   string
}

func (s *Shard) ExecuteSpeculative(ctx context.Context, txID string, accountID int64, delta float64, expiresAt time.Time) (string, error) {
	undoID := uuid.New().String()

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("shard.ExecuteSpeculative: get conn: %w", err)
	}
	defer conn.Close()

	result, err := conn.ExecContext(ctx,
		"CALL create_undo_and_apply_spec(?, ?, ?, ?, ?)",
		undoID, txID, accountID, delta, expiresAt)
	if err != nil {
		if isDuplicateUndoEntry(err) {
			existing, err := s.getExistingUndoID(ctx, conn, txID, accountID)
			if err != nil {
				return "", fmt.Errorf("shard.ExecuteSpeculative: duplicate but lookup failed: %w", err)
			}
			if existing != "" {
				s.log.Debug("speculative execute: idempotent retry",
					zap.String("tx_id", txID),
					zap.Int64("account", accountID),
					zap.String("existing_undo_id", existing))
				return existing, nil
			}
		}
		return "", fmt.Errorf("shard.ExecuteSpeculative: proc call: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil || rows == 0 {
		return "", fmt.Errorf("shard.ExecuteSpeculative: no rows affected (possible constraint violation)")
	}

	s.log.Debug("speculative execute completed",
		zap.String("undo_id", undoID),
		zap.String("tx_id", txID),
		zap.Int64("account", accountID),
		zap.Float64("delta", delta))
	return undoID, nil
}

func (s *Shard) CommitSpeculative(ctx context.Context, undoID, txID string) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("shard.CommitSpeculative: get conn: %w", err)
	}
	defer conn.Close()
	result, err := conn.ExecContext(ctx,
		"CALL finalize_speculative_commit(?, ?)",
		undoID, txID)
	if err != nil {
		return fmt.Errorf("shard.CommitSpeculative: proc call: %w", err)
	}
	rows, _ := result.RowsAffected()
	s.log.Debug("speculative commit finalized",
		zap.String("undo_id", undoID),
		zap.String("tx_id", txID),
		zap.Int64("rows_affected", rows))
	return nil
}

func (s *Shard) RollbackSpeculative(ctx context.Context, undoID, txID string, reason string) (bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("shard.RollbackSpeculative: get conn: %w", err)
	}
	defer conn.Close()

	row := conn.QueryRowContext(ctx,
		"CALL rollback_speculative_change(?, ?)",
		undoID, txID)

	var result string
	if err := row.Scan(&result); err != nil {
		return false, fmt.Errorf("shard.RollbackSpeculative: proc result scan: %w", err)
	}

	switch result {
	case "rolled_back":
		s.log.Info("speculative rollback completed",
			zap.String("undo_id", undoID),
			zap.String("tx_id", txID),
			zap.String("reason", reason))
		return true, nil
	case "already_processed":
		s.log.Debug("speculative rollback: idempotent no-op (already processed)",
			zap.String("undo_id", undoID),
			zap.String("tx_id", txID))
		return false, nil
	case "version_conflict":
		s.log.Error("speculative rollback: VERSION CONFLICT - potential serializability violation",
			zap.String("undo_id", undoID),
			zap.String("tx_id", txID),
			zap.String("reason", reason))
		return false, fmt.Errorf("shard.RollbackSpeculative: version conflict on account (serializability risk)")
	default:
		return false, fmt.Errorf("shard.RollbackSpeculative: unknown result: %s", result)
	}
}

func (s *Shard) DetectSpecConflict(ctx context.Context, accountID int64, window time.Duration) (bool, error) {
	cutoff := time.Now().Add(-window)

	row := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM undo_log 
		 WHERE account_id = ? 
		   AND state = 'active' 
		   AND created_at >= ?`,
		accountID, cutoff)

	var count int
	if err := row.Scan(&count); err != nil {
		return false, fmt.Errorf("shard.DetectSpecConflict: query: %w", err)
	}
	return count > 0, nil
}

func (s *Shard) GCUndoLog(ctx context.Context, batchSize int) (int, error) {
	row := s.db.QueryRowContext(ctx,
		"CALL gc_undo_log(?)", batchSize)

	var deleted int
	if err := row.Scan(&deleted); err != nil {
		return 0, fmt.Errorf("shard.GCUndoLog: proc call: %w", err)
	}

	if deleted > 0 {
		s.log.Debug("undo log GC completed", zap.Int("deleted", deleted))
	}
	return deleted, nil
}

func isDuplicateUndoEntry(err error) bool {
	// MySQL error code 1062 = ER_DUP_ENTRY
	return err != nil && (err.Error() == "Error 1062: Duplicate entry" || 
		(err.Error() != "" && contains(err.Error(), "Duplicate entry")))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		indexOf(s, substr) >= 0))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func (s *Shard) getExistingUndoID(ctx context.Context, conn *sql.Conn, txID string, accountID int64) (string, error) {
	row := conn.QueryRowContext(ctx,
		"SELECT undo_id FROM undo_log WHERE tx_id = ? AND account_id = ? AND state = 'active'",
		txID, accountID)

	var undoID string
	err := row.Scan(&undoID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return undoID, nil
}