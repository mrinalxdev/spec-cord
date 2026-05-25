package twopc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"

	"github.com/mrinalxdev/spec-coordinator/internal/config"
	"github.com/mrinalxdev/spec-coordinator/internal/metrics"
	"github.com/mrinalxdev/spec-coordinator/internal/shard"
	"github.com/mrinalxdev/spec-coordinator/internal/txlog"
	"github.com/mrinalxdev/spec-coordinator/internal/speculation"
)

type TransferOp struct {
	ShardID   string  `json:"shard_id"`
	AccountID int64   `json:"account_id"`
	Delta     float64 `json:"delta"`
}
type Coordinator struct {
	cfg     *config.Config
	log     *zap.Logger
	txLog   *txlog.Log
	shards  *shard.Pool
	metrics *metrics.Metrics
	specLog *speculation.Log
}

func New(cfg *config.Config, log *zap.Logger, txLog *txlog.Log, shards *shard.Pool, m *metrics.Metrics, specLog *speculation.Log) *Coordinator {
	return &Coordinator{
		cfg:     cfg,
		log:     log,
		txLog:   txLog,
		shards:  shards,
		metrics: m,
		specLog: specLog,
	}
}

func (c *Coordinator) Execute(ctx context.Context, ops []TransferOp) (txID string, err error) {
	start := time.Now()
	txID = uuid.New().String()
	log := c.log.With(zap.String("tx_id", txID))
	log.Info("transaction started", zap.Int("ops", len(ops)))

	shardIDs := uniqueShards(ops)
	if err := c.txLog.Begin(ctx, txID, shardIDs); err != nil {
		return txID, fmt.Errorf("2pc.Execute: log begin: %w", err)
	}
	prepCtx, prepCancel := context.WithTimeout(ctx, c.cfg.PrepareTimeout())
	defer prepCancel()

	prepareErr := c.fanOutPrepare(prepCtx, txID, ops, log)
	if prepareErr != nil {
		log.Warn("prepare failed, aborting", zap.Error(prepareErr))
		c.metrics.TxTotal.WithLabelValues("aborted").Inc()
		_ = c.txLog.Transition(ctx, txID, txlog.StatePreparing, txlog.StateAborting)
		c.fanOutAbort(ctx, txID, shardIDs, log)
		_ = c.txLog.Transition(ctx, txID, txlog.StateAborting, txlog.StateAborted)
		return txID, fmt.Errorf("2pc.Execute: prepare: %w", prepareErr)
	}

	if err := c.txLog.Transition(ctx, txID, txlog.StatePreparing, txlog.StatePrepared); err != nil {
		return txID, fmt.Errorf("2pc.Execute: log prepared: %w", err)
	}

	var specUndoIDs map[string]string // shard_id -> undo_id
	if c.cfg.SpeculationEnabled && c.canSpeculate(ops) {
		var specErr error
		specUndoIDs, specErr = c.executeSpeculatively(ctx, txID, ops, log)
		if specErr != nil {
			log.Warn("speculative execution failed, falling back to standard commit",
				zap.Error(specErr))
			c.metrics.SpeculationTotal.WithLabelValues("miss").Inc()
			specUndoIDs = nil 
		} else {
			c.metrics.SpeculationTotal.WithLabelValues("hit").Inc()
			log.Debug("speculative execution started", zap.Int("ops", len(ops)))
		}
	}
	if err := c.txLog.Transition(ctx, txID, txlog.StatePrepared, txlog.StateCommitting); err != nil {
		if specUndoIDs != nil {
			c.rollbackSpeculative(ctx, txID, specUndoIDs, "commit_transition_failed", log)
		}
		return txID, fmt.Errorf("2pc.Execute: log committing: %w", err)
	}

	commitCtx, commitCancel := context.WithTimeout(ctx, c.cfg.CommitTimeout())
	defer commitCancel()

	commitErr := c.fanOutCommit(commitCtx, txID, shardIDs, log)
	if specUndoIDs != nil {
		if commitErr == nil {
			if finalizeErr := c.finalizeSpeculative(ctx, txID, specUndoIDs, log); finalizeErr != nil {
				log.Warn("speculative finalization failed (non-fatal, GC will clean up)",
					zap.Error(finalizeErr))
			}
		} else {
			c.rollbackSpeculative(ctx, txID, specUndoIDs, "commit_failed", log)
		}
	}

	if commitErr != nil {
		log.Error("COMMIT FAILURE — transaction in uncertain state",
			zap.Error(commitErr), zap.Strings("shards", shardIDs))
		c.metrics.TxTotal.WithLabelValues("uncertain").Inc()
		return txID, fmt.Errorf("2pc.Execute: commit: %w", commitErr)
	}

	if err := c.txLog.Transition(ctx, txID, txlog.StateCommitting, txlog.StateCommitted); err != nil {
		log.Warn("could not mark committed in txlog (non-fatal)", zap.Error(err))
	}

	elapsed := time.Since(start)
	c.metrics.TxLatency.WithLabelValues("committed").Observe(elapsed.Seconds())
	c.metrics.TxTotal.WithLabelValues("committed").Inc()
	log.Info("transaction committed", zap.Duration("elapsed", elapsed))
	return txID, nil
}


func (c *Coordinator) canSpeculate(ops []TransferOp) bool {
	if len(ops) > c.cfg.MaxSpecOpsPerTx {
		return false
	}
	return true
}

func (c *Coordinator) executeSpeculatively(ctx context.Context, txID string, ops []TransferOp, log *zap.Logger) (map[string]string, error) {
	undoIDs := make(map[string]string, len(ops))
	expiresAt := time.Now().Add(c.cfg.UndoLogTTL())

	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for _, op := range ops {
		op := op
		sh, ok := c.shards.Get(op.ShardID)
		if !ok {
			return nil, fmt.Errorf("unknown shard: %s", op.ShardID)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if conflict, err := sh.DetectSpecConflict(ctx, op.AccountID, c.cfg.SpecConflictWindow()); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("conflict check failed on %s: %w", op.ShardID, err)
				}
				mu.Unlock()
				return
			} else if conflict {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("speculation conflict detected on shard %s account %d", op.ShardID, op.AccountID)
				}
				mu.Unlock()
				return
			}
			undoID, err := sh.ExecuteSpeculative(ctx, txID, op.AccountID, op.Delta, expiresAt)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("speculative execute failed on %s: %w", op.ShardID, err)
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			undoIDs[op.ShardID] = undoID
			mu.Unlock()
			specRec := speculation.SpecRecord{
				UndoID:    undoID,
				TxID:      txID,
				ShardID:   op.ShardID,
				AccountID: op.AccountID,
				State:     speculation.SpecExecuting,
				StartedAt: time.Now().UTC(),
				ExpiresAt: expiresAt.UTC(),
			}
			if err := c.specLog.BeginSpeculation(ctx, specRec); err != nil {
				log.Warn("failed to record speculation start (non-fatal)",
					zap.String("undo_id", undoID), zap.Error(err))
			}
		}()
	}

	wg.Wait()
	return undoIDs, firstErr
}


func (c *Coordinator) finalizeSpeculative(ctx context.Context, txID string, undoIDs map[string]string, log *zap.Logger) error {
	var wg sync.WaitGroup
	var firstErr error
	var mu sync.Mutex

	for shardID, undoID := range undoIDs {
		shardID, undoID := shardID, undoID
		sh, ok := c.shards.Get(shardID)
		if !ok {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Finalize on shard
			if err := sh.CommitSpeculative(ctx, undoID, txID); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("finalize failed on %s: %w", shardID, err)
				}
				mu.Unlock()
				return
			}
			if err := c.specLog.TransitionSpecState(ctx, undoID, speculation.SpecExecuting, speculation.SpecCommitted, ""); err != nil {
				log.Warn("failed to transition spec state to committed",
					zap.String("undo_id", undoID), zap.Error(err))
			}

			// Mark for GC via etcd lease
			if err := c.specLog.MarkForGC(ctx, undoID, c.cfg.UndoLogTTL()); err != nil {
				log.Debug("failed to mark spec entry for GC (non-fatal)",
					zap.String("undo_id", undoID), zap.Error(err))
			}
		}()
	}

	wg.Wait()
	return firstErr
}

func (c *Coordinator) rollbackSpeculative(ctx context.Context, txID string, undoIDs map[string]string, reason string, log *zap.Logger) {
	var wg sync.WaitGroup

	for shardID, undoID := range undoIDs {
		shardID, undoID := shardID, undoID
		sh, ok := c.shards.Get(shardID)
		if !ok {
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Rollback on shard
			rolledBack, err := sh.RollbackSpeculative(ctx, undoID, txID, reason)
			if err != nil {
				log.Error("speculative rollback failed on shard",
					zap.String("shard", shardID),
					zap.String("undo_id", undoID),
					zap.Error(err))
				// In production: consider alerting on version_conflict errors
				return
			}
			if rolledBack {
				c.metrics.RollbackTotal.Inc()
			}

			// Update speculation log state
			if err := c.specLog.TransitionSpecState(ctx, undoID, speculation.SpecExecuting, speculation.SpecRolledBack, reason); err != nil {
				log.Warn("failed to transition spec state to rolled_back",
					zap.String("undo_id", undoID), zap.Error(err))
			}
		}()
	}

	wg.Wait()
}

func (c *Coordinator) Recover(ctx context.Context) error {
	records, err := c.txLog.ListIncomplete(ctx)
	if err != nil {
		return fmt.Errorf("2pc.Recover: list: %w", err)
	}
	for _, rec := range records {
		c.log.Warn("recovering in-flight transaction",
			zap.String("tx_id", rec.TxID),
			zap.String("state", string(rec.State)))
		c.metrics.TxTotal.WithLabelValues("recovered").Inc()
		switch rec.State {
		case txlog.StatePreparing:
			_ = c.txLog.Transition(ctx, rec.TxID, txlog.StatePreparing, txlog.StateAborting)
			c.fanOutAbort(ctx, rec.TxID, rec.ShardIDs, c.log)
			_ = c.txLog.Transition(ctx, rec.TxID, txlog.StateAborting, txlog.StateAborted)
		case txlog.StatePrepared, txlog.StateCommitting:
			_ = c.txLog.Transition(ctx, rec.TxID, rec.State, txlog.StateCommitting)
			c.fanOutCommit(ctx, rec.TxID, rec.ShardIDs, c.log)
			_ = c.txLog.Transition(ctx, rec.TxID, txlog.StateCommitting, txlog.StateCommitted)
		case txlog.StateAborting:
			c.fanOutAbort(ctx, rec.TxID, rec.ShardIDs, c.log)
			_ = c.txLog.Transition(ctx, rec.TxID, txlog.StateAborting, txlog.StateAborted)
		}
	}

	specRecords, err := c.specLog.ListActiveByTx(ctx, "") // empty txID = all
	if err != nil {
		c.log.Warn("failed to list active spec records during recovery", zap.Error(err))
		return nil // Non-fatal: speculation is optimization
	}

	for _, specRec := range specRecords {
		txRec, err := c.txLog.Get(ctx, specRec.TxID)
		if err != nil || txRec == nil {
			// Parent tx not found: treat as aborted (conservative)
			c.log.Warn("spec recovery: parent tx not found, rolling back speculation",
				zap.String("tx_id", specRec.TxID),
				zap.String("undo_id", specRec.UndoID))
			c.rollbackSpeculative(ctx, specRec.TxID, map[string]string{specRec.ShardID: specRec.UndoID}, "recovery_parent_not_found", c.log)
			continue
		}

		switch txRec.State {
		case txlog.StateCommitted:
			if err := c.finalizeSpeculative(ctx, specRec.TxID, map[string]string{specRec.ShardID: specRec.UndoID}, c.log); err != nil {
				c.log.Warn("spec recovery: finalize failed (non-fatal)",
					zap.String("undo_id", specRec.UndoID), zap.Error(err))
			}
		case txlog.StateAborted, txlog.StateAborting:
			c.rollbackSpeculative(ctx, specRec.TxID, map[string]string{specRec.ShardID: specRec.UndoID}, "recovery_parent_aborted", c.log)
		default:
			c.log.Debug("spec recovery: parent tx still in-progress, deferring",
				zap.String("tx_id", specRec.TxID),
				zap.String("state", string(txRec.State)))
		}
	}

	return nil
}

func (c *Coordinator) fanOutPrepare(ctx context.Context, txID string, ops []TransferOp, log *zap.Logger) error {
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for _, op := range ops {
		op := op
		sh, ok := c.shards.Get(op.ShardID)
		if !ok {
			return fmt.Errorf("unknown shard: %s", op.ShardID)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			prepStart := time.Now()
			err := sh.PrepareTransfer(ctx, txID, op.AccountID, op.Delta)
			c.metrics.ShardLatency.WithLabelValues(op.ShardID, "prepare").
				Observe(time.Since(prepStart).Seconds())
			if err != nil {
				log.Warn("shard prepare failed",
					zap.String("shard", op.ShardID),
					zap.Error(err))
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (c *Coordinator) fanOutCommit(ctx context.Context, txID string, shardIDs []string, log *zap.Logger) error {
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for _, sid := range shardIDs {
		sid := sid
		sh, ok := c.shards.Get(sid)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			commitStart := time.Now()
			err := sh.Commit(ctx, txID)
			c.metrics.ShardLatency.WithLabelValues(sid, "commit").
				Observe(time.Since(commitStart).Seconds())
			if err != nil {
				log.Error("shard commit failed", zap.String("shard", sid), zap.Error(err))
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (c *Coordinator) fanOutAbort(ctx context.Context, txID string, shardIDs []string, log *zap.Logger) {
	var wg sync.WaitGroup
	for _, sid := range shardIDs {
		sid := sid
		sh, ok := c.shards.Get(sid)
		if !ok {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sh.Abort(ctx, txID); err != nil {
				log.Error("shard abort failed", zap.String("shard", sid), zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

func uniqueShards(ops []TransferOp) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, op := range ops {
		if _, ok := seen[op.ShardID]; !ok {
			seen[op.ShardID] = struct{}{}
			out = append(out, op.ShardID)
		}
	}
	return out
}
