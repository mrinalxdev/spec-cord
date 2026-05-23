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
}

func New(cfg *config.Config, log *zap.Logger, txLog *txlog.Log, shards *shard.Pool, m *metrics.Metrics) *Coordinator {
	return &Coordinator{
		cfg:     cfg,
		log:     log,
		txLog:   txLog,
		shards:  shards,
		metrics: m,
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
	if err := c.txLog.Transition(ctx, txID, txlog.StatePrepared, txlog.StateCommitting); err != nil {
		return txID, fmt.Errorf("2pc.Execute: log committing: %w", err)
	}

	commitCtx, commitCancel := context.WithTimeout(ctx, c.cfg.CommitTimeout())
	defer commitCancel()

	if commitErr := c.fanOutCommit(commitCtx, txID, shardIDs, log); commitErr != nil {
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
