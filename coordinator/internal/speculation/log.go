package speculation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)


type SpecState string

const (
	SpecExecuting SpecState = "SPEC_EXECUTING"
	SpecCommitted SpecState = "SPEC_COMMITTED"
	SpecRolledBack SpecState = "SPEC_ROLLED_BACK"
)

type SpecRecord struct {
	UndoID string `json:"undo_id"`
	TxID     string    `json:"tx_id"`
	ShardID  string    `json:"shard_id"`
	AccountID int64    `json:"account_id"`
	State    SpecState `json:"state"`
	StartedAt   time.Time `json:"started_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	ConflictDetected bool `json:"conflict_detected,omitempty"`
	RollbackReason   string `json:"rollback_reason,omitempty"`
}


type Log struct {
	client *clientv3.Client
	prefix string
	log *zap.Logger
}


const defaultPrefix = "/spec-coordinator/speculation/"

func New(client *clientv3.Client, logger *zap.Logger, prefix string) *Log {
	if prefix == "" {
		prefix = defaultPrefix
	}

	return &Log {
		client: client,
		prefix: prefix,
		log: logger.With(zap.String("component","speculation_log")),
	}
}

func (l *Log) key(undoID string) string {
	return l.prefix + undoID
}

func (l *Log) BeginSpeculation(ctx context.Context, rec SpecRecord) error {
	if rec.UndoID == "" || rec.TxID == "" {
		return fmt.Errorf("speculation.Begin: undo_id and tx_id required")
	}
	if rec.State != SpecExecuting {
		return fmt.Errorf("speculation.Begin: initial state must be SPEC_EXECUTING, got %s", rec.State)
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("speculation.Begin: marshal: %w", err)
	}

	_, err = l.client.Put(ctx, l.key(rec.UndoID), string(data),
		clientv3.WithLease(l.leaseForDuration(ctx, rec.ExpiresAt.Sub(rec.StartedAt))))
	if err != nil {
		return fmt.Errorf("speculation.Begin: etcd put: %w", err)
	}

	l.log.Debug("speculation started",
		zap.String("undo_id", rec.UndoID),
		zap.String("tx_id", rec.TxID),
		zap.String("shard", rec.ShardID),
		zap.Int64("account", rec.AccountID))
	return nil
}

func (l *Log) TransitionSpecState(ctx context.Context, undoID string, from, to SpecState, reason string) error {
	key := l.key(undoID)
	resp, err := l.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("speculation.Transition: get %s: %w", undoID, err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("speculation.Transition: spec record %s not found", undoID)
	}

	kv := resp.Kvs[0]
	var rec SpecRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return fmt.Errorf("speculation.Transition: unmarshal: %w", err)
	}
	if rec.State != from {
		return fmt.Errorf("speculation.Transition: expected state %s for %s, got %s", from, undoID, rec.State)
	}
	rec.State = to
	if reason != "" {
		rec.RollbackReason = reason
	}
	rec.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("speculation.Transition: marshal: %w", err)
	}

	txnResp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("speculation.Transition: etcd txn: %w", err)
	}
	if !txnResp.Succeeded {
		return fmt.Errorf("speculation.Transition: CAS failed for %s (concurrent modification)", undoID)
	}

	l.log.Debug("speculation state transitioned",
		zap.String("undo_id", undoID),
		zap.String("from", string(from)),
		zap.String("to", string(to)),
		zap.String("reason", reason))
	return nil
}

func (l *Log) GetSpecRecord(ctx context.Context, undoID string) (*SpecRecord, error) {
	resp, err := l.client.Get(ctx, l.key(undoID))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil // Not found → not an error for idempotent operations
	}

	var rec SpecRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return nil, fmt.Errorf("speculation.Get: unmarshal: %w", err)
	}
	return &rec, nil
}

func (l *Log) ListActiveByTx(ctx context.Context, txID string) ([]SpecRecord, error) {
	resp, err := l.client.Get(ctx, l.prefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, fmt.Errorf("speculation.ListActiveByTx: etcd get: %w", err)
	}

	var results []SpecRecord
	for _, kv := range resp.Kvs {
		var rec SpecRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			l.log.Warn("speculation.ListActiveByTx: skip malformed record", zap.Error(err))
			continue
		}
		if rec.TxID == txID && rec.State == SpecExecuting {
			results = append(results, rec)
		}
	}
	return results, nil
}

func (l *Log) MarkForGC(ctx context.Context, undoID string, ttl time.Duration) error {
	leaseResp, err := l.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("speculation.MarkForGC: grant lease: %w", err)
	}

	key := l.key(undoID)
	_, err = l.client.Put(ctx, key, "", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		// Best-effort: if put fails, entry will still be GC'd by application sweeper
		l.log.Warn("speculation.MarkForGC: put with lease failed (non-fatal)",
			zap.String("undo_id", undoID), zap.Error(err))
		return nil
	}
	return nil
}


func (l *Log) leaseForDuration(ctx context.Context, d time.Duration) clientv3.LeaseID {
	if d <= 0 {
		return 0
	}

	leaseResp, err := l.client.Grant(ctx, int64(d.Seconds()))
	if err != nil {
		l.log.Warn("speculation : failed to create lease", zap.Error(err))
		return 0
	}

	return leaseResp.ID
}

func (l *Log) Close() error {
	// etcd client managed externally --- so no op here
	return nil
}