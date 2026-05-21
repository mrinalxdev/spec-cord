package txlog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)
type TxState string

const (
	StatePreparing  TxState = "PREPARING"
	StatePrepared   TxState = "PREPARED"
	StateCommitting TxState = "COMMITTING"
	StateCommitted  TxState = "COMMITTED"
	StateAborting   TxState = "ABORTING"
	StateAborted    TxState = "ABORTED"
)
type TxRecord struct {
	TxID      string    `json:"tx_id"`
	State     TxState   `json:"state"`
	ShardIDs  []string  `json:"shard_ids"`   
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

const etcdKeyPrefix = "/spec-coordinator/txlog/"
type Log struct {
	client *clientv3.Client
}

func New(client *clientv3.Client) *Log {
	return &Log{client: client}
}
func (l *Log) Begin(ctx context.Context, txID string, shardIDs []string) error {
	rec := TxRecord{
		TxID:      txID,
		State:     StatePreparing,
		ShardIDs:  shardIDs,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	return l.put(ctx, txID, rec)
}

func (l *Log) Transition(ctx context.Context, txID string, from, to TxState) error {
	key := etcdKeyPrefix + txID
	resp, err := l.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("txlog.Transition: get %s: %w", txID, err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("txlog.Transition: tx %s not found", txID)
	}

	kv := resp.Kvs[0]
	var rec TxRecord
	if err := json.Unmarshal(kv.Value, &rec); err != nil {
		return fmt.Errorf("txlog.Transition: unmarshal: %w", err)
	}
	if rec.State != from {
		return fmt.Errorf("txlog.Transition: expected state %s, got %s", from, rec.State)
	}

	rec.State = to
	rec.UpdatedAt = time.Now().UTC()

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("txlog.Transition: marshal: %w", err)
	}

	txResp, err := l.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kv.ModRevision)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("txlog.Transition: etcd txn: %w", err)
	}
	if !txResp.Succeeded {
		return fmt.Errorf("txlog.Transition: CAS failed for tx %s (concurrent write)", txID)
	}
	return nil
}

func (l *Log) Get(ctx context.Context, txID string) (*TxRecord, error) {
	resp, err := l.client.Get(ctx, etcdKeyPrefix+txID)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var rec TxRecord
	if err := json.Unmarshal(resp.Kvs[0].Value, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (l *Log) ListIncomplete(ctx context.Context) ([]TxRecord, error) {
	resp, err := l.client.Get(ctx, etcdKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var out []TxRecord
	for _, kv := range resp.Kvs {
		var rec TxRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			continue
		}
		if rec.State != StateCommitted && rec.State != StateAborted {
			out = append(out, rec)
		}
	}
	return out, nil
}

func (l *Log) put(ctx context.Context, txID string, rec TxRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = l.client.Put(ctx, etcdKeyPrefix+txID, string(data))
	return err
}
