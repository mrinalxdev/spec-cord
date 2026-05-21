package shard

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type Shard struct {
	ID  string
	db  *sql.DB
	log *zap.Logger
}

type Pool struct {
	shards map[string]*Shard
	log    *zap.Logger
}

func NewPool(dsns map[string]string, log *zap.Logger) (*Pool, error) {
	p := &Pool{
		shards: make(map[string]*Shard, len(dsns)),
		log:    log,
	}
	for id, dsn := range dsns {
		db, err := sql.Open("mysql", dsn+"?parseTime=true&multiStatements=true")
		if err != nil {
			return nil, fmt.Errorf("shard.NewPool: open %s: %w", id, err)
		}
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(5 * time.Minute)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			return nil, fmt.Errorf("shard.NewPool: ping %s: %w", id, err)
		}

		p.shards[id] = &Shard{ID: id, db: db, log: log.With(zap.String("shard", id))}
		log.Info("shard connected", zap.String("id", id))
	}
	return p, nil
}

func (p *Pool) Get(id string) (*Shard, bool) {
	s, ok := p.shards[id]
	return s, ok
}

func (p *Pool) All() []*Shard {
	out := make([]*Shard, 0, len(p.shards))
	for _, s := range p.shards {
		out = append(out, s)
	}
	return out
}

func (s *Shard) PrepareTransfer(ctx context.Context, xid string, accountID int64, delta float64) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("shard.PrepareTransfer: get conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("XA START '%s'", xid)); err != nil {
		return fmt.Errorf("shard.PrepareTransfer: XA START: %w", err)
	}

	var balance float64
	row := conn.QueryRowContext(ctx,
		"SELECT balance FROM accounts WHERE id = ? FOR UPDATE", accountID)
	if err := row.Scan(&balance); err != nil {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: scan balance: %w", err)
	}
	if balance+delta < 0 {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: insufficient balance (%.2f + %.2f < 0)", balance, delta)
	}

	if _, err := conn.ExecContext(ctx,
		"UPDATE accounts SET balance = balance + ?, version = version + 1 WHERE id = ?",
		delta, accountID,
	); err != nil {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: update: %w", err)
	}
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO tx_history (tx_id, account_id, delta, state) VALUES (?, ?, ?, 'prepared')",
		xid, accountID, delta,
	); err != nil {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: history insert: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("XA END '%s'", xid)); err != nil {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: XA END: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("XA PREPARE '%s'", xid)); err != nil {
		_ = s.xaRollback(conn, ctx, xid)
		return fmt.Errorf("shard.PrepareTransfer: XA PREPARE: %w", err)
	}

	s.log.Debug("prepared", zap.String("xid", xid), zap.Int64("account", accountID))
	return nil
}

func (s *Shard) Commit(ctx context.Context, xid string) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("XA COMMIT '%s'", xid)); err != nil {
		return fmt.Errorf("shard.Commit: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		"UPDATE tx_history SET state = 'committed' WHERE tx_id = ?", xid)
	s.log.Debug("committed", zap.String("xid", xid))
	return nil
}

func (s *Shard) Abort(ctx context.Context, xid string) error {
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("XA ROLLBACK '%s'", xid)); err != nil {
		return fmt.Errorf("shard.Abort: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		"UPDATE tx_history SET state = 'aborted' WHERE tx_id = ?", xid)
	s.log.Debug("aborted", zap.String("xid", xid))
	return nil
}

func (s *Shard) xaRollback(conn *sql.Conn, ctx context.Context, xid string) error {
	_, err := conn.ExecContext(ctx, fmt.Sprintf("XA END '%s'", xid))
	if err != nil {
		s.log.Warn("XA END failed during rollback", zap.Error(err))
	}
	_, err = conn.ExecContext(ctx, fmt.Sprintf("XA ROLLBACK '%s'", xid))
	return err
}

func (s *Shard) HealthCheck(ctx context.Context) error {
	return s.db.PingContext(ctx)
}
