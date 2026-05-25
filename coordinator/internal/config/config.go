package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	CoordinatorID    string   `envconfig:"COORDINATOR_ID" default:"coordinator-1"`
	EtcdEndpoints    []string `envconfig:"ETCD_ENDPOINTS" default:"localhost:2379"`
	ShardADSN        string   `envconfig:"SHARD_A_DSN" required:"true"`
	ShardBDSN        string   `envconfig:"SHARD_B_DSN" required:"true"`
	ShardCDSN        string   `envconfig:"SHARD_C_DSN" required:"true"`
	GRPCListen       string   `envconfig:"GRPC_LISTEN" default:":9090"`
	HTTPListen       string   `envconfig:"HTTP_LISTEN" default:":8080"`
	PrepareTimeoutMs int      `envconfig:"PREPARE_TIMEOUT_MS" default:"2000"`
	CommitTimeoutMs  int      `envconfig:"COMMIT_TIMEOUT_MS"  default:"2000"`
	TxTotalTimeoutMs int      `envconfig:"TX_TOTAL_TIMEOUT_MS" default:"10000"`
	LogLevel         string   `envconfig:"LOG_LEVEL" default:"info"`

	SpeculationEnabled bool `envconfig:"SPECULATION_ENABLED" defualt:"false"`
	UndoLogTTLHours    int  `envconfig:"UNDO_LOG_TTL_HOURS" default:"2"`
	MaxSpecOpsPerTx    int  `envconfig:"MAX_SPEC_OPS_PER_TX" default:"100"`
	/*
	 * speculation conflict detection window like if another tx modifies an account within this window after speculation starts, we abort specualtion early .. only trade off i can think for now is ... smaller == safer but more false aborts and larger == more speculation hits but higher risk of rollback.
	 */
	SpecConflictWindowMs int `envconfig:"SPEC_CONFLICT_WINDOW_MS" default:"500"`
}

func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	if c.SpeculationEnabled {
		if c.UndoLogTTLHours < 1 || c.UndoLogTTLHours > 168 { // 1h to 1 week
			return nil, fmt.Errorf("config: UNDO_LOG_TTL_HOURS must be 1-168, got %d", c.UndoLogTTLHours)
		}
		if c.MaxSpecOpsPerTx < 1 || c.MaxSpecOpsPerTx > 1000 {
			return nil, fmt.Errorf("config: MAX_SPEC_OPS_PER_TX must be 1-1000, got %d", c.MaxSpecOpsPerTx)
		}
		if c.SpecConflictWindowMs < 100 || c.SpecConflictWindowMs > 5000 {
			return nil, fmt.Errorf("config: SPEC_CONFLICT_WINDOW_MS must be 100-5000, got %d", c.SpecConflictWindowMs)
		}
	}
	return &c, nil
}

func (c *Config) PrepareTimeout() time.Duration {
	return time.Duration(c.PrepareTimeoutMs) * time.Millisecond
}

func (c *Config) CommitTimeout() time.Duration {
	return time.Duration(c.CommitTimeoutMs) * time.Millisecond
}

func (c *Config) TxTotalTimeout() time.Duration {
	return time.Duration(c.TxTotalTimeoutMs) * time.Millisecond
}

func (c *Config) ShardDSNs() map[string]string {
	return map[string]string{
		"shard-a": c.ShardADSN,
		"shard-b": c.ShardBDSN,
		"shard-c": c.ShardCDSN,
	}
}

func (c *Config) EtcdEndpointList() []string {
	var out []string
	for _, e := range c.EtcdEndpoints {
		for _, s := range strings.Split(e, ",") {
			if t := strings.TrimSpace(s); t != "" {
				out = append(out, t)
			}
		}
	}
	return out
}

func (c *Config) UndoLogTTL() time.Duration {
	return time.Duration(c.UndoLogTTLHours) * time.Hour
}

func (c *Config) SpecConflictWindow() time.Duration {
	return time.Duration(c.SpecConflictWindowMs) * time.Millisecond
}