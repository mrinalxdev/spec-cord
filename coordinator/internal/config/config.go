package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	CoordinatorID string `envconfig:"COORDINATOR_ID" default:"coordinator-1"`
	EtcdEndpoints []string `envconfig:"ETCD_ENDPOINTS" default:"localhost:2379"`
	ShardADSN string `envconfig:"SHARD_A_DSN" required:"true"`
	ShardBDSN string `envconfig:"SHARD_B_DSN" required:"true"`
	ShardCDSN string `envconfig:"SHARD_C_DSN" required:"true"`
	GRPCListen string `envconfig:"GRPC_LISTEN" default:":9090"`
	HTTPListen string `envconfig:"HTTP_LISTEN" default:":8080"`
	PrepareTimeoutMs  int `envconfig:"PREPARE_TIMEOUT_MS" default:"2000"`
	CommitTimeoutMs   int `envconfig:"COMMIT_TIMEOUT_MS"  default:"2000"`
	TxTotalTimeoutMs  int `envconfig:"TX_TOTAL_TIMEOUT_MS" default:"10000"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`

	SpeculationEnabled bool `envconfig:"SPECULATION_ENABLED" defualt:"false"`
	SpeculationTimeoutMs int `envconfig:"SPECULATION_TIMEOUT_MS" default:"5000"`
}

/*
 * adding a helper method for the timeout 
 */
 func (c *Config) SpeculationTimeout() time.Duration {
 	return time.Duration(c.SpeculationTimeoutMs) * time.Millisecond
 }


func Load() (*Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
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
