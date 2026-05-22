package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/mrinalxdev/spec-coordinator/internal/config"
	"github.com/mrinalxdev/spec-coordinator/internal/metrics"
	"github.com/mrinalxdev/spec-coordinator/internal/shard"
	"github.com/mrinalxdev/spec-coordinator/internal/twopc"
	"github.com/mrinalxdev/spec-coordinator/internal/txlog"
)

func main() {
	cfg, err := config.Load()
	must(err, "load config")

	log := buildLogger(cfg.LogLevel)
	defer log.Sync()
	log.Info("coordinator starting", zap.String("id", cfg.CoordinatorID))

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpointList(),
		DialTimeout: 5 * time.Second,
	})
	must(err, "connect etcd")
	defer etcdClient.Close()
	txLog := txlog.New(etcdClient)
	shardPool, err := shard.NewPool(cfg.ShardDSNs(), log)
	must(err, "connect shards")
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	coord := twopc.New(cfg, log, txLog, shardPool, m)
	recoverCtx, recoverCancel := context.WithTimeout(context.Background(), 30*time.Second)
	must(coord.Recover(recoverCtx), "recover")
	recoverCancel()
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	// POST /transfer  body: {"ops":[{"shard_id":"shard-a","account_id":1,"delta":-50.0},{"shard_id":"shard-b","account_id":10001,"delta":50.0}]}
	mux.HandleFunc("/transfer", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Ops []twopc.TransferOp `json:"ops"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		txCtx, cancel := context.WithTimeout(r.Context(), cfg.TxTotalTimeout())
		defer cancel()

		txID, err := coord.Execute(txCtx, req.Ops)
		if err != nil {
			log.Warn("transfer failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"tx_id": txID, "status": "committed"})
	})

	mux.HandleFunc("/admin/shards", func(w http.ResponseWriter, r *http.Request) {
		status := make(map[string]string)
		for _, sh := range shardPool.All() {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := sh.HealthCheck(ctx); err != nil {
				status[sh.ID] = "unhealthy: " + err.Error()
			} else {
				status[sh.ID] = "healthy"
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	httpServer := &http.Server{
		Addr:         cfg.HTTPListen,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		log.Info("HTTP listening", zap.String("addr", cfg.HTTPListen))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server error", zap.Error(err))
		}
	}()

	grpcLis, err := net.Listen("tcp", cfg.GRPCListen)
	must(err, "gRPC listen")
	log.Info("gRPC port reserved", zap.String("addr", cfg.GRPCListen),
		zap.String("note", "gRPC service registered in Milestone 2"))
	_ = grpcLis 

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	_ = httpServer.Shutdown(shutCtx)
	log.Info("coordinator stopped")
}

func buildLogger(level string) *zap.Logger {
	cfg := zap.NewProductionConfig()
	switch level {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	log, _ := cfg.Build()
	return log
}

func must(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s: %v\n", msg, err)
		os.Exit(1)
	}
}

var _ = strconv.Itoa
