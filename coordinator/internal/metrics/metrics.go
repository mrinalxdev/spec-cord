package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	TxTotal      *prometheus.CounterVec
	TxLatency    *prometheus.HistogramVec
	ShardLatency *prometheus.HistogramVec
	SpeculationTotal *prometheus.CounterVec
	RollbackTotal    prometheus.Counter
}

func New(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		TxTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "coordinator_tx_total",
			Help: "Total transactions by outcome",
		}, []string{"outcome"}),

		TxLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "coordinator_tx_latency_seconds",
			Help:    "End-to-end transaction latency",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"outcome"}),

		ShardLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "coordinator_shard_latency_seconds",
			Help:    "Per-shard prepare/commit round-trip latency",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
		}, []string{"shard", "phase"}),

		// ── Milestone 2 stubs ────────────────────────────────────────────────
		SpeculationTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "coordinator_speculation_total",
			Help: "Speculative commit attempts by result (hit/miss) — active from Milestone 2",
		}, []string{"result"}),

		RollbackTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "coordinator_rollback_total",
			Help: "Undo-log rollbacks triggered — active from Milestone 2",
		}),
	}
}
