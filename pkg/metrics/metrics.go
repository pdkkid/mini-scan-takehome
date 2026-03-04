// Package metrics provides Prometheus instrumentation for the scan processor.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Recorder holds the Prometheus metrics for the scan processor.
type Recorder struct {
	// MessagesProcessed counts processed Pub/Sub messages by outcome:
	//   "ok"        — successfully stored and Acked
	//   "permanent" — unrecoverable parse error, Acked (dropped)
	//   "transient" — store/network error, Nacked for redelivery
	MessagesProcessed *prometheus.CounterVec

	// ProcessingDuration observes end-to-end message handling latency in seconds.
	ProcessingDuration prometheus.Histogram
}

// NewRecorder registers and returns a Recorder using the given registerer.
// Prefer prometheus.NewRegistry() over the global default so the process
// only exposes the metrics it explicitly registers.
func NewRecorder(reg prometheus.Registerer) *Recorder {
	r := &Recorder{
		MessagesProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "scan_messages_processed_total",
				Help: "Total Pub/Sub messages processed, labelled by outcome.",
			},
			[]string{"status"},
		),
		ProcessingDuration: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "scan_message_processing_duration_seconds",
				Help:    "End-to-end latency of Pub/Sub message handling.",
				Buckets: prometheus.DefBuckets,
			},
		),
	}
	reg.MustRegister(r.MessagesProcessed, r.ProcessingDuration)
	return r
}
