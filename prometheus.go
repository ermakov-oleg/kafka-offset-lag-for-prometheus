package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// OffsetLag is a Prometheus gauge of kafka offset lag
	OffsetLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_group_lag",
			Help: "How far behind the consumer group is from the topic head.",
		},
		[]string{
			"topic",
			"group",
			"partition",
		},
	)
	// Current state of offset by consumer group
	CurrentOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_consumer_group_current_offset",
			Help: "Current state of offset by consumer group.",
		},
		[]string{
			"topic",
			"group",
			"partition",
		},
	)
	// Timestamp of the first message in the topic partition
	FirstMessageTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_topic_first_message_timestamp",
			Help: "Timestamp of the first message in the topic partition.",
		},
		[]string{
			"topic",
			"partition",
		},
	)
	// LookupHist is a Prometheus histogram of our kafka offset lookup time
	LookupHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "kafka_consumer_group_lag_lookup_duration_seconds",
			Help:    "Histogram for the runtime of the offset request.",
			Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 15, 30, 60, 120},
		},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(CurrentOffset)
	prometheus.MustRegister(OffsetLag)
	prometheus.MustRegister(LookupHist)
	prometheus.MustRegister(FirstMessageTimestamp)
}

func prometheusListen(addr string) {
	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}
