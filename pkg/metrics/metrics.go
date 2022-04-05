// Package metrics defines prometheus metrics.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const subsystem = "placement_controller"

// Gauge is an implementation of controller.MetricGauge.
type Gauge prometheus.GaugeVec

// Inc implements controller.MetricGauage.Inc.
func (g *Gauge) Inc(zone string) {
	(*prometheus.GaugeVec)(g).WithLabelValues(zone).Inc()
}

// Dec implements controller.MetricGauage.Dec.
func (g *Gauge) Dec(zone string) {
	(*prometheus.GaugeVec)(g).WithLabelValues(zone).Dec()
}

// Distribution is an implementation of controller.MetricDistribution.
type Distribution prometheus.HistogramVec

// Observe implements controller.MetricDistribution.Observe.
func (d *Distribution) Observe(zone string, v time.Duration) {
	(*prometheus.HistogramVec)(d).WithLabelValues(zone).Observe(float64(v))
}

var (
	// RequestedPolicies defines the number of instances with a policy request.
	RequestedPolicies = (*Gauge)(prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "requested_policies",
			Help:      "The number of outstanding policy requests.",
		},
		[]string{
			"zone", // For example, "us-central1-c", "europe-west4-a".
		},
	))
	// AppliedPolicies defines the number of instances with an applied policy.
	AppliedPolicies = (*Gauge)(prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: subsystem,
			Name:      "applied_policies",
			Help:      "The number of actively applied policies.",
		},
		[]string{
			"zone", // For example, "us-central1-c", "europe-west4-a".
		},
	))
	// AddPolicyDuration represents the duration of the add policy call alone.
	AddPolicyDuration = (*Distribution)(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "add_policy_duration",
			Help:      "The add policy API request duration in seconds.",
			Buckets:   prometheus.ExponentialBuckets(120, 1.2, 10),
		},
		[]string{
			"zone", // For example, "us-central1-c", "europe-west4-a".
		},
	))
	// ApplyPolicyDuration represents the duration of the add policy call and simluated maintenance event.
	ApplyPolicyDuration = (*Distribution)(prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Subsystem: subsystem,
			Name:      "apply_policy_duration",
			Help:      "The add policy API request duration and simulated maintenance event in seconds.",
			Buckets:   prometheus.ExponentialBuckets(120, 1.2, 10),
		},
		[]string{
			"zone", // For example, "us-central1-c", "europe-west4-a".
		},
	))
)

func init() {
	prometheus.MustRegister((*prometheus.GaugeVec)(RequestedPolicies))
	prometheus.MustRegister((*prometheus.GaugeVec)(AppliedPolicies))
	prometheus.MustRegister((*prometheus.HistogramVec)(AddPolicyDuration))
	prometheus.MustRegister((*prometheus.HistogramVec)(ApplyPolicyDuration))
}
