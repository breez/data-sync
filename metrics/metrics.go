package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ActiveSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sync_active_subscriptions",
		Help: "Number of open ListenChanges streams.",
	})

	DistinctSubscribedUsers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sync_distinct_subscribed_users",
		Help: "Number of unique users with at least one open stream.",
	})

	NotificationsSent = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sync_notifications_sent_total",
		Help: "Total notifications successfully delivered to subscriber channels.",
	})

	NotificationsDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sync_notifications_dropped_total",
		Help: "Total notifications dropped due to full subscriber buffer.",
	})

	MsgChanDelay = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sync_msgchan_delay_seconds",
		Help:    "Time messages wait in the eventsManager msgChan before processing.",
		Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0},
	})
)
