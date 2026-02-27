package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/breez/data-sync/metrics"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestMetrics(t *testing.T) {
	cfg, privateKey := testParameters(t)
	privateKey2, err := btcec.NewPrivateKey()
	require.NoError(t, err)

	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	// Baseline
	requireMetricValue(t, metrics.ActiveSubscriptions, 0)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 0)
	requireMetricValue(t, metrics.NotificationsSent, 0)

	// --- Single subscriber ---
	ctx1, cancel1 := context.WithCancel(context.Background())
	stream1 := openListenChangesCtx(t, ctx1, privateKey, client)
	_, err = stream1.Recv() // initial empty notification
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 1)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 1)

	// --- Second subscriber, same user ---
	ctx2, cancel2 := context.WithCancel(context.Background())
	stream2 := openListenChangesCtx(t, ctx2, privateKey, client)
	_, err = stream2.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 2)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 1) // still 1 distinct user

	// --- Third subscriber, different user ---
	ctx3, cancel3 := context.WithCancel(context.Background())
	stream3 := openListenChangesCtx(t, ctx3, privateKey2, client)
	_, err = stream3.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 3)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 2) // now 2 distinct users

	// --- SetRecord for user1 → notification sent to both of user1's streams ---
	_, err = client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, &proto.Record{
		Id:       "metrics-test-1",
		Revision: 0,
		Data:     []byte("hello"),
	}))
	require.NoError(t, err)
	_, err = stream1.Recv()
	require.NoError(t, err)
	_, err = stream2.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.NotificationsSent, 2) // 2 streams for user1

	// --- Unsubscribe one of user1's streams ---
	cancel1()
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 2)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 2) // user1 still has stream2

	// --- Unsubscribe user1's last stream ---
	cancel2()
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 1)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 1) // only user2 remains

	// --- Notifications for user2 (still has stream3) ---
	_, err = client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey2, &proto.Record{
		Id:       "metrics-test-2",
		Revision: 0,
		Data:     []byte("user2-data"),
	}))
	require.NoError(t, err)
	_, err = stream3.Recv()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.NotificationsSent, 3) // 2 earlier + 1 now

	// --- Unsubscribe last stream ---
	cancel3()
	time.Sleep(100 * time.Millisecond)

	requireMetricValue(t, metrics.ActiveSubscriptions, 0)
	requireMetricValue(t, metrics.DistinctSubscribedUsers, 0)

	// --- msgChan delay histogram should have observations ---
	delayCount := testutil.CollectAndCount(metrics.MsgChanDelay)
	require.Greater(t, delayCount, 0, "msgchan_delay histogram should have observations")
}

func openListenChangesCtx(t *testing.T, ctx context.Context, privateKey *btcec.PrivateKey, client proto.SyncerClient) proto.Syncer_ListenChangesClient {
	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v", requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err)
	stream, err := client.ListenChanges(ctx, &proto.ListenChangesRequest{
		RequestTime: requestTime,
		Signature:   signature,
	})
	require.NoError(t, err)
	return stream
}

func requireMetricValue(t *testing.T, c prometheus.Collector, expected float64) {
	t.Helper()
	require.Equal(t, expected, testutil.ToFloat64(c))
}
