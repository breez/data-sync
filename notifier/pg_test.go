package notifier

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_PG_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_PG_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), url)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func testDirectURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_PG_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_PG_DATABASE_URL not set")
	}
	return url
}

func TestPGNotifier_CrossInstance(t *testing.T) {
	pool := testPool(t)
	directURL := testDirectURL(t)

	// Channels to collect received notifications
	receivedA := make(chan pgPayload, 10)
	receivedB := make(chan pgPayload, 10)

	handlerA := func(pubkey string, clientID *string) {
		receivedA <- pgPayload{Pubkey: pubkey, ClientID: clientID}
	}
	handlerB := func(pubkey string, clientID *string) {
		receivedB <- pgPayload{Pubkey: pubkey, ClientID: clientID}
	}

	notifierA := NewPGNotifier(pool, directURL, handlerA)
	notifierB := NewPGNotifier(pool, directURL, handlerB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, notifierA.Start(ctx))
	require.NoError(t, notifierB.Start(ctx))

	// Give listeners time to connect and issue LISTEN
	time.Sleep(200 * time.Millisecond)

	// Send a notification from notifier A
	testPubkey := "02abc123def456"
	testClientID := "client-1"
	err := notifierA.Notify(context.Background(), testPubkey, &testClientID)
	require.NoError(t, err)

	// Both A and B should receive the notification (unified model: all instances
	// receive via LISTEN, including the originator)
	select {
	case p := <-receivedA:
		require.Equal(t, testPubkey, p.Pubkey)
		require.NotNil(t, p.ClientID)
		require.Equal(t, testClientID, *p.ClientID)
	case <-time.After(5 * time.Second):
		t.Fatal("notifier A did not receive notification")
	}

	select {
	case p := <-receivedB:
		require.Equal(t, testPubkey, p.Pubkey)
		require.NotNil(t, p.ClientID)
		require.Equal(t, testClientID, *p.ClientID)
	case <-time.After(5 * time.Second):
		t.Fatal("notifier B did not receive notification")
	}

	// Clean up
	notifierA.Stop()
	notifierB.Stop()
}

func TestPGNotifier_NilClientID(t *testing.T) {
	pool := testPool(t)
	directURL := testDirectURL(t)

	received := make(chan pgPayload, 10)
	handler := func(pubkey string, clientID *string) {
		received <- pgPayload{Pubkey: pubkey, ClientID: clientID}
	}

	n := NewPGNotifier(pool, directURL, handler)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, n.Start(ctx))
	time.Sleep(200 * time.Millisecond)

	testPubkey := "02def789"
	err := n.Notify(context.Background(), testPubkey, nil)
	require.NoError(t, err)

	select {
	case p := <-received:
		require.Equal(t, testPubkey, p.Pubkey)
		require.Nil(t, p.ClientID)
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive notification")
	}

	n.Stop()
}

func TestPGNotifier_GracefulShutdown(t *testing.T) {
	pool := testPool(t)
	directURL := testDirectURL(t)

	handler := func(pubkey string, clientID *string) {}
	n := NewPGNotifier(pool, directURL, handler)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, n.Start(ctx))
	time.Sleep(200 * time.Millisecond)

	// Stopping should not hang
	cancel()
	n.Stop()
}
