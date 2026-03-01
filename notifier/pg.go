package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// PgNotifySender can execute a PostgreSQL NOTIFY on a named channel.
type PgNotifySender interface {
	PgNotify(ctx context.Context, channel, payload string) error
}

const (
	pgChannel         = "data_sync_changes"
	reconnectDelay    = 1 * time.Second
	maxReconnectDelay = 30 * time.Second
)

// pgPayload is the JSON structure sent as the NOTIFY payload.
type pgPayload struct {
	Pubkey   string  `json:"p"`
	ClientID *string `json:"c,omitempty"`
}

// PGNotifier broadcasts notifications via PostgreSQL LISTEN/NOTIFY.
// NOTIFY is sent through the pooled connection (fine for single statements).
// LISTEN uses a direct connection that bypasses the connection pooler.
type PGNotifier struct {
	sender    PgNotifySender
	directURL string
	handler   ChangeHandler
	cancel    context.CancelFunc
}

func NewPGNotifier(sender PgNotifySender, directURL string, handler ChangeHandler) *PGNotifier {
	return &PGNotifier{
		sender:    sender,
		directURL: directURL,
		handler:   handler,
	}
}

// Notify sends a notification to all instances via PostgreSQL NOTIFY.
func (n *PGNotifier) Notify(ctx context.Context, pubkey string, clientID *string) error {
	p := pgPayload{
		Pubkey:   pubkey,
		ClientID: clientID,
	}
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("failed to marshal notification payload: %w", err)
	}
	if err := n.sender.PgNotify(ctx, pgChannel, string(data)); err != nil {
		return fmt.Errorf("pg_notify failed: %w", err)
	}
	return nil
}

// Start begins the LISTEN loop in a background goroutine.
// It reconnects with exponential backoff on connection failure.
func (n *PGNotifier) Start(ctx context.Context) error {
	ctx, n.cancel = context.WithCancel(ctx)
	go n.listenLoop(ctx)
	return nil
}

// Stop cancels the listener context, causing the LISTEN loop to exit.
func (n *PGNotifier) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
}

func (n *PGNotifier) listenLoop(ctx context.Context) {
	delay := reconnectDelay
	for {
		if ctx.Err() != nil {
			return
		}
		err := n.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		log.Printf("pgNotifier: listen connection lost: %v, reconnecting in %v", err, delay)
		select {
		case <-time.After(delay):
			delay = min(delay*2, maxReconnectDelay)
		case <-ctx.Done():
			return
		}
	}
}

func (n *PGNotifier) listenOnce(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, n.directURL)
	if err != nil {
		return fmt.Errorf("failed to connect for LISTEN: %w", err)
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(ctx, "LISTEN "+pgChannel)
	if err != nil {
		return fmt.Errorf("LISTEN failed: %w", err)
	}
	log.Printf("pgNotifier: listening on channel %q", pgChannel)

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("WaitForNotification: %w", err)
		}

		var p pgPayload
		if err := json.Unmarshal([]byte(notification.Payload), &p); err != nil {
			log.Printf("pgNotifier: ignoring malformed payload %q: %v", notification.Payload, err)
			continue
		}

		n.handler(p.Pubkey, p.ClientID)
	}
}
