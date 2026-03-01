package notifier

import "context"

// ChangeHandler is called when a notification is received.
type ChangeHandler func(pubkey string, clientID *string)

// ChangeNotifier abstracts how change notifications are broadcast.
// For SQLite (single-instance), notifications go directly to the local
// eventsManager. For PostgreSQL (multi-instance), notifications are
// broadcast via LISTEN/NOTIFY so all instances receive them.
type ChangeNotifier interface {
	// Notify broadcasts a change event for the given user.
	Notify(ctx context.Context, pubkey string, clientID *string) error
	// Start begins listening for remote notifications (PG) or is a no-op (local).
	Start(ctx context.Context) error
	// Stop gracefully shuts down the notifier.
	Stop()
}
