package notifier

import "context"

// LocalNotifier dispatches notifications directly to the local eventsManager.
// Used when running with SQLite (single-instance) or when no direct
// PostgreSQL URL is configured.
type LocalNotifier struct {
	handler ChangeHandler
}

func NewLocalNotifier(handler ChangeHandler) *LocalNotifier {
	return &LocalNotifier{handler: handler}
}

func (n *LocalNotifier) Notify(_ context.Context, pubkey string, clientID *string) error {
	n.handler(pubkey, clientID)
	return nil
}

func (n *LocalNotifier) Start(_ context.Context) error {
	return nil
}

func (n *LocalNotifier) Stop() {}
