package main

import (
	"context"
	"sync"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
)

type changeRecordEvent struct {
	pubkey string
	record *proto.Record
}

type PersistentSyncerServer struct {
	proto.UnimplementedSyncerServer
	sync.Mutex
	config        *config.Config
	eventsManager *eventsManager
}

func NewPersistentSyncerServer(config *config.Config) *PersistentSyncerServer {
	return &PersistentSyncerServer{
		config:        config,
		eventsManager: newEventsManager(),
	}
}

func (s *PersistentSyncerServer) Start(quitChan chan struct{}) {
	s.eventsManager.start(quitChan)
}

func (s *PersistentSyncerServer) SetRecord(ctx context.Context, msg *proto.SetRecordRequest) (*proto.SetRecordReply, error) {
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		return nil, err
	}
	newRevision, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).SetRecord(c, msg.Record.Id, msg.Record.Data, msg.Record.Revision)
	if err != nil {
		if err == store.ErrSetConflict {
			return &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			}, nil
		}
		return nil, err
	}
	newRecord := msg.Record
	newRecord.Revision = newRevision
	s.eventsManager.notifyChange(c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string), newRecord)
	return &proto.SetRecordReply{
		Status:      proto.SetRecordStatus_SUCCESS,
		NewRevision: newRevision,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(ctx context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		return nil, err
	}
	changed, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).ListChanges(c, msg.SinceRevision)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, len(changed))
	for i, r := range changed {
		records[i] = &proto.Record{
			Id:       r.Id,
			Data:     r.Data,
			Revision: r.Revision,
		}
	}
	return &proto.ListChangesReply{
		Changes: records,
	}, nil
}

func (s *PersistentSyncerServer) TrackChanges(request *proto.TrackChangesRequest, stream proto.Syncer_TrackChangesServer) error {
	context, err := middleware.Authenticate(s.config, stream.Context(), request)
	if err != nil {
		return err
	}

	pubkey := context.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	subscription := s.eventsManager.subscribe(pubkey)
	defer s.eventsManager.unsubscribe(pubkey, subscription.id)
	for {
		select {
		case event, ok := <-subscription.eventsChan:
			if !ok {
				// No more payment updates.
				return nil
			}

			// Send event to the client.
			if err := stream.Send(event.record); err != nil {
				return err
			}

		case <-context.Done():
			return nil
		}
	}
}

type notifyChange struct {
	pubkey string
	record *proto.Record
}

type unsubscribe struct {
	pubkey string
	id     int64
}

type subscription struct {
	id         int64
	pubkey     string
	eventsChan chan *changeRecordEvent
}

type eventsManager struct {
	globalIDs int64
	streams   map[string][]*subscription
	msgChan   chan interface{}
}

func newEventsManager() *eventsManager {
	return &eventsManager{
		globalIDs: 0,
		streams:   make(map[string][]*subscription),
		msgChan:   make(chan interface{}),
	}

}

func (c *eventsManager) start(quitChan chan struct{}) {
	go func() {
		for {
			select {
			case msg := <-c.msgChan:
				if s, ok := msg.(*subscription); ok {
					c.streams[s.pubkey] = append(c.streams[s.pubkey], s)
				}
				if s, ok := msg.(*unsubscribe); ok {
					var newSubs []*subscription
					for _, sub := range c.streams[s.pubkey] {
						if sub.id != s.id {
							newSubs = append(newSubs, sub)
							continue
						}
						close(sub.eventsChan)
					}
					delete(c.streams, s.pubkey)
					if len(newSubs) > 0 {
						c.streams[s.pubkey] = newSubs
					}
				}
				if s, ok := msg.(*notifyChange); ok {
					for _, sub := range c.streams[s.pubkey] {
						sub.eventsChan <- &changeRecordEvent{pubkey: s.pubkey, record: s.record}
					}
				}

			case <-quitChan:
				return
			}
		}
	}()
}

func (c *eventsManager) notifyChange(pubkey string, record *proto.Record) {
	c.msgChan <- &notifyChange{pubkey: pubkey, record: record}
}

func (c *eventsManager) subscribe(pubkey string) *subscription {
	eventsChan := make(chan *changeRecordEvent)
	c.globalIDs += 1
	s := &subscription{pubkey: pubkey, eventsChan: eventsChan, id: c.globalIDs}
	c.msgChan <- s
	return s
}

func (c *eventsManager) unsubscribe(pubkey string, id int64) {
	c.msgChan <- &unsubscribe{pubkey: pubkey, id: id}
}
