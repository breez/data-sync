package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
	"github.com/breez/data-sync/store/postgres"
	"github.com/breez/data-sync/store/sqlite"
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
	storage       store.SyncStorage
}

func NewPersistentSyncerServer(config *config.Config) (*PersistentSyncerServer, error) {

	var storage store.SyncStorage
	var err error

	if config.PgDatabaseUrl != "" {
		log.Printf("creating postgres storage: %v\n", config.PgDatabaseUrl)
		storage, err = postgres.NewPGSyncStorage(config.PgDatabaseUrl)
		if err != nil {
			return nil, err
		}
	} else {
		log.Printf("creating sqlite storage: %v\n", config.SQLiteDirPath)
		if err := os.MkdirAll(config.SQLiteDirPath, 0700); err != nil {
			return nil, fmt.Errorf("failed to create databases directory %w", err)
		}
		file := fmt.Sprintf("%v/db.sqlite", config.SQLiteDirPath)
		storage, err = sqlite.NewSQLiteSyncStorage(file)
		if err != nil {
			return nil, err
		}
	}

	return &PersistentSyncerServer{
		config:        config,
		eventsManager: newEventsManager(),
		storage:       storage,
	}, nil
}

func (s *PersistentSyncerServer) Start(quitChan chan struct{}) {
	s.eventsManager.start(quitChan)
}

func (s *PersistentSyncerServer) SetRecord(ctx context.Context, msg *proto.SetRecordRequest) (*proto.SetRecordReply, error) {
	log.Println("SetRecord: started")
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		log.Printf("SetRecord completed with auth error: %v\n", err)
		return nil, err
	}
	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	log.Printf("SetRecord: pubkey: %v\n", pubkey)
	newRevision, err := s.storage.SetRecord(c, pubkey, msg.Record.Id, msg.Record.Data, msg.Record.Revision, msg.Record.SchemaVersion)

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
	log.Println("SetRecord: finished")
	return &proto.SetRecordReply{
		Status:      proto.SetRecordStatus_SUCCESS,
		NewRevision: newRevision,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(ctx context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	log.Println("ListChanges: started")
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		log.Printf("ListChanges completed with auth error: %v\n", err)
		return nil, err
	}
	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	log.Printf("ListChanges: pubkey: %v\n", pubkey)
	changed, err := s.storage.ListChanges(c, pubkey, msg.SinceRevision)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, len(changed))
	for i, r := range changed {
		records[i] = &proto.Record{
			Id:            r.Id,
			Data:          r.Data,
			Revision:      r.Revision,
			SchemaVersion: r.SchemaVersion,
		}
	}
	log.Printf("ListChanges: finished with %v records\n", len(records))
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
	log.Printf("New connection for user %s: id - %d\n", pubkey, c.globalIDs)
	return s
}

func (c *eventsManager) unsubscribe(pubkey string, id int64) {
	c.msgChan <- &unsubscribe{pubkey: pubkey, id: id}
	log.Printf("Removing connection for user %s - id %d\n", pubkey, id)
}
