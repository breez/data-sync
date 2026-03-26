package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
	"github.com/breez/data-sync/store/postgres"
	"github.com/breez/data-sync/store/sqlite"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
		log.Printf("creating postgres storage\n")
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
	log.Println("Deleting expired locks on startup")
	if err := s.storage.DeleteExpiredLocks(context.Background()); err != nil {
		log.Printf("Failed to delete expired locks on startup: %v\n", err)
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("Deleting expired locks")
				if err := s.storage.DeleteExpiredLocks(context.Background()); err != nil {
					log.Printf("Failed to delete expired locks: %v\n", err)
				}
			case <-quitChan:
				return
			}
		}
	}()
	s.eventsManager.start(quitChan)
}

func validateRecord(record *proto.Record) error {
	if record == nil {
		return status.Errorf(codes.InvalidArgument, "record must not be empty")
	}
	if len(record.Id) > maxRecordIDLength {
		return status.Errorf(codes.InvalidArgument, "record id exceeds maximum length of %d", maxRecordIDLength)
	}
	if len(record.Data) > maxRecordDataSize {
		return status.Errorf(codes.InvalidArgument, "record data exceeds maximum size of %d bytes", maxRecordDataSize)
	}
	if len(record.SchemaVersion) > maxSchemaVersionLength {
		return status.Errorf(codes.InvalidArgument, "record schema_version exceeds maximum length of %d", maxSchemaVersionLength)
	}
	return nil
}

func (s *PersistentSyncerServer) SetRecord(ctx context.Context, msg *proto.SetRecordRequest) (*proto.SetRecordReply, error) {
	log.Println("SetRecord: started")
	if err := validateRecord(msg.Record); err != nil {
		return nil, err
	}
	if err := validateRequestTime(msg.RequestTime); err != nil {
		return nil, err
	}
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
	s.eventsManager.notifyChange(c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string), msg.ClientId)
	log.Println("SetRecord: finished")
	return &proto.SetRecordReply{
		Status:      proto.SetRecordStatus_SUCCESS,
		NewRevision: newRevision,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(ctx context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	log.Println("ListChanges: started")
	if err := validateRequestTime(msg.RequestTime); err != nil {
		return nil, err
	}
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		log.Printf("ListChanges completed with auth error: %v\n", err)
		return nil, err
	}
	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	log.Printf("ListChanges: pubkey: %v\n", pubkey)
	changed, err := s.storage.ListChanges(c, pubkey, msg.SinceRevision, maxListChangesLimit)
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

func (s *PersistentSyncerServer) ListenChanges(request *proto.ListenChangesRequest, stream proto.Syncer_ListenChangesServer) error {
	if err := validateRequestTime(request.RequestTime); err != nil {
		return err
	}
	context, err := middleware.Authenticate(s.config, stream.Context(), request)
	if err != nil {
		return err
	}

	pubkey := context.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	subscription := s.eventsManager.subscribe(pubkey)
	defer s.eventsManager.unsubscribe(pubkey, subscription.id)

	if err := stream.Send(&proto.Notification{}); err != nil {
		return err
	}
	for {
		select {
		case notification, ok := <-subscription.eventsChan:
			if !ok {
				// No more payment updates.
				return nil
			}

			// Send event to the client.
			if err := stream.Send(notification); err != nil {
				return err
			}

		case <-context.Done():
			return nil
		}
	}
}

const (
	defaultLockTTLSeconds  = 30
	minLockTTLSeconds      = 1
	maxLockTTLSeconds      = 300
	maxRequestAge          = 5 * time.Minute
	maxLockNameLength      = 256
	maxListChangesLimit    = 1000
	maxRecordDataSize      = 65536 // 64 KB
	maxRecordIDLength      = 64
	maxSchemaVersionLength = 15
)

func validateLockName(name string) error {
	if name == "" {
		return status.Errorf(codes.InvalidArgument, "lock_name must not be empty")
	}
	if len(name) > maxLockNameLength {
		return status.Errorf(codes.InvalidArgument, "lock_name exceeds maximum length of %d", maxLockNameLength)
	}
	return nil
}

func validateInstanceID(instanceID string) error {
	if _, err := uuid.Parse(instanceID); err != nil {
		return status.Errorf(codes.InvalidArgument, "instance_id must be a valid UUID")
	}
	return nil
}

func validateRequestTime(requestTime uint32) error {
	diff := time.Since(time.Unix(int64(requestTime), 0))
	if diff.Abs() > maxRequestAge {
		return status.Errorf(codes.InvalidArgument, "request time too far from server time")
	}
	return nil
}

func (s *PersistentSyncerServer) SetLock(ctx context.Context, msg *proto.SetLockRequest) (*proto.SetLockReply, error) {
	log.Println("SetLock: started")
	if err := validateLockName(msg.LockName); err != nil {
		return nil, err
	}
	if err := validateInstanceID(msg.InstanceId); err != nil {
		return nil, err
	}
	if err := validateRequestTime(msg.RequestTime); err != nil {
		return nil, err
	}
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		log.Printf("SetLock completed with auth error: %v\n", err)
		return nil, err
	}
	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	log.Printf("SetLock: pubkey: %v, lock_name: %v, acquire: %v\n", pubkey, msg.LockName, msg.Acquire)

	ttl := defaultLockTTLSeconds
	if msg.TtlSeconds != nil {
		if int(*msg.TtlSeconds) < minLockTTLSeconds {
			return nil, status.Errorf(codes.InvalidArgument, "ttl_seconds must be at least %d", minLockTTLSeconds)
		}
		ttl = min(int(*msg.TtlSeconds), maxLockTTLSeconds)
	}

	err = s.storage.SetLock(c, pubkey, msg.LockName, msg.InstanceId, msg.Acquire, uint32(ttl), msg.Exclusive)
	if err != nil {
		if err == store.ErrLockHeld {
			return nil, status.Errorf(codes.FailedPrecondition, "lock held by another instance")
		}
		return nil, err
	}
	log.Println("SetLock: finished")
	return &proto.SetLockReply{}, nil
}

func (s *PersistentSyncerServer) GetLock(ctx context.Context, msg *proto.GetLockRequest) (*proto.GetLockReply, error) {
	log.Println("GetLock: started")
	if err := validateLockName(msg.LockName); err != nil {
		return nil, err
	}
	if err := validateRequestTime(msg.RequestTime); err != nil {
		return nil, err
	}
	c, err := middleware.Authenticate(s.config, ctx, msg)
	if err != nil {
		log.Printf("GetLock completed with auth error: %v\n", err)
		return nil, err
	}
	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)
	log.Printf("GetLock: pubkey: %v, lock_name: %v\n", pubkey, msg.LockName)

	locked, err := s.storage.HasActiveLock(c, pubkey, msg.LockName)
	if err != nil {
		return nil, err
	}
	log.Printf("GetLock: finished, locked: %v\n", locked)
	return &proto.GetLockReply{Locked: locked}, nil
}

type notifyChange struct {
	pubkey   string
	clientId *string
}

type unsubscribe struct {
	pubkey string
	id     int64
}

type subscription struct {
	id         int64
	pubkey     string
	eventsChan chan *proto.Notification
}

type eventsManager struct {
	sync.Mutex
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
			log.Printf("eventsManager select started\n")
			select {
			case msg := <-c.msgChan:
				if s, ok := msg.(*subscription); ok {
					c.streams[s.pubkey] = append(c.streams[s.pubkey], s)
					log.Printf("eventsManager: new subscription for user %s: id - %d\n", s.pubkey, s.id)
				}
				if s, ok := msg.(*unsubscribe); ok {
					log.Printf("eventsManager: unsubscribing user %s: id - %d\n", s.pubkey, s.id)
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
					log.Printf("eventsManager: notifying change for user %v\n", s.pubkey)
					for _, sub := range c.streams[s.pubkey] {
						select {
						case sub.eventsChan <- &proto.Notification{ClientId: s.clientId}:
						default:
							log.Printf("eventsManager: dropping notification for user %v subscription %d: channel full\n", s.pubkey, sub.id)
						}
					}
				}
			case <-quitChan:
				log.Printf("eventsManager: quitChan received\n")
				return
			}
			log.Printf("eventsManager select finished. number of subscriptions = %v\n", len(c.streams))
		}
	}()
}

func (c *eventsManager) notifyChange(pubkey string, clientId *string) {
	c.msgChan <- &notifyChange{pubkey: pubkey, clientId: clientId}
}

func (c *eventsManager) subscribe(pubkey string) *subscription {
	eventsChan := make(chan *proto.Notification, 10)
	c.Lock()
	c.globalIDs += 1
	s := &subscription{pubkey: pubkey, eventsChan: eventsChan, id: c.globalIDs}
	c.Unlock()

	c.msgChan <- s
	log.Printf("New connection for user %s: id - %d\n", pubkey, s.id)
	return s
}

func (c *eventsManager) unsubscribe(pubkey string, id int64) {
	c.msgChan <- &unsubscribe{pubkey: pubkey, id: id}
	log.Printf("Removing connection for user %s - id %d\n", pubkey, id)
}
