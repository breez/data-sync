package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
)

type User struct {
	records_channel chan (*proto.Record)
	listeners       []proto.Syncer_ListenChangesServer
	mutex           sync.RWMutex
}

type PersistentSyncerServer struct {
	proto.UnimplementedSyncerServer
	config *config.Config

	users map[string](*User)
	mutex sync.RWMutex
}

func (s *PersistentSyncerServer) SetRecord(c context.Context, msg *proto.SetRecordRequest) (*proto.SetRecordReply, error) {
	newRevision, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).SetRecord(c, msg.Record.Id, msg.Record.Data, msg.Record.Revision)
	if err != nil {
		if err == store.ErrSetConflict {
			return &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			}, nil
		}
		return nil, err
	}

	pubkey := c.Value(middleware.USER_PUBKEY_CONTEXT_KEY).(string)

	if _, exists := s.users[pubkey]; !exists {
		addUser(s, pubkey)
	}
	s.users[pubkey].records_channel <- msg.Record

	return &proto.SetRecordReply{
		Status:      proto.SetRecordStatus_SUCCESS,
		NewRevision: newRevision,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(c context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	changed, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).ListChanges(c, msg.SinceRevision)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, len(changed))
	for i, r := range changed {
		records[i] = &proto.Record{
			Id:       r.RecordID,
			Data:     r.Data,
			Revision: r.Revision,
		}
	}
	return &proto.ListChangesReply{
		Changes: records,
	}, nil
}

func addUser(s *PersistentSyncerServer, pubkey string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.users[pubkey] = &User{
		make(chan (*proto.Record)),
		[]proto.Syncer_ListenChangesServer{},
		sync.RWMutex{},
	}
	log.Printf("New user detected - pubkey %s id %p", pubkey, s.users[pubkey])
}

func removeUser(s *PersistentSyncerServer, pubkey string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.users, pubkey)
	log.Println("Removed user", pubkey)
}

func addListener(c *User, srv proto.Syncer_ListenChangesServer) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.listeners = append(c.listeners, srv)
	log.Printf("New listener detected for user %p\n", c)
}

func removeListener(c *User, srv proto.Syncer_ListenChangesServer) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, listener := range c.listeners {
		if listener == srv {
			c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
		}
	}
	log.Printf("Removed listener for user %p\n", c)
}

func (s *PersistentSyncerServer) ListenChanges(msg *proto.ListenChangesRequest, srv proto.Syncer_ListenChangesServer) error {
	var toVerify string
	var signature string

	toVerify = fmt.Sprintf("%v", msg.RequestTime)
	signature = msg.Signature

	pubkeyBytes, err := middleware.VerifyMessage([]byte(toVerify), signature)
	if err != nil {
		return err
	}

	pubkey := hex.EncodeToString(pubkeyBytes.SerializeCompressed())
	if _, exists := s.users[pubkey]; !exists {
		addUser(s, pubkey)
	}
	addListener(s.users[pubkey], srv)

	user := s.users[pubkey]
	for record := range user.records_channel {
		user.mutex.RLock()
		for _, listener := range user.listeners {
			listener.Send(record)
		}
		user.mutex.RUnlock()
	}

	return nil
}
