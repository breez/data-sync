package main

import (
	"context"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/breez/data-sync/store"
)

type PersistentSyncerServer struct {
	proto.UnimplementedSyncerServer
	config *config.Config
}

func (s *PersistentSyncerServer) SetRecord(c context.Context, msg *proto.SetRecordRequest) (*proto.SetRecordReply, error) {
	newId, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).SetRecord(
		c,
		msg.Record.Id,
		msg.Record.Version,
		msg.Record.Data,
	)
	if err != nil {
		if err == store.ErrSetConflict {
			return &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			}, nil
		}
		return nil, err
	}
	return &proto.SetRecordReply{
		Status: proto.SetRecordStatus_SUCCESS,
		NewId:  newId,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(c context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	changed, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).ListChanges(c, msg.FromId)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, len(changed))
	for i, r := range changed {
		records[i] = &proto.Record{
			Id:      r.Id,
			Version: r.Version,
			Data:    r.Data,
		}
	}
	return &proto.ListChangesReply{
		Changes: records,
	}, nil
}
