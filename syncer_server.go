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
	newVersion, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).SetRecord(c, msg.Record.Id, msg.Record.Data, msg.Record.Version)
	if err != nil {
		if err == store.ErrSetConflict {
			return &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			}, nil
		}
		return nil, err
	}
	return &proto.SetRecordReply{
		Status:     proto.SetRecordStatus_SUCCESS,
		NewVersion: newVersion,
	}, nil
}

func (s *PersistentSyncerServer) ListChanges(c context.Context, msg *proto.ListChangesRequest) (*proto.ListChangesReply, error) {
	changed, err := c.Value(middleware.USER_DB_CONTEXT_KEY).(*store.SQLiteSyncStorage).ListChanges(c, msg.SinceVersion)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, len(changed))
	for i, r := range changed {
		records[i] = &proto.Record{
			Id:      r.RecordID,
			Data:    r.Data,
			Version: r.Version,
		}
	}
	return &proto.ListChangesReply{
		Changes: records,
	}, nil
}
