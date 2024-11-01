package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type testCase struct {
	name    string
	request protoreflect.ProtoMessage
	reply   protoreflect.ProtoMessage
}

func TestSyncService(t *testing.T) {
	config, privateKey := testParameters(t)
	client, closer := server(context.Background(), config)
	defer closer()
	defer func() {
		os.RemoveAll(config.UsersDatabasesDir)
	}()

	for _, testCase := range testCases() {
		if setRecordRequest, ok := testCase.request.(*proto.SetRecordRequest); ok {
			testSetRecord(t, privateKey, client, setRecordRequest, testCase)
		}
		if listChangesRequest, ok := testCase.request.(*proto.ListChangesRequest); ok {
			testListChanges(t, privateKey, client, listChangesRequest, testCase)
		}
	}
}

func TestTrackChanges(t *testing.T) {
	config, privateKey := testParameters(t)
	client, closer := server(context.Background(), config)
	defer closer()
	defer func() {
		os.RemoveAll(config.UsersDatabasesDir)
	}()

	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v", requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request := &proto.TrackChangesRequest{
		RequestTime: requestTime,
		Signature:   signature,
	}
	request.RequestTime = requestTime
	request.Signature = signature
	stream, err := client.TrackChanges(context.Background(), request)
	require.NoError(t, err, "failed to call TrackChanges")

	// check realtime changes
	client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, &proto.Record{
		Id:       "1",
		Revision: 0,
		Data:     []byte("revision1"),
	}))
	record, err := stream.Recv()
	require.NoError(t, err, "failed to receive record")
	require.Equal(t, record.Data, []byte("revision1"))
}

func createSetRecordRequest(t *testing.T, privateKey *btcec.PrivateKey, record *proto.Record) *proto.SetRecordRequest {
	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v-%x-%v-%v", record.Id, record.Data, record.Revision, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	return &proto.SetRecordRequest{
		Record:      record,
		RequestTime: requestTime,
		Signature:   signature,
	}
}

func testParameters(t *testing.T) (*config.Config, *btcec.PrivateKey) {
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "failed to create private key")
	os.RemoveAll(config.UsersDatabasesDir)
	return config, privateKey
}

func testCases() []testCase {
	return []testCase{

		// empty db, no changes.
		{
			name: "empty db, no changes",
			request: &proto.ListChangesRequest{
				SinceRevision: 0,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{},
			},
		},

		// set first record
		{
			name: "initial record insert",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:       "1",
					Revision: 0,
					Data:     []byte("revision1"),
				},
			},
			reply: &proto.SetRecordReply{
				Status:      proto.SetRecordStatus_SUCCESS,
				NewRevision: 1,
			},
		},

		// update record
		{
			name: "initial record update",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:       "1",
					Revision: 1,
					Data:     []byte("revision2"),
				},
			},
			reply: &proto.SetRecordReply{
				Status:      proto.SetRecordStatus_SUCCESS,
				NewRevision: 2,
			},
		},

		// test conflict
		{
			name: "test conflict",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:       "1",
					Revision: 1,
					Data:     []byte("revision3"),
				},
			},
			reply: &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			},
		},

		// no changes since revision 5
		{
			name: "empty changes",
			request: &proto.ListChangesRequest{
				SinceRevision: 5,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{},
			},
		},

		// 1 record changes
		{
			name: "list changes returns 1 record",
			request: &proto.ListChangesRequest{
				SinceRevision: 0,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{
					{
						Id:       "1",
						Revision: 2,
						Data:     []byte("revision2"),
					},
				},
			},
		},
	}
}

func testSetRecord(t *testing.T, privateKey *btcec.PrivateKey, client proto.SyncerClient, request *proto.SetRecordRequest, test testCase) {
	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v-%x-%v-%v", request.Record.Id, request.Record.Data, request.Record.Revision, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request.RequestTime = requestTime
	request.Signature = signature
	response, err := client.SetRecord(context.Background(), request)
	require.NoError(t, err, "failed to call SetRecord")
	res, err := json.Marshal(response)
	require.NoError(t, err, "failed to marshal response")
	expected, err := json.Marshal(test.reply)
	require.NoError(t, err, "failed to marshal expected response")
	require.Equal(t, res, expected, fmt.Sprintf("failed to compare test results for %v", test.name))
}

func testListChanges(t *testing.T, privateKey *btcec.PrivateKey, client proto.SyncerClient, request *proto.ListChangesRequest, test testCase) {
	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v-%v", request.SinceRevision, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request.RequestTime = requestTime
	request.Signature = signature
	response, err := client.ListChanges(context.Background(), request)
	require.NoError(t, err, "failed to call ListChanges")
	res, err := json.Marshal(response)
	require.NoError(t, err, "failed to marshal response")
	expected, err := json.Marshal(test.reply)
	require.NoError(t, err, "failed to marshal expected response")
	require.Equal(t, res, expected, fmt.Sprintf("failed to compare test results for %v", test.name))
}

func server(ctx context.Context, config *config.Config) (proto.SyncerClient, func()) {
	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)

	quitChan := make(chan struct{})
	syncServer := NewPersistentSyncerServer(config)
	syncServer.Start(quitChan)

	baseServer := CreateServer(config, lis, syncServer)
	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	conn, err := grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	closer := func() {
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
		quitChan <- struct{}{}
	}

	client := proto.NewSyncerClient(conn)

	return client, closer
}
