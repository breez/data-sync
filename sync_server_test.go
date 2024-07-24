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

	expectedRecords := []*proto.Record{
		{
			Id:      "1",
			Version: 0,
			Data:    []byte("version1"),
		},
		{
			Id:      "2",
			Version: 0,
			Data:    []byte("version1"),
		},
	}
	client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, expectedRecords[0]))
	client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, expectedRecords[1]))

	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v-%v", 0, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request := &proto.TrackChangesRequest{
		RequestTime:  requestTime,
		Signature:    signature,
		SinceVersion: 0,
	}
	request.RequestTime = requestTime
	request.Signature = signature
	stream, err := client.TrackChanges(context.Background(), request)
	require.NoError(t, err, "failed to call TrackChanges")

	// check historical changes
	var records []*proto.Record
	for i := 0; i < 2; i++ {
		record, err := stream.Recv()
		require.NoError(t, err, "failed to receive record")
		if err != nil {
			break
		}
		records = append(records, record)
		expectedRecords[i].Version = record.Version
	}
	res, _ := json.Marshal(records)
	expected, _ := json.Marshal(expectedRecords)
	require.Equal(t, string(res), string(expected), "failed to compare test results")

	// check realtime changes
	client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, &proto.Record{
		Id:      expectedRecords[1].Id,
		Version: expectedRecords[1].Version,
		Data:    []byte("updated"),
	}))
	record, err := stream.Recv()
	require.NoError(t, err, "failed to receive record")
	require.Equal(t, record.Data, []byte("updated"))
}

func createSetRecordRequest(t *testing.T, privateKey *btcec.PrivateKey, record *proto.Record) *proto.SetRecordRequest {
	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v-%v-%x-%v", record.Id, record.Version, record.Data, requestTime)
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
				SinceVersion: 0,
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
					Id:      "1",
					Version: 0,
					Data:    []byte("version1"),
				},
			},
			reply: &proto.SetRecordReply{
				Status:     proto.SetRecordStatus_SUCCESS,
				NewVersion: 1,
			},
		},

		// update record
		{
			name: "initial record update",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:      "1",
					Version: 1,
					Data:    []byte("version2"),
				},
			},
			reply: &proto.SetRecordReply{
				Status:     proto.SetRecordStatus_SUCCESS,
				NewVersion: 2,
			},
		},

		// test conflict
		{
			name: "test conflict",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:      "1",
					Version: 1,
					Data:    []byte("version3"),
				},
			},
			reply: &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			},
		},

		// no changes since version 5
		{
			name: "empty changes",
			request: &proto.ListChangesRequest{
				SinceVersion: 5,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{},
			},
		},

		// 1 record changes
		{
			name: "list changes returns 1 record",
			request: &proto.ListChangesRequest{
				SinceVersion: 0,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{
					{
						Id:      "1",
						Version: 2,
						Data:    []byte("version2"),
					},
				},
			},
		},
	}
}

func testSetRecord(t *testing.T, privateKey *btcec.PrivateKey, client proto.SyncerClient, request *proto.SetRecordRequest, test testCase) {
	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v-%v-%x-%v", request.Record.Id, request.Record.Version, request.Record.Data, requestTime)
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
	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v-%v", request.SinceVersion, requestTime)
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
