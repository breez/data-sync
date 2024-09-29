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
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	os.Create(config.UsersDatabasesDir)
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "failed to create private key")
	os.RemoveAll(config.UsersDatabasesDir)
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

func testCases() []testCase {
	return []testCase{

		// empty db, no changes.
		{
			name: "empty db, no changes",
			request: &proto.ListChangesRequest{
				FromId: 0,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{},
			},
		},

		// set first record
		{
			name: "first record insert",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:      1,
					Version: 0.1,
					Data:    []byte("{}"),
				},
			},
			reply: &proto.SetRecordReply{
				Status: proto.SetRecordStatus_SUCCESS,
				NewId:  1,
			},
		},

		// set second record
		{
			name: "second record insert",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:      2,
					Version: 0.1,
					Data:    []byte("{}"),
				},
			},
			reply: &proto.SetRecordReply{
				Status: proto.SetRecordStatus_SUCCESS,
				NewId:  2,
			},
		},

		// test conflict
		{
			name: "test conflict",
			request: &proto.SetRecordRequest{
				Record: &proto.Record{
					Id:      1,
					Version: 0.1,
					Data:    []byte("{}"),
				},
			},
			reply: &proto.SetRecordReply{
				Status: proto.SetRecordStatus_CONFLICT,
			},
		},

		// 1 record changes
		{
			name: "list changes returns 1 record",
			request: &proto.ListChangesRequest{
				FromId: 2,
			},
			reply: &proto.ListChangesReply{
				Changes: []*proto.Record{
					{
						Id:      2,
						Version: 0.1,
						Data:    []byte("{}"),
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
	request.RequestTime = uint32(requestTime)
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
	toSign := fmt.Sprintf("%v-%v", request.FromId, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request.RequestTime = uint32(requestTime)
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
	baseServer := CreateServer(config, lis)
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
	}

	client := proto.NewSyncerClient(conn)

	return client, closer
}
