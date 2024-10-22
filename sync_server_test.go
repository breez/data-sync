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
	privateKey, err := btcec.NewPrivateKey()
	require.NoError(t, err, "failed to create private key")
	os.RemoveAll(config.UsersDatabasesDir)

	buffer := 101024 * 1024
	lis := bufconn.Listen(buffer)
	closer := newServer(lis, config)

	client1 := newClient(context.Background(), lis)
	client2 := newClient(context.Background(), lis)

	changes_stream1 := listenChanges(t, privateKey, client1)

	changes_stream2 := listenChanges(t, privateKey, client2)

	defer closer()
	defer func() {
		os.RemoveAll(config.UsersDatabasesDir)
	}()

	for _, testCase := range testCases() {
		if setRecordRequest, ok := testCase.request.(*proto.SetRecordRequest); ok {
			testSetRecord(t, privateKey, client1, setRecordRequest, testCase)

			if testCase.reply.(*proto.SetRecordReply).Status != proto.SetRecordStatus_SUCCESS {
				continue
			}

			// Test that the expected value matches the one received from the stream
			record1, err := changes_stream1.Recv()
			require.NoError(t, err, "failed to receive changes")

			received_json, err := json.Marshal(record1)
			require.NoError(t, err, "failed to serialize received record")

			expected_json, err := json.Marshal(testCase.request.(*proto.SetRecordRequest).Record)
			require.NoError(t, err, "failed to serialize expected record")

			require.Equal(t, received_json, expected_json)

			// Test that the second client also received a valid value
			record2, err := changes_stream2.Recv()
			require.NoError(t, err, "failed to receive changes")

			received_json, err = json.Marshal(record2)
			require.NoError(t, err, "failed to serialize received record")

			require.Equal(t, received_json, expected_json)
		}
		if listChangesRequest, ok := testCase.request.(*proto.ListChangesRequest); ok {
			testListChanges(t, privateKey, client1, listChangesRequest, testCase)
		}
	}
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
					Data:     []byte("{}"),
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
					Data:     []byte("{}"),
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
					Data:     []byte("{}"),
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
						Data:     []byte("{}"),
					},
				},
			},
		},
	}
}

func listenChanges(t *testing.T, privateKey *btcec.PrivateKey, client proto.SyncerClient) grpc.ServerStreamingClient[proto.Record] {
	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v", requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request := proto.ListenChangesRequest{
		RequestTime: uint32(requestTime),
		Signature:   signature,
	}
	stream, err := client.ListenChanges(context.Background(), &request)
	require.NoError(t, err, "failed to call ListenChanges")
	return stream
}

func testSetRecord(t *testing.T, privateKey *btcec.PrivateKey, client proto.SyncerClient, request *proto.SetRecordRequest, test testCase) {
	requestTime := time.Now().Unix()
	toSign := fmt.Sprintf("%v-%v-%x-%v", request.Record.Id, request.Record.Revision, request.Record.Data, requestTime)
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

func newClient(ctx context.Context, lis *bufconn.Listener) proto.SyncerClient {
	conn, err := grpc.DialContext(ctx, "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	client := proto.NewSyncerClient(conn)
	return client
}

func newServer(lis *bufconn.Listener, config *config.Config) func() {
	baseServer := CreateServer(config, lis)
	go func() {
		if err := baseServer.Serve(lis); err != nil {
			log.Printf("error serving server: %v", err)
		}
	}()

	closer := func() {
		err := lis.Close()
		if err != nil {
			log.Printf("error closing listener: %v", err)
		}
		baseServer.Stop()
	}

	return closer
}
