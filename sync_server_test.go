package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
	"github.com/breez/data-sync/proto"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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
		os.RemoveAll(config.SQLiteDirPath)
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
		os.RemoveAll(config.SQLiteDirPath)
	}()

	requestTime := uint32(time.Now().Unix())
	toSign := fmt.Sprintf("%v", requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err, "failed to sign message")
	request := &proto.ListenChangesRequest{
		RequestTime: requestTime,
		Signature:   signature,
	}
	request.RequestTime = requestTime
	request.Signature = signature
	stream, err := client.ListenChanges(context.Background(), request)
	require.NoError(t, err, "failed to call TrackChanges")

	// check realtime changes
	client.SetRecord(context.Background(), createSetRecordRequest(t, privateKey, &proto.Record{
		Id:       "1",
		Revision: 0,
		Data:     []byte("revision1"),
	}))
	notification, err := stream.Recv()
	require.NoError(t, err, "failed to receive notification")
	res, err := json.Marshal(notification)
	require.NoError(t, err, "failed to deserialize notification")
	require.Equal(t, res, []byte("{}"))
}

func createSetRecordRequest(t *testing.T, privateKey *btcec.PrivateKey, record *proto.Record) *proto.SetRecordRequest {
	requestTime := uint32(time.Now().Unix())
	toSign := middleware.SignSetRecord(record, requestTime)
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
	os.RemoveAll(config.SQLiteDirPath)
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
	toSign := middleware.SignSetRecord(request.Record, requestTime)
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
	syncServer, err := NewPersistentSyncerServer(config)
	if err != nil {
		log.Fatalf("failed to create sync server: %v", err)
	}
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

func signSetLock(t *testing.T, privateKey *btcec.PrivateKey, lockName, instanceID string, acquire bool, requestTime uint32) string {
	toSign := fmt.Sprintf("%v-%v-%v-%v", lockName, instanceID, acquire, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err)
	return signature
}

func signGetLock(t *testing.T, privateKey *btcec.PrivateKey, lockName string, requestTime uint32) string {
	toSign := fmt.Sprintf("%v-%v", lockName, requestTime)
	signature, err := middleware.SignMessage(privateKey, []byte(toSign))
	require.NoError(t, err)
	return signature
}

func TestSetLock_EmptyLockName(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	requestTime := uint32(time.Now().Unix())
	instanceID := uuid.New().String()
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    "",
		InstanceId:  instanceID,
		Acquire:     true,
		RequestTime: requestTime,
		Signature:   signSetLock(t, privateKey, "", instanceID, true, requestTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "lock_name must not be empty")
}

func TestSetLock_LockNameTooLong(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	longName := strings.Repeat("a", 257)
	requestTime := uint32(time.Now().Unix())
	instanceID := uuid.New().String()
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    longName,
		InstanceId:  instanceID,
		Acquire:     true,
		RequestTime: requestTime,
		Signature:   signSetLock(t, privateKey, longName, instanceID, true, requestTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "exceeds maximum length")
}

func TestSetLock_InvalidInstanceID(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	requestTime := uint32(time.Now().Unix())
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    "test_lock",
		InstanceId:  "not-a-uuid",
		Acquire:     true,
		RequestTime: requestTime,
		Signature:   signSetLock(t, privateKey, "test_lock", "not-a-uuid", true, requestTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "valid UUID")
}

func TestSetLock_StaleRequestTime(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	staleTime := uint32(time.Now().Add(-10 * time.Minute).Unix())
	instanceID := uuid.New().String()
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    "test_lock",
		InstanceId:  instanceID,
		Acquire:     true,
		RequestTime: staleTime,
		Signature:   signSetLock(t, privateKey, "test_lock", instanceID, true, staleTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "request time too far")
}

func TestSetLock_TTLCapped(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	// Acquire with a huge TTL — should succeed (silently capped, not rejected)
	requestTime := uint32(time.Now().Unix())
	instanceID := uuid.New().String()
	hugeTTL := uint32(999999)
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    "test_lock",
		InstanceId:  instanceID,
		Acquire:     true,
		TtlSeconds:  &hugeTTL,
		RequestTime: requestTime,
		Signature:   signSetLock(t, privateKey, "test_lock", instanceID, true, requestTime),
	})
	require.NoError(t, err)

	// Verify the lock is held
	getLockTime := uint32(time.Now().Unix())
	reply, err := client.GetLock(context.Background(), &proto.GetLockRequest{
		LockName:    "test_lock",
		RequestTime: getLockTime,
		Signature:   signGetLock(t, privateKey, "test_lock", getLockTime),
	})
	require.NoError(t, err)
	require.True(t, reply.Locked)
}

func TestSetLock_TTLExpiration(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	// Acquire with a 1s TTL
	instanceID := uuid.New().String()
	requestTime := uint32(time.Now().Unix())
	smallTTL := uint32(1)
	_, err := client.SetLock(context.Background(), &proto.SetLockRequest{
		LockName:    "test_lock",
		InstanceId:  instanceID,
		Acquire:     true,
		TtlSeconds:  &smallTTL,
		RequestTime: requestTime,
		Signature:   signSetLock(t, privateKey, "test_lock", instanceID, true, requestTime),
	})
	require.NoError(t, err)

	// Should be held immediately
	getLockTime := uint32(time.Now().Unix())
	reply, err := client.GetLock(context.Background(), &proto.GetLockRequest{
		LockName:    "test_lock",
		RequestTime: getLockTime,
		Signature:   signGetLock(t, privateKey, "test_lock", getLockTime),
	})
	require.NoError(t, err)
	require.True(t, reply.Locked)

	// Wait for expiry
	time.Sleep(2 * time.Second)

	// Should have expired
	getLockTime2 := uint32(time.Now().Unix())
	reply, err = client.GetLock(context.Background(), &proto.GetLockRequest{
		LockName:    "test_lock",
		RequestTime: getLockTime2,
		Signature:   signGetLock(t, privateKey, "test_lock", getLockTime2),
	})
	require.NoError(t, err)
	require.False(t, reply.Locked, "lock should have expired after TTL")
}

func TestGetLock_EmptyLockName(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	requestTime := uint32(time.Now().Unix())
	_, err := client.GetLock(context.Background(), &proto.GetLockRequest{
		LockName:    "",
		RequestTime: requestTime,
		Signature:   signGetLock(t, privateKey, "", requestTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestGetLock_StaleRequestTime(t *testing.T) {
	cfg, privateKey := testParameters(t)
	client, closer := server(context.Background(), cfg)
	defer closer()
	defer os.RemoveAll(cfg.SQLiteDirPath)

	staleTime := uint32(time.Now().Add(-10 * time.Minute).Unix())
	_, err := client.GetLock(context.Background(), &proto.GetLockRequest{
		LockName:    "test_lock",
		RequestTime: staleTime,
		Signature:   signGetLock(t, privateKey, "test_lock", staleTime),
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
