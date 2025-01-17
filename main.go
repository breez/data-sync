package main

import (
	"log"
	"net"
	"time"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	config, err := config.NewConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	grpcListener, err := net.Listen("tcp", config.GrpcListenAddress)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	quitChan := make(chan struct{})
	syncServer, err := NewPersistentSyncerServer(config)
	if err != nil {
		log.Fatalf("failed to create sync server: %v", err)
	}
	syncServer.Start(quitChan)
	s := CreateServer(config, grpcListener, syncServer)
	log.Printf("Server listening at %s", config.GrpcListenAddress)
	if err := s.Serve(grpcListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func CreateServer(config *config.Config, listener net.Listener, syncServer proto.SyncerServer) *grpc.Server {
	s := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Second * 5,
			PermitWithoutStream: true,
		}),
	)
	proto.RegisterSyncerServer(s, syncServer)
	return s
}
