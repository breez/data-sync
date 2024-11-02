package main

import (
	"log"
	"net"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/proto"
	"google.golang.org/grpc"
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
	syncServer := NewPersistentSyncerServer(config)
	syncServer.Start(quitChan)
	s := CreateServer(config, grpcListener, syncServer)
	log.Printf("Server listening at %s", config.GrpcListenAddress)
	if err := s.Serve(grpcListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func CreateServer(config *config.Config, listener net.Listener, syncServer proto.SyncerServer) *grpc.Server {
	s := grpc.NewServer()
	proto.RegisterSyncerServer(s, syncServer)
	return s
}
