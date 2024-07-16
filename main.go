package main

import (
	"log"
	"net"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/middleware"
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

	s := CreateServer(config, grpcListener)
	if err := s.Serve(grpcListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func CreateServer(config *config.Config, listener net.Listener) *grpc.Server {
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(middleware.UnaryAuth(config)))
	proto.RegisterSyncerServer(s, &PersistentSyncerServer{
		config: config,
	})
	return s
}
