package main

import (
	"log"
	"net"
	"sync"
	"time"

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

	log.Printf("Listening at address %v", config.GrpcListenAddress)

	s := CreateServer(config, grpcListener)
	if err := s.Serve(grpcListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func cleanupUnusedListeners(srv *PersistentSyncerServer) {
	for {
		for pubkey, user := range srv.users {
			for _, listener := range user.listeners {
				if err := listener.Context().Err(); err != nil {
					removeListener(user, listener)
					if len(user.listeners) == 0 {
						removeUser(srv, pubkey)
					}
				}
			}
		}
		time.Sleep(30 * time.Second)
	}
}

func CreateServer(config *config.Config, listener net.Listener) *grpc.Server {
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(middleware.UnaryAuth(config)))
	srv := &PersistentSyncerServer{
		config: config,
		users:  make(map[string](*User)),
		mutex:  sync.RWMutex{},
	}
	go cleanupUnusedListeners(srv)
	proto.RegisterSyncerServer(s, srv)
	return s
}
