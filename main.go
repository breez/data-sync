package main

import (
	"log"
	"net"
	"net/http"
	"time"

	"github.com/breez/data-sync/config"
	"github.com/breez/data-sync/proto"
	"github.com/improbable-eng/grpc-web/go/grpcweb"
	"github.com/rs/cors"
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
	RunWebProxy(s, config)
	log.Printf("Server listening at %s", config.GrpcListenAddress)
	if err := s.Serve(grpcListener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func RunWebProxy(grpcServer *grpc.Server, config *config.Config) {
	wrappedGrpc := grpcweb.WrapServer(grpcServer,
		grpcweb.WithOriginFunc(func(origin string) bool {
			// Allow all origins for development, restrict as needed for production
			return true
		}),
	)

	// Create a multiplexer to handle both HTTP and gRPC-Web requests
	httpMux := http.NewServeMux()

	// Define the handler that will check if a request is gRPC-Web or standard HTTP
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if this is a gRPC-Web request
		if wrappedGrpc.IsGrpcWebRequest(r) || wrappedGrpc.IsAcceptableGrpcCorsRequest(r) {
			wrappedGrpc.ServeHTTP(w, r)
			return
		}

		// Otherwise, handle as a standard HTTP request
		httpMux.ServeHTTP(w, r)
	})

	// Add CORS headers for browsers
	corsHandler := cors.Default().Handler(handler)

	// Set up HTTP routes
	httpMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("gRPC-Web service is running!"))
	})

	// Start server
	log.Printf("Starting server on %s", config.GrpcWebListenAddress)
	go func() {
		if err := http.ListenAndServe(config.GrpcWebListenAddress, corsHandler); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()
}

func CreateServer(config *config.Config, listener net.Listener, syncServer proto.SyncerServer) *grpc.Server {
	s := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Second * 5,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Second * 10,
			Timeout: time.Second * 5,
		}),
	)
	proto.RegisterSyncerServer(s, syncServer)
	return s
}
