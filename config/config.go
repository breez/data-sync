package config

import (
	"crypto/x509"
	"encoding/pem"
	"log"
	"os"

	"github.com/Netflix/go-env"
)

type Config struct {
	GrpcListenAddress string `env:"GRPC_LISTEN_ADDRESS,default=0.0.0.0:8080"`
	SQLiteDirPath     string `env:"SQLITE_DIR_PATH,default=db"`
	PgDatabaseUrl     string `env:"DATABASE_URL"`
	CACert            *x509.Certificate
}

func initializeCACert() *x509.Certificate {
	certPath := os.Getenv("CA_CERT_PATH")
	if certPath == "" {
		log.Fatal("CA certificate path not specified")
	}

	CACertBlock, _ := pem.Decode([]byte(certPath))
	if CACertBlock == nil {
		log.Fatal("CA certificate is invalid")
	}

	CACert, err := x509.ParseCertificate(CACertBlock.Bytes)
	if err != nil {
		log.Fatal("Could not parse CA cert:", err)
	}

	return CACert
}

func NewConfig() (*Config, error) {
	var config Config
	if _, err := env.UnmarshalFromEnviron(&config); err != nil {
		return nil, err
	}
	config.CACert = initializeCACert()

	return &config, nil
}
