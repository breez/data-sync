package config

import (
	"crypto/x509"
	"encoding/pem"
	"log"
	"os"

	"github.com/Netflix/go-env"
)

type Config struct {
	GrpcListenAddress string  `env:"GRPC_LISTEN_ADDRESS,default=0.0.0.0:8080"`
	SQLiteDirPath     string  `env:"SQLITE_DIR_PATH,default=db"`
	PgDatabaseUrl     string  `env:"DATABASE_URL"`
	CACertPath        *string `env:"CA_CERT_PATH"`
	CACert            *x509.Certificate
}

func initializeCACert(certPath string) *x509.Certificate {
	certData, err := os.ReadFile(certPath)
	if err != nil {
		log.Fatal("CA certificate not found")
	}

	CACertBlock, _ := pem.Decode(certData)
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
	if config.CACertPath != nil {
		config.CACert = initializeCACert(*config.CACertPath)
	}

	return &config, nil
}
