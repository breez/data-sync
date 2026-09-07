package config

import (
	"crypto/x509"
	"encoding/base64"
	"log"

	"github.com/Netflix/go-env"
)

type Certificate struct {
	Raw *x509.Certificate
}

func (c *Certificate) UnmarshalEnvironmentValue(data string) error {
	CACertBlock, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		log.Fatal("Could not decode certificate base64 body: ", err)
	}

	CACert, err := x509.ParseCertificate(CACertBlock)
	if err != nil {
		log.Fatal("Could not parse CA cert: ", err)
	}

	c.Raw = CACert

	return nil
}

type Config struct {
	GrpcListenAddress    string       `env:"GRPC_LISTEN_ADDRESS,default=0.0.0.0:8080"`
	GrpcWebListenAddress string       `env:"GRPC_WEB_LISTEN_ADDRESS,default=0.0.0.0:8081"`
	SQLiteDirPath        string       `env:"SQLITE_DIR_PATH,default=db"`
	PgRuntimeUrl         string       `env:"DATABASE_RUNTIME_URL"`
	PgMigrationUrl       string       `env:"DATABASE_MIGRATION_URL"`
	PgMaxConns           int32        `env:"DATABASE_MAX_CONNS"`
	CACert               *Certificate `env:"CA_CERT"`
	CRLUrl               string       `env:"CRL_URL"`
}

func NewConfig() (*Config, error) {
	var config Config
	if _, err := env.UnmarshalFromEnviron(&config); err != nil {
		return nil, err
	}
	return &config, nil
}
