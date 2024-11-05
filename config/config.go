package config

import (
	"github.com/Netflix/go-env"
)

type Config struct {
	GrpcListenAddress string `env:"GRPC_LISTEN_ADDRESS"`
	SQLiteDirPath     string `env:"SQLITE_DIR_PATH"`
	PgDatabaseUrl     string `env:"DATABASE_URL"`
}

func NewConfig() (*Config, error) {
	var config Config
	if _, err := env.UnmarshalFromEnviron(&config); err != nil {
		return nil, err
	}

	return &config, nil
}
