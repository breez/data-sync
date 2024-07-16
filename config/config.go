package config

import (
	"github.com/Netflix/go-env"
)

type Config struct {
	GrpcListenAddress string `env:"GRPC_LISTEN_ADDRESS"`
	UsersDatabasesDir string `env:"USERS_DATABASES_DIR"`
}

func NewConfig() (*Config, error) {
	var config Config
	if _, err := env.UnmarshalFromEnviron(&config); err != nil {
		return nil, err
	}
	if config.UsersDatabasesDir == "" {
		config.UsersDatabasesDir = "databases"
	}

	return &config, nil
}
