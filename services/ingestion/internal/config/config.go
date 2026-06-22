package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	Addr  string      `mapstructure:"addr"`
	Kafka KafkaConfig `mapstructure:"kafka"`
	Redis RedisConfig `mapstructure:"redis"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Pass     string `mapstructure:"pass"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`
}

type KafkaConfig struct {
	Addr  []string `mapstructure:"addr"`
	Topic string   `mapstructure:"topic"`
}

func ReadConfig(path string, typ string) (*Config, error) {
	viper.SetConfigName("config")
	viper.AddConfigPath(path)
	viper.AutomaticEnv()
	viper.SetConfigType(typ)

	var cfg Config

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("config read error: %w", err)
	}

	err := viper.Unmarshal(&cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to decode to config struct, error: %w", err)
	}

	return &cfg, nil
}
