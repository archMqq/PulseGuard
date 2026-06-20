package config

type Config struct {
	RedisConfig
}

type RedisConfig struct {
	Addr string
	Pass string
	DB int
	PoolSize int
}