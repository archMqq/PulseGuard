package config

type Config struct {
	kafka KafkaConfig
}

type KafkaConfig struct {
	Brokers      []string
	GroupId      string
	Topic        string
	MaxBytes     int
	batchTimeout int
}

type RedisConfig struct {
	Addr     string
	Pass     string
	DB       int
	PoolSize int
}
