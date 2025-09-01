package config

import (
	"os"
	"strconv"
)

type GatewayConfig struct {
	Port         string
	KafkaBrokers string
	RedisURL     string
	DBURL        string
	MongoURL     string
	UpstreamURLs string
	Environment  string
	LogLevel     string
}

type ConfigServiceConfig struct {
	Port        string
	DBURL       string
	RedisURL    string
	MongoURL    string
	Environment string
	LogLevel    string
}

type AnalysisConfig struct {
	KafkaBrokers          string
	RedisURL              string
	DBURL                 string
	MongoURL              string
	WindowMinutes         int
	AnomalyThresholdSigma int
	AnomalyTTLMinutes     int
	LogLevel              string
}

type TelemetryConfig struct {
	KafkaBrokers string
	MongoURL     string
	LogLevel     string
}

func LoadGatewayConfig() *GatewayConfig {
	return &GatewayConfig{
		Port:         getEnvOrDefault("GATEWAY_PORT", "8080"),
		KafkaBrokers: getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
		RedisURL:     getEnvOrDefault("REDIS_URL", "redis://localhost:6379"),
		DBURL:        getEnvOrDefault("DB_URL", "postgres://aether:aether123@localhost:5432/aether?sslmode=disable"),
		MongoURL:     getEnvOrDefault("MONGO_URL", "mongodb://localhost:27017/aether"),
		UpstreamURLs: getEnvOrDefault("UPSTREAM_URLS", "http://localhost:3001,http://localhost:3002"),
		Environment:  getEnvOrDefault("ENVIRONMENT", "development"),
		LogLevel:     getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func LoadConfigServiceConfig() *ConfigServiceConfig {
	return &ConfigServiceConfig{
		Port:        getEnvOrDefault("CONFIG_PORT", "8081"),
		DBURL:       getEnvOrDefault("DB_URL", "postgres://aether:aether123@localhost:5432/aether?sslmode=disable"),
		RedisURL:    getEnvOrDefault("REDIS_URL", "redis://localhost:6379"),
		MongoURL:    getEnvOrDefault("MONGO_URL", "mongodb://localhost:27017/aether"),
		Environment: getEnvOrDefault("ENVIRONMENT", "development"),
		LogLevel:    getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func LoadAnalysisConfig() *AnalysisConfig {
	windowMinutes := 60
	if w := os.Getenv("ANALYSIS_WINDOW_MINUTES"); w != "" {
		if parsed, err := strconv.Atoi(w); err == nil {
			windowMinutes = parsed
		}
	}

	thresholdSigma := 3
	if t := os.Getenv("ANOMALY_THRESHOLD_SIGMA"); t != "" {
		if parsed, err := strconv.Atoi(t); err == nil {
			thresholdSigma = parsed
		}
	}

	ttlMinutes := 5
	if ttl := os.Getenv("ANOMALY_TTL_MINUTES"); ttl != "" {
		if parsed, err := strconv.Atoi(ttl); err == nil {
			ttlMinutes = parsed
		}
	}

	return &AnalysisConfig{
		KafkaBrokers:          getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
		RedisURL:              getEnvOrDefault("REDIS_URL", "redis://localhost:6379"),
		DBURL:                 getEnvOrDefault("DB_URL", "postgres://aether:aether123@localhost:5432/aether?sslmode=disable"),
		MongoURL:              getEnvOrDefault("MONGO_URL", "mongodb://localhost:27017/aether"),
		WindowMinutes:         windowMinutes,
		AnomalyThresholdSigma: thresholdSigma,
		AnomalyTTLMinutes:     ttlMinutes,
		LogLevel:              getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func LoadTelemetryConfig() *TelemetryConfig {
	return &TelemetryConfig{
		KafkaBrokers: getEnvOrDefault("KAFKA_BROKERS", "localhost:9092"),
		MongoURL:     getEnvOrDefault("MONGO_URL", "mongodb://localhost:27017/aether"),
		LogLevel:     getEnvOrDefault("LOG_LEVEL", "info"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
