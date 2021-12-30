package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

//ServerConfiguration ...
type ServerConfiguration struct {
	Brokers []string `envconfig:"KAFKA_BROKERS" default:"kafka:9092"`
	Topic   string   `envconfig:"TOPICS" default:"test-topic"`
	GroupID string   `envconfig:"GROUP_ID" default:"consumer-example"`
}

func GetEnvironmentConfigurations() (*ServerConfiguration, error) {
	conf := &ServerConfiguration{}
	if err := envconfig.Process("", conf); err != nil {
		return nil, fmt.Errorf("Environment variables reading error, %v ", err)
	}
	return conf, nil
}
