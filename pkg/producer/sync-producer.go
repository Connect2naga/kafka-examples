package producer

import (
	"log"
	"os"

	"github.com/Shopify/sarama"
)

// Producer is emitting operation feedback events to operation.feedback topic
type kakfaProducer struct {
	syncProducer sarama.SyncProducer
	logger       *log.Logger
}

// NewProducer instantiates new Producer instance using built-in kafka client
func NewProducer(brokers []string, logger *log.Logger) (*kakfaProducer, error) {
	config := newSaramaConfig(logger)
	syncProducer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &kakfaProducer{syncProducer: syncProducer, logger: logger}, nil
}

// NewSaramaConfig creates sarama (kafka client) config out of standard input
func newSaramaConfig(logger *log.Logger) *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	config.Producer.Return.Successes = true
	config.ClientID, _ = os.Hostname()
	sarama.Logger = logger
	return config
}

// SendMessage emits an event with provided data
func (p *kakfaProducer) SendMessage(topic string, msgData string, headers map[string]string) error {
	p.logger.Printf("Sending test message to %s, %s", topic, msgData)

	if _, _, err := p.syncProducer.SendMessage(&sarama.ProducerMessage{
		Value:   sarama.ByteEncoder(msgData),
		Topic:   topic,
		Headers: convertMapToHeaders(headers),
	}); err != nil {
		return err
	}

	return nil
}

func (p *kakfaProducer) Close() error {
	return p.syncProducer.Close()
}

func convertMapToHeaders(mapHeaders map[string]string) []sarama.RecordHeader {
	recordHeaders := []sarama.RecordHeader{}
	for key, val := range mapHeaders {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(key), Value: []byte(val)})
	}
	return recordHeaders
}
