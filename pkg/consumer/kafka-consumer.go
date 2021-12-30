package consumer

import (
	"sync"

	"log"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

type KafkaConsumer struct {
	consumer *cluster.Consumer
	logger   *log.Logger
	messages []string
	m        sync.Mutex
}

func NewKafkaConsumer(brokers []string, topics []string, grpID string, logger *log.Logger) (*KafkaConsumer, error) {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Version = sarama.V1_0_0_0
	config.Consumer.Offsets.CommitInterval = config.Consumer.Offsets.AutoCommit.Interval
	consumer, err := cluster.NewConsumer(brokers, grpID, topics, config)
	if err != nil {
		return nil, err
	}

	//consumer.MarkPartitionOffset(topics[0],0,1, "")
	return &KafkaConsumer{
		consumer: consumer,
		logger:   logger,
		messages: []string{},
		m:        sync.Mutex{},
	}, nil
}

func (kw *KafkaConsumer) Close() {
	if err := kw.consumer.Close(); err != nil {
		kw.logger.Printf("Error closing kafka consumer: ", err)
	}
	log.Printf("kafka consumer closed")
}

func (kw *KafkaConsumer) process() {
	for {
		select {
		case notification, ok := <-kw.consumer.Notifications():
			if ok {
				kw.logger.Printf("Notification:", notification.Type)
			}
		case err, ok := <-kw.consumer.Errors():
			if ok {
				kw.logger.Printf("Kafka consumer error:", err)
			}
		case msg, ok := <-kw.consumer.Messages():
			if ok {
				kw.logger.Printf("offset:%v Partition : %+v",msg.Offset,msg.Partition)
				kw.addMessageForSchedule(msg)
			}
		}
	}
}

func (kw *KafkaConsumer) addMessageForSchedule(msg *sarama.ConsumerMessage) {
	kw.m.Lock()
	defer kw.m.Unlock()
	kw.logger.Printf("Message schedule : %s", string(msg.Value))
	kw.messages = append(kw.messages, string(msg.Value))

}

//Listen starts the consumer process
func (m *KafkaConsumer) StartProcessing() {
	go m.process()
}

func (kw *KafkaConsumer) getScheduleMsg() (msg string) {
	kw.m.Lock()
	defer kw.m.Unlock()
	if len(kw.messages) == 0 {
		return
	}
	msg, kw.messages = kw.messages[0], kw.messages[1:]
	return
}
