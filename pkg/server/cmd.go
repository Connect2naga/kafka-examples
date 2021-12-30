package server

import (
	"fmt"
	"kafak-examples/pkg/config"
	"kafak-examples/pkg/consumer"
	"kafak-examples/pkg/producer"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
)

func StartService() {
	logger.Printf("Starting service..")

	conf, err := config.GetEnvironmentConfigurations()
	if err != nil {
		logger.Panicf("Environment variables reading error, %v ", err)
		os.Exit(1)
	}

	logger.Printf("start the producer, it producer 50 messages .....")
	logger.Printf("start the consumer, will read all the messages....")

	p, err := producer.NewProducer(conf.Brokers, logger)
	if err != nil {
		logger.Panicf("enable to start producer, %v ", err)
		os.Exit(1)
	}
	go func() {
		for i := 0; i <= 100; i++ {
			p.SendMessage(conf.Topic, fmt.Sprintf("message %s : %d", time.Now().Format(time.RFC3339),i), nil)
			time.Sleep(1 * time.Second)
		}
	}()

	kw, err := consumer.NewKafkaConsumer(conf.Brokers, []string{conf.Topic}, conf.GroupID, logger)
	if err != nil {
		logger.Panicf("Event handler exited with error, %v", err)
	}
	kw.StartProcessing()
	logger.Printf(" service is running")

	// Wait SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals

	p.Close()
	kw.Close()
	logger.Printf("service is stopped")
}
