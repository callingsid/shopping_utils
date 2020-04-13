package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/callingsid/shopping_utils/logger"
	"github.com/callingsid/shopping_utils/rest_errors"
)

var (
	PClient kafkaProducerInterface = &kafkaProducerClient{}
)

type kafkaProducerInterface interface {
	setClient(consumer *sarama.SyncProducer)
	Publish(string,  interface{}) rest_errors.RestErr

}

type kafkaProducerClient struct {
	producer *sarama.SyncProducer
}

func init() {
	brokers := []string{"kafka:9092"}
	//brokers := []string{"localhost:9092"}
	// Create new consumer
	master, err := sarama.NewSyncProducer(brokers, newKafkaConfiguration2())
	if err != nil {
		panic(err)
	}
	/*
		defer func() {
			if err := master.Close(); err != nil {
				panic(err)
			}
		}()
	*/
	PClient.setClient(&master)

}

func (k *kafkaProducerClient) setClient(producer *sarama.SyncProducer)  {
	k.producer = producer
}

func (k *kafkaProducerClient) Publish(topic string,  event interface{}) rest_errors.RestErr {

	var master sarama.SyncProducer
	master = *k.producer
	jsonData, err := json.Marshal(event)
	if err != nil {
		return rest_errors.NewInternalServerError("error when trying to marshal json", errors.New("queue error"))
	}

	msgLog := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(string(jsonData)),
	}
	partition, offset, err := master.SendMessage(msgLog)
	if err != nil {
		return rest_errors.NewInternalServerError("error when trying to publish json in the queue", errors.New("queue error"))
	}
	logger.Info(fmt.Sprintf("Message is stored in partition %d, offset %d\n", partition, offset))

	return nil

}
