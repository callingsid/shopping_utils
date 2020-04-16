package queue

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/callingsid/shopping_utils/logger"
	"strings"
)

var (
	Client kafkaConsumerInterface = &kafkaConsumerClient{}
)

type kafkaConsumerInterface interface {
	setClient(consumer *sarama.Consumer)
	StartConsumer([] string) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError)
}

type kafkaConsumerClient struct {
	consumer *sarama.Consumer
}

func newKafkaConfiguration2() *sarama.Config {
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.ChannelBufferSize = 1
	conf.Version = sarama.V0_10_1_0
	return conf
}

func init() {
	brokers := []string{"kafka:9092"}
	//brokers := []string{"localhost:9092"}
	// Create new consumer
	master, err := sarama.NewConsumer(brokers, newKafkaConfiguration2())
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

	Client.setClient(&master)

}

func (k *kafkaConsumerClient) setClient(consumer *sarama.Consumer)  {
	k.consumer = consumer
}

func (k *kafkaConsumerClient) StartConsumer(topics []string) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumer, errors := k.consume(topics)
	return consumer, errors
}

func (k *kafkaConsumerClient) consume(topics []string) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError)
	var master sarama.Consumer
	master = *k.consumer
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := master.Partitions(topic)
		// this only consumes partition no 1, you would probably want to consume all partitions
		consumer, err := master.ConsumePartition(topic, partitions[0], sarama.OffsetNewest)
		if nil != err {
			fmt.Printf("Topic %v Partitions: %v", topic, partitions)
			panic(err)
		}
		fmt.Println(" Start consuming topic:  & partition : ", topic, partitions[0])
		go func(topic string, consumer sarama.PartitionConsumer) {
			for {
				select {
				case consumerError := <-consumer.Errors():
					fmt.Println(" Inside consumer case consumer error :  ", consumer.Errors())
					errors <- consumerError
					fmt.Println("consumerError: ", consumerError.Err)

				case msg := <-consumer.Messages():
					fmt.Println("Inside case msg")
					consumers <- msg
					logger.Info(fmt.Sprintf("Got message on topic %s, data %s in utils kafka", topic, msg.Value))
				}
			}
		}(topic, consumer)
	}
	return consumers, errors

}
