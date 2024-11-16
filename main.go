package main

import (
	"encoding/json"
	"fmt"
	"kafkago/consumer"
	"kafkago/producer"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Response struct {
	Message   string              `json:"message"`
	Timestamp kafka.TimestampType `json:"timestamp"`
}

func main() {
	topic := "MyFirstTopic"
	p := producer.New()
	defer p.Close()
	c := consumer.New(topic)
	run := true

	go func() {
		for run {
			msg, _ := json.Marshal(&Response{
				Message:   "OK",
				Timestamp: kafka.TimestampType(time.Now().UnixMilli()),
			})

			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          msg,
			}, nil)

			time.Sleep(10 * time.Second)
		}

		p.Flush(15 * 1000)
	}()

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
	c.Close()

}
