# Belajar Kafka dengan Go

Prerequisites:

- `go` >= `1.21.5`

Initialize project

```sh
go mod init kafkago
go get github.com/confluentinc/confluent-kafka-go/kafka
```

Create producer

`producer/producer.go`

```go
package producer

import (
 "fmt"
 "os"

 "github.com/confluentinc/confluent-kafka-go/kafka"
)

func New() *kafka.Producer {
 p, err := kafka.NewProducer(&kafka.ConfigMap{
  "bootstrap.servers": "localhost:9092",
 })

 if err != nil {
  fmt.Printf("Failed to create producer: %s", err)
  os.Exit(1)
 }

 return p
}

```

Create consumer

`consumer/consumer.go`

```go
package consumer

import (
 "fmt"
 "os"

 "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func New(topic string) *kafka.Consumer {
 c, err := kafka.NewConsumer(&kafka.ConfigMap{
  "bootstrap.servers": "localhost:9092",
  "group.id":          "test",
 })

 if err != nil {
  fmt.Printf("Failed to create consumer: %s", err)
  os.Exit(1)
 }

 err = c.Subscribe(topic, nil)

 if err != nil {
  fmt.Printf("Failed to subscribe to topic: %s", err)
  os.Exit(1)
 }

 return c
}

```

Check if producer and consumer is working

`main.go`

```go
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

```
