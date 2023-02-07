package main

import (
	"fmt"
	kafka2 "github.com/Fuzus/imersaofsfc2-simulator/application/kafka"
	ckafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"log"

	"github.com/Fuzus/imersaofsfc2-simulator/infra/kafka"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	msgChain := make(chan *ckafka.Message)
	consumer := kafka.NewKafkaConsumer(msgChain)
	go consumer.Consume() //go cria uma nova thread
	for msg := range msgChain {
		fmt.Println(string(msg.Value))
		go kafka2.Produce(msg)
	}
}
