package main

import (
	"context"
	"gseph/ocd-fizzbuzz/pkg/datamodel"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true
	config.Consumer.Group.InstanceId = datamodel.GroupIdCyclic

	group, err := sarama.NewConsumerGroup([]string{datamodel.BootstrapServer}, datamodel.GroupIdCyclic, config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			log.Printf("prodcons::main group errors tracking - groupId: %v - error: %v\n", datamodel.GroupIdCyclic, err)
		}
	}()

	producer, err := sarama.NewSyncProducer([]string{datamodel.BootstrapServer}, nil) // config instead of nil
	if err != nil {
		log.Fatalf("prodcons::main - producer error %v\n", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("prodcons::main producer closing error: %v\n", err)
		}
	}()
	ctx := context.Background()
	topics := []string{datamodel.Topic}
	handler := datamodel.CyclicConsumerGroupHandler{Producer: &producer}
	for {
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			log.Panicf("prodcons::main group consuming error - groupId: %v - error: %v\n", datamodel.GroupIdCyclic, err)
		}
	}
}
