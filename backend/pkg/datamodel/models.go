package datamodel

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type FizzBuzz struct {
	Uuid    string `json:"uuid"`
	Value   int    `json:"value"`
	Target  int    `json:"target"`
	Message string `json:"message"`
}

func (fb *FizzBuzz) EvaluateFizzBuzz() *FizzBuzz {
	str := ""
	fb.Value++
	if fb.Value%3 == 0 {
		str += "Fizz"
	}
	if fb.Value%5 == 0 {
		str += "Buzz"
	}
	fb.Message = str

	return fb
}

type CyclicConsumerGroupHandler struct {
	Producer *sarama.SyncProducer
}

type SSEGroupHandler struct {
	OutChannel   chan FizzBuzz
	DoneChannel  chan bool
	ReadyChannel *chan bool
	Uuid         string
}

func (CyclicConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (CyclicConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h CyclicConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		time.Sleep(1 * time.Second)
		log.Printf("CyclicConsumerGroupHandler::ConsumeClaim - Message topic: %v partition: %v offset: %v value: %v\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		sess.MarkMessage(msg, "")

		desFizzBuzzMsg := FizzBuzz{}
		unmarshal_error := json.Unmarshal(msg.Value, &desFizzBuzzMsg)

		if unmarshal_error != nil {
			log.Printf("CyclicConsumerGroupHandler::ConsumeClaim - Error while unmashaling msg: %v - error: %v\n", string(msg.Value), unmarshal_error)
			return nil
		}

		if desFizzBuzzMsg.Value >= desFizzBuzzMsg.Target {
			log.Printf("CyclicConsumerGroupHandler::ConsumeClaim - FizzBuzz streak %v finished\n", desFizzBuzzMsg.Uuid)
			return nil
		}

		desFizzBuzzMsg = *desFizzBuzzMsg.EvaluateFizzBuzz()

		err := desFizzBuzzMsg.ProduceMessage(h.Producer, msg.Topic)
		if err != nil {
			log.Printf("CyclicConsumerGroupHandler::ConsumeClaim - Error producing message %v - error: %v\n", desFizzBuzzMsg, err)
			return err
		}
	}
	return nil
}

func (SSEGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (SSEGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h SSEGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	readyMsgSent := false
	for msg := range claim.Messages() {
		if !readyMsgSent {
			log.Printf("SSEGroupHandler::ConsumeClaim - is ready!\n")
			readyMsgSent = true
			*h.ReadyChannel <- true
		}

		log.Printf("SSEGroupHandler::ConsumeClaim - Message topic: %v partition: %v offset: %v value: %v\n", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		desFizzBuzzMsg := FizzBuzz{}
		unmarshal_error := json.Unmarshal(msg.Value, &desFizzBuzzMsg)

		if unmarshal_error != nil {
			log.Printf("SSEGroupHandler::ConsumeClaim - Error while unmashaling msg: %v - error: %v\n", string(msg.Value), unmarshal_error)
			return nil
		}

		if desFizzBuzzMsg.Uuid == h.Uuid && desFizzBuzzMsg.Value > 0 {
			log.Printf("SSEGroupHandler::ConsumeClaim - Got a fizzbuzz message to send out - msg: %v\n", desFizzBuzzMsg.Uuid)
			h.OutChannel <- desFizzBuzzMsg
		}

		if desFizzBuzzMsg.Uuid == h.Uuid && desFizzBuzzMsg.Value == desFizzBuzzMsg.Target {
			log.Printf("SSEGroupHandler::ConsumeClaim - FizzBuzz streak %v finished consuming\n", desFizzBuzzMsg.Uuid)
			h.DoneChannel <- true
			close(h.OutChannel)
			close(*h.ReadyChannel)
			return nil
		}

		h.DoneChannel <- false
	}
	return nil
}

func (fbMsgInstance FizzBuzz) ProduceMessage(producer *sarama.SyncProducer, evalTopic string) error {
	marshaledFizzBuzzMsg, err := json.Marshal(fbMsgInstance)
	if err != nil {
		log.Printf("FizzBuzz::ProduceMessage - Error while mashaling msg: %v - error: %v\n", fbMsgInstance, err)
		return err
	}

	msg := &sarama.ProducerMessage{Topic: evalTopic, Value: sarama.StringEncoder(marshaledFizzBuzzMsg)}
	partition, offset, err := (*producer).SendMessage(msg)

	if err != nil {
		log.Printf("FizzBuzz::ProduceMessage - Error while sending message - msg: %v - error: %v\n", fbMsgInstance, err)
		return err
	}
	log.Printf("FizzBuzz::ProduceMessage - message msg: %v - sent to partition %d at offset %d\n", fbMsgInstance, partition, offset)
	return nil
}
