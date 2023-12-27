package main

import (
	"context"
	"gseph/ocd-fizzbuzz/pkg/datamodel"
	"io"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/hashicorp/go-uuid"
)

func main() {

	producer, err := sarama.NewSyncProducer([]string{datamodel.BootstrapServer}, nil)
	if err != nil {
		log.Fatalf("api::main - producer error %v\n", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("api::main - consumer closing error %v\n", err)
		}
	}()

	router := gin.Default()

	router.POST("/send", CORSMiddleware(), PostSendHandler(&producer, datamodel.Topic))

	router.GET("/stream/:uuid", CORSMiddleware(), HeadersMiddleware(), StreamHandler())

	router.Run()

}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func CORSStreamMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func HeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
		c.Writer.Header().Set("Transfer-Encoding", "chunked")
		c.Next()
	}
}

func PostSendHandler(producer *sarama.SyncProducer, topic string) gin.HandlerFunc {
	fn := func(c *gin.Context) {
		var fizzBuzzMessage datamodel.FizzBuzz
		err := c.BindJSON(&fizzBuzzMessage)

		if err != nil {
			log.Printf("api::PostSendHandler - Error binding json %v\n", fizzBuzzMessage)
		}

		fizzBuzzMessage.Uuid, err = uuid.GenerateUUID()

		if err != nil {
			log.Printf("api::PostSendHandler - Error generating uuid - error: %v\n", err)
			c.JSON(200, datamodel.FizzBuzz{})
			return
		}

		log.Printf("api::PostSendHandler - producing message %v\n", fizzBuzzMessage)

		err = fizzBuzzMessage.ProduceMessage(producer, topic)
		if err != nil {
			log.Printf("api::PostSendHandler - Error producing message %v - error: %v\n", fizzBuzzMessage, err)
			c.JSON(200, datamodel.FizzBuzz{})
			return
		}

		c.JSON(200, fizzBuzzMessage)

	}
	return gin.HandlerFunc(fn)
}

func StreamHandler() gin.HandlerFunc {
	fn := func(c *gin.Context) {
		uuid := c.Param("uuid")
		groupId := strings.Replace(uuid, "-", "", 4)
		config := sarama.NewConfig()
		config.Version = sarama.V3_6_0_0 // specify appropriate version
		config.Consumer.Return.Errors = true
		config.Consumer.Offsets.Initial = sarama.OffsetOldest

		group, err := sarama.NewConsumerGroup([]string{datamodel.BootstrapServer}, groupId, config)
		if err != nil {
			panic(err)
		}
		defer func() { _ = group.Close() }()

		// Track errors
		go func() {
			for err := range group.Errors() {
				log.Printf("api::StreamHandler group errors tracking - groupId: %v - error: %v\n", groupId, err)
			}
		}()

		doneCh := make(chan bool)
		outChannel := make(chan datamodel.FizzBuzz)
		ready := make(chan bool)

		go func() {
			// Iterate over consumer sessions.
			ctx := context.Background()
			topics := []string{datamodel.Topic}
			handler := datamodel.SSEGroupHandler{ReadyChannel: &ready, OutChannel: outChannel, DoneChannel: doneCh, Uuid: uuid}

			for {
				err := group.Consume(ctx, topics, handler)

				if err != nil {
					log.Printf("api::StreamHandler goroutine consume - group consuming error - groupId: %v - error: %v\n", groupId, err)
					return
				}

			}
		}()
		<-ready

		go func() {
			c.Stream(func(w io.Writer) bool {
				if msg, ok := <-outChannel; ok {
					log.Printf("api::StreamHandler goroutine stream - message %v - ok: %v\n", msg, ok)
					c.SSEvent("message", msg)
					return true
				}
				return false
			})
		}()

		for done := range doneCh {
			if done {
				group.Close()
				log.Printf("api::StreamHandler - closing for group %v\n", groupId)
				return
			}
		}

	}
	return gin.HandlerFunc(fn)
}
