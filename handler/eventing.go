package handler

import (
	"encoding/json"
	"errors"
	"github.com/bludot/event-streaming-redis-demo/config"
	"github.com/bludot/event-streaming-redis-demo/internal/services/consumer"
	"github.com/bludot/event-streaming-redis-demo/internal/services/processor"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
	"log"
	"math/rand"
	"time"
)

type EventHeader struct {
	TraceID string `json:"trace_id"`
}

type Event[T any] struct {
	Payload T   `json:"payload"`
	Retries int `json:"retries"`
}

type MessagePayload struct {
	Headers EventHeader `json:"headers"`
	Message string      `json:"message"`
	Retries int         `json:"retries"`
}

func Consume() error {
	cfg := config.LoadConfig()
	redisClient := redis.NewRedisClient(cfg.Redis)
	// consumerID := uuid.NewString()
	consumerID := "message-consumer"
	consumerInstance := consumer.NewConsumer[Event[MessagePayload]](consumerID, []string{"messages"}, "message-consumer-group", redisClient)
	publisherInstance := publisher.NewPublisher[Event[MessagePayload]](redisClient)
	processorInstance := processor.NewProcessor[Event[MessagePayload]](publisherInstance)

	var count int

	err := consumerInstance.Consume(func(data string) error {
		return processorInstance.Process(data, func(data Event[MessagePayload]) error {
			jsonString, _ := json.Marshal(data)
			log.Println("Processing event", string(jsonString))
			// sleep between 0.1 to 20 seconds
			time.Sleep(time.Duration(rand.Intn(20000)) * time.Millisecond)
			//return nil
			if count > 100 && count < 105 {

				count = 0
				return errors.New("Fake fail")
			}
			count++

			return nil
		})
	})

	return err
}
