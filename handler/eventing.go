package handler

import (
	"github.com/bludot/event-streaming-redis-demo/internal/services/consumer"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
	"log"
)

type EventHeader struct {
	TraceID string `json:"trace_id"`
}

type Event[T any] struct {
	EventHeader `json:"headers"`
	Payload     T   `json:"payload"`
	Retries     int `json:"retries"`
}

type MessagePayload struct {
	Message string `json:"message"`
}

func Consume() error {
	redisClient := redis.NewRedisClient()
	// consumerID := uuid.NewString()
	consumerID := "message-consumer"
	consumerInstance := consumer.NewConsumer[Event[MessagePayload]](consumerID, []string{"messages"}, "message-consumer-group", redisClient)
	err := consumerInstance.Consume(func(data Event[MessagePayload]) error {
		log.Println(data.Payload.Message)
		return nil
	})

	return err
}