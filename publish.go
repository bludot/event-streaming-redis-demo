package main

import (
	"github.com/bludot/event-streaming-redis-demo/config"
	"github.com/bludot/event-streaming-redis-demo/handler"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
)

func main() {
	redisClient := redis.NewRedisClient(config.RedisConfig{
		Host: "localhost",
		Port: 6379,
	})
	publisherInstance := publisher.NewPublisher[handler.Event[handler.MessagePayload]](redisClient)
	err := publisherInstance.Publish("messages", handler.Event[handler.MessagePayload]{
		Payload: handler.MessagePayload{
			Headers: handler.EventHeader{
				TraceID: "1234",
			},
			Message: "Hello world",
		},
		Retries: 1,
	})
	if err != nil {
		panic(err)
	}
}
