package main

import (
	"github.com/bludot/event-streaming-redis-demo/handler"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
)

func main() {
	redisClient := redis.NewRedisClient()
	publisherInstance := publisher.NewPublisher[handler.EventHeader, handler.MessagePayload](redisClient)
	err := publisherInstance.Publish("messages", handler.EventHeader{TraceID: "123"}, handler.MessagePayload{Message: "Hello World2"})
	if err != nil {
		panic(err)
	}
}
