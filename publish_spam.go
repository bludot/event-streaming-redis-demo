package main

import (
	"github.com/bludot/event-streaming-redis-demo/config"
	"github.com/bludot/event-streaming-redis-demo/handler"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
	"strconv"
	"sync"
	"time"
)

func main() {
	// run go routines to publish x messages, use waitgroups
	count := 0
	wg := sync.WaitGroup{}
	for i := 0; i < 100000; i++ {

		count++
		if count == 100 {
			count = 0
			wg.Wait()
			time.Sleep(100 * time.Millisecond)
		}
		wg.Add(1)
		go publishMessage(func() {
			wg.Done()
		}, i)

	}

	wg.Wait()

}

func publishMessage(finish func(), count int) {
	defer finish()
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
			Message: "Hello world " + strconv.Itoa(count),
		},
		Retries: 1,
	})
	if err != nil {
		panic(err)
	}

}
