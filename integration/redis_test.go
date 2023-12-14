package integration

import (
	"errors"
	"github.com/bludot/event-streaming-redis-demo/config"
	"github.com/bludot/event-streaming-redis-demo/handler"
	"github.com/bludot/event-streaming-redis-demo/internal/services/consumer"
	"github.com/bludot/event-streaming-redis-demo/internal/services/processor"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	redisService "github.com/bludot/event-streaming-redis-demo/internal/services/redis"
	"github.com/stretchr/testify/assert"
	"log"
	"sync"
	"testing"
	"time"
)

type ConsumeMessages struct {
	Events []handler.Event[handler.MessagePayload]
	sync.Mutex
}

func TestRedisConsumer(t *testing.T) {
	cfg := config.LoadConfig()
	log.Println(cfg.Redis.Host)
	redisClient := redisService.NewRedisClient(config.RedisConfig{
		Host: cfg.Redis.Host,
		Port: cfg.Redis.Port,
	})

	publish := func() {
		redisClient.Publish("messages", map[string]interface{}{
			"headers": map[string]interface{}{
				"trace_id": "1234",
			},
			"payload": map[string]interface{}{
				"message": "Hello world",
			},
		})

	}

	t.Run("should consume message", func(t *testing.T) {
		a := assert.New(t)
		messages := ConsumeMessages{}
		publisherInstance := publisher.NewPublisher[handler.Event[handler.MessagePayload]](redisClient)
		processorInstance := processor.NewProcessor[handler.Event[handler.MessagePayload]](publisherInstance)
		consumerInstance := consumer.NewConsumer[handler.Event[handler.MessagePayload]]("consumer1", []string{"messages"}, "consumer_group", redisClient)
		go func() {
			err := consumerInstance.Consume(func(data string) error {
				return processorInstance.Process(data, func(data handler.Event[handler.MessagePayload]) error {
					t.Log(data)
					messages.Lock()

					messages.Events = append(messages.Events, data)
					messages.Unlock()
					return errors.New("Fake fail")
				})
			})

			a.NoError(err)
		}()
		publish()

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for len(messages.Events) < 10 {
				t.Log("Waiting for messages")
				time.Sleep(1 * time.Second)
			}
			t.Log("Done waiting for messages")
			wg.Done()
		}()
		wg.Wait()
		a.Equal("Hello world", messages.Events[0].Payload.Message)
	})
}
