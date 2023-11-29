package consumer

import (
	"encoding/base64"
	"encoding/json"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
)

type ConsumerFunc[T any] func(jsonString string) error

type Consumer[T any] interface {
	Consume(fn ConsumerFunc[T]) error
}

type ConsumerImpl[T any] struct {
	ConsumerID    string
	Streams       []string
	ConsumerGroup string
	RedisClient   redis.RedisClient
}

func NewConsumer[T any](consumerID string, streams []string, consumerGroup string, redisClient redis.RedisClient) ConsumerImpl[T] {
	return ConsumerImpl[T]{
		ConsumerID:    consumerID,
		Streams:       streams,
		ConsumerGroup: consumerGroup,
		RedisClient:   redisClient,
	}
}

func (c *ConsumerImpl[T]) Consume(fn ConsumerFunc[T]) error {

	err := c.RedisClient.Consume(c.Streams, c.ConsumerID, c.ConsumerGroup, func(data map[string]interface{}) error {
		var payload T
		// data to json
		// base64 decode payload
		dataPayloadJsonString, err := base64.StdEncoding.DecodeString(data["payload"].(string))
		if err != nil {
			return err
		}

		// base64 decode headers
		dataHeadersJsonString, err := base64.StdEncoding.DecodeString(data["headers"].(string))
		if err != nil {
			return err
		}

		headersMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(dataHeadersJsonString), &headersMap)
		if err != nil {
			return err
		}

		payloadMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(dataPayloadJsonString), &payloadMap)
		if err != nil {
			return err
		}

		eventMap := make(map[string]interface{})
		eventMap["payload"] = payloadMap
		eventMap["headers"] = headersMap

		bytes, err := json.Marshal(eventMap)
		if err != nil {
			return err
		}

		err = json.Unmarshal(bytes, &payload)
		err = fn(string(bytes))
		if err != nil {
			return err
		}
		return nil
	})

	return err
}
