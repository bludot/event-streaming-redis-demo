package publisher

import (
	"encoding/json"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
)

type Publisher[T any] interface {
	Publish(channel string, payload T) error
}

type PublisherImpl[T any] struct {
	Redis redis.RedisClient
}

func NewPublisher[T any](redisClient redis.RedisClient) Publisher[T] {
	return &PublisherImpl[T]{
		Redis: redisClient,
	}
}

func (p *PublisherImpl[T]) Publish(channel string, payload T) error {

	payloadMap, err := convertToMapStringInterface(payload)
	if err != nil {
		return err
	}

	return p.Redis.Publish(channel, payloadMap)
}

func convertToMapStringInterface(data interface{}) (map[string]interface{}, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
