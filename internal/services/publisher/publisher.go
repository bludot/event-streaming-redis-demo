package publisher

import (
	"encoding/json"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
)

type Publisher[H any, P any] interface {
	Publish(channel string, headers H, payload P) error
}

type PublisherImpl[H any, P any] struct {
	Redis redis.RedisClient
}

func NewPublisher[H any, P any](redisClient redis.RedisClient) Publisher[H, P] {
	return &PublisherImpl[H, P]{
		Redis: redisClient,
	}
}

func (p *PublisherImpl[H, P]) Publish(channel string, headers H, payload P) error {
	headersMap, err := convertToMapStringInterface(headers)
	if err != nil {
		return err
	}

	payloadMap, err := convertToMapStringInterface(payload)
	if err != nil {
		return err
	}

	return p.Redis.Publish(channel, headersMap, payloadMap)
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
