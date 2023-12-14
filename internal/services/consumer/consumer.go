package consumer

import (
	"encoding/base64"
	"encoding/json"
	"github.com/bludot/event-streaming-redis-demo/internal/services/redis"
	"log"
	"strconv"
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
		payload, err := c.decodeToStruct(data)
		if err != nil {
			log.Println("Error decoding payload", err)
			return nil
		}

		bytes, err := json.Marshal(payload)
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

func (c *ConsumerImpl[T]) decodeToStruct(data map[string]interface{}) (*T, error) {

	// for each field, decode from base64 and convert whole map to struct
	for key, value := range data {

		decoded, err := base64.StdEncoding.DecodeString(value.(string))
		if err != nil {
			// check if its a number
			val, err := strconv.Atoi(value.(string))
			if err != nil {
				data[key] = string(decoded)
				continue
			}

			data[key] = val
			continue
		}
		// convert to map[string]interface{}
		var decodedMap map[string]interface{}
		err = json.Unmarshal(decoded, &decodedMap)
		if err != nil {
			data[key] = string(decoded)
			continue
		}
		data[key] = decodedMap

	}

	// convert date to JSON string
	jsonString, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var payload T
	err = json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		return nil, err
	}

	return &payload, nil

}
