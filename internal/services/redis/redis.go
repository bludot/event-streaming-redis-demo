package redis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"log"
)

type ConsumerFunc func(data map[string]interface{}) error

type RedisClient interface {
	Publish(channel string, headers map[string]interface{}, payload map[string]interface{}) error
	Consume(streams []string, consumerID string, consumerGroup string, fn ConsumerFunc) error
}

type RedisImpl struct {
	Client *redis.Client
}

func NewRedisClient() RedisClient {
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", "127.0.0.1", "6379"),
	})
	return &RedisImpl{
		Client: redisClient,
	}
}

func (r *RedisImpl) Publish(channel string, headers map[string]interface{}, payload map[string]interface{}) error {
	log.Println("Publishing event to RedisClient")

	// convert payload to JSON string
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	payloadBase64 := base64.StdEncoding.EncodeToString(payloadJSON)
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return err
	}
	headersBase64 := base64.StdEncoding.EncodeToString(headersJSON)

	err = r.Client.XAdd(&redis.XAddArgs{
		Stream:       "messages",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values: map[string]interface{}{
			"headers": headersBase64,
			"payload": payloadBase64,
		},
	}).Err()

	return err
}

func (r *RedisImpl) Consume(streams []string, consumerID string, consumerGroup string, fn ConsumerFunc) error {
	log.Println("Consuming events from RedisClient")
	// only get new messages
	streams = append(streams, ">")
	for _, stream := range streams {
		err := r.Client.XGroupCreateMkStream(stream, consumerGroup, "0").Err()
		if err != nil {
			log.Println(err)
		}
	}

	for {
		streams, err := r.Client.XReadGroup(&redis.XReadGroupArgs{
			Group:    consumerGroup,
			Consumer: consumerID,
			Streams:  streams,
			Count:    1,
			Block:    0,
			NoAck:    false,
		}).Result()
		if err != nil {
			return err
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				processorError := fn(message.Values)

				err = r.Client.XAck(stream.Stream, consumerGroup, message.ID).Err()
				if err != nil {
					return err
				}
				if processorError != nil {
					return processorError
				}
			}
		}
	}
}
