package redis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/bludot/event-streaming-redis-demo/config"
	"github.com/go-redis/redis"
	"log"
	"strconv"
)

type ConsumerFunc func(data map[string]interface{}) error

type RedisClient interface {
	Publish(channel string, payload map[string]interface{}) error
	Consume(streams []string, consumerID string, consumerGroup string, fn ConsumerFunc) error
}

type RedisImpl struct {
	Client *redis.Client
}

func NewRedisClient(conf config.RedisConfig) RedisClient {
	log.Println("Creating RedisClient" + conf.Host + ":" + strconv.Itoa(conf.Port))
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%s", conf.Host, strconv.Itoa(conf.Port)),
	})
	return &RedisImpl{
		Client: redisClient,
	}
}

func (r *RedisImpl) convertMapStringToBase64(data map[string]interface{}) (map[string]interface{}, error) {
	for key, value := range data {
		switch value.(type) {
		case string:
			data[key] = base64.StdEncoding.EncodeToString([]byte(value.(string)))
		case map[string]interface{}:
			// convert to json string
			jsonString, err := json.Marshal(value)
			if err != nil {
				return nil, err
			}
			data[key] = base64.StdEncoding.EncodeToString(jsonString)
		}
	}
	return data, nil
}

func (r *RedisImpl) Publish(channel string, payload map[string]interface{}) error {
	log.Println("Publishing event to RedisClient")
	payload64, err := r.convertMapStringToBase64(payload)

	err = r.Client.XAdd(&redis.XAddArgs{
		Stream:       "messages",
		MaxLen:       0,
		MaxLenApprox: 0,
		ID:           "",
		Values:       payload64,
	}).Err()

	return err
}

func (r *RedisImpl) Consume(streams []string, consumerID string, consumerGroup string, fn ConsumerFunc) error {

	//_ = r.consumePending(streams, consumerID, consumerGroup, fn)

	_ = r.consumeFromBeginning(streams, consumerID, consumerGroup, fn)

	log.Println("Consuming events from RedisClient")
	// only get new messages
	streams = append(streams, ">")
	for _, stream := range streams {
		err := r.Client.XGroupCreateMkStream(stream, consumerGroup, "$").Err()
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
		log.Println("Got new messages")

		for _, stream := range streams {
			for _, message := range stream.Messages {
				processorError := fn(message.Values)

				// err = r.Client.XAck(stream.Stream, consumerGroup, message.ID).Err()
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

func (r *RedisImpl) consumeFromBeginning(streamsStrings []string, consumerID string, consumerGroup string, fn ConsumerFunc) error {
	streamsStrings = append(streamsStrings, "0")
	for _, stream := range streamsStrings {
		err := r.Client.XGroupCreateMkStream(stream, consumerGroup, "0").Err()
		if err != nil {
			log.Println(err)
		}
	}

	streams, err := r.Client.XReadGroup(&redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumerID,
		Streams:  streamsStrings,
		Count:    100000,
		Block:    0,
		NoAck:    false,
	}).Result()
	if err != nil {
		return err
	}

	for _, stream := range streams {
		for _, message := range stream.Messages {
			processorError := fn(message.Values)

			// err = r.Client.XAck(stream.Stream, consumerGroup, message.ID).Err()
			if err != nil {
				return err
			}
			if processorError != nil {
				return processorError
			}
		}
	}
	return nil

}

func (r *RedisImpl) consumePending(streamsStrings []string, consumerID string, consumerGroup string, fn ConsumerFunc) error {
	log.Println("Consuming pending messages from RedisClient")
	streamsStrings = append(streamsStrings, "0")
	for _, stream := range streamsStrings {
		err := r.Client.XGroupCreateMkStream(stream, consumerGroup, "$").Err()
		if err != nil {
			log.Println(err)
		}
	}

	stream, err := r.Client.XPendingExt(&redis.XPendingExtArgs{
		Stream:   streamsStrings[0],
		Group:    consumerGroup,
		Start:    "-",
		End:      "+",
		Count:    100000,
		Consumer: consumerID,
	}).Result()

	if err != nil {
		return err
	}
	// get all pending messages ids and consume them
	for _, message := range stream {
		// get message
		message, err := r.Client.XRangeN(streamsStrings[0], message.Id, message.Id, 1).Result()
		if err != nil {
			return err
		}
		// consume message
		processorError := fn(message[0].Values)

		//err = r.Client.XAck(streamsStrings[0], consumerGroup, message[0].ID).Err()
		if err != nil {
			return err
		}
		if processorError != nil {
			return processorError
		}
	}

	log.Println("Done")
	return nil

}
