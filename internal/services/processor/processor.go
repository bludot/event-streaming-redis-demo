package processor

import (
	"encoding/json"
	"errors"
	"github.com/bludot/event-streaming-redis-demo/internal/services/publisher"
	"github.com/cenkalti/backoff/v4"
	"time"
)

type ProcessorFunc[T any] func(data T) error

type ProcessorImpl[T any] interface {
	Parse(payload string) (*T, error)
	Process(payload string, fn ProcessorFunc[T]) error
}

type Processor[T any] struct {
	ExponentialBackOff *backoff.ExponentialBackOff
	Publisher          publisher.Publisher[T]
}

func NewProcessor[T any](publisher publisher.Publisher[T]) ProcessorImpl[T] {
	return &Processor[T]{
		ExponentialBackOff: backoff.NewExponentialBackOff(),
		Publisher:          publisher,
	}
}

func (p *Processor[T]) Parse(payload string) (*T, error) {
	// parse from json
	var data T
	err := json.Unmarshal([]byte(payload), &data)
	if err != nil {
		return nil, err
	}
	return &data, nil

}

func (p *Processor[T]) increaseRetries(payload string) (*T, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(payload), &data)
	if err != nil {
		return nil, err
	}

	if data["payload"] == nil {
		return nil, err
	}
	if data["retries"] == nil {
		data["retries"] = 0

	}
	retries := data["retries"].(float64)
	data["retries"] = retries + 1

	newPayloadJSON, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var newPayload T
	err = json.Unmarshal(newPayloadJSON, &newPayload)
	if err != nil {
		return nil, err
	}

	return &newPayload, nil

}

func (p *Processor[T]) getRetries(payload string) (int, error) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(payload), &data)
	if err != nil {
		return 0, err
	}

	if data["payload"] == nil {
		return 0, err
	}

	if data["retries"] == nil {
		data["retries"] = 0
	}

	retries, _ := data["retries"].(float64)

	return int(retries), nil

}

func (p *Processor[T]) Process(payload string, fn ProcessorFunc[T]) error {
	data, err := p.Parse(payload)
	if err != nil {
		return err
	}

	// wrap operation
	operation := func() error {
		return fn(*data)
	}

	retries, err := p.getRetries(payload)
	if err != nil {
		return err
	}

	if retries >= 10 {
		return errors.New("max retries")
	}

	duration := p.ExponentialBackOff.NextBackOff()
	if duration == backoff.Stop {
		p.ExponentialBackOff.Reset()
	} else {
		err = operation()
		if err != nil {
			newPayload, _ := p.increaseRetries(payload)
			p.publishRetry(*newPayload)

			time.Sleep(duration)
			return nil
		}
		p.ExponentialBackOff.Reset()
	}

	return nil
}

func (p *Processor[T]) publishRetry(message T) {
	_ = p.Publisher.Publish("messages", message)
}
