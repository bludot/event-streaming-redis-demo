package processor

import (
	"encoding/json"
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
}

func NewProcessor[T any]() ProcessorImpl[T] {
	return &Processor[T]{
		ExponentialBackOff: backoff.NewExponentialBackOff(),
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

func (p *Processor[T]) Process(payload string, fn ProcessorFunc[T]) error {
	data, err := p.Parse(payload)
	if err != nil {
		return err
	}

	// wrap operation
	operation := func() error {
		return fn(*data)
	}

	duration := p.ExponentialBackOff.NextBackOff()
	if duration == backoff.Stop {
		p.ExponentialBackOff.Reset()
	} else {
		err = operation()
		if err != nil {
			time.Sleep(duration)
			return err
		}
	}

	return nil
}
