package processor_test

import (
	"errors"
	"github.com/bludot/event-streaming-redis-demo/internal/services/processor"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type GenderType = string

const (
	GenderMale      GenderType = "male"
	GenderFemale    GenderType = "female"
	GenderNonBinary GenderType = "non-binary"
	GenderOther     GenderType = "other"
)

type EventHeader struct {
	Key     string `json:"key"`
	TraceID string `json:"trace_id"`
}

type Event[T any] struct {
	EventHeader `json:"event_header"`
	Payload     T   `json:"payload"`
	Retries     int `json:"retries"`
}

type OnboardPayload struct {
	UserID      string     `json:"user_id"`
	DateOfBirth time.Time  `json:"date_of_birth"`
	Gender      GenderType `json:"gender"`
}

func TestProcessor_Process(t *testing.T) {
	t.Run("Should process payload successfully", func(t *testing.T) {
		a := assert.New(t)
		processorInstance := processor.NewProcessor[Event[OnboardPayload]]()
		err := processorInstance.Process(`{
			"key": "onboard",
			"trace_id": "1234",
			"payload": {
				"user_id": "1234",
				"date_of_birth": "2021-01-01T00:00:00Z",
				"gender": "male"
			},
			"retries": 0
		}`, func(data Event[OnboardPayload]) error {
			return nil
		})

		a.NoError(err)
	})

	t.Run("Should retry payload", func(t *testing.T) {
		a := assert.New(t)
		processorInstance := processor.NewProcessor[Event[OnboardPayload]]()
		err := processorInstance.Process(`{
			"key": "onboard",
			"trace_id": "1234",
			"payload": {
				"user_id": "1234",
				"date_of_birth": "2021-01-01T00:00:00Z",
				"gender": "male"
			},
			"retries": 0
		}`, func(data Event[OnboardPayload]) error {
			return errors.New("error")
		})

		a.Error(err)
		startTime := time.Now()
		err = processorInstance.Process(`{
			"key": "onboard",
			"trace_id": "1234",
			"payload": {
				"user_id": "1234",
				"date_of_birth": "2021-01-01T00:00:00Z",
				"gender": "male"
			},
			"retries": 0
		}`, func(data Event[OnboardPayload]) error {
			return errors.New("error")
		})
		endTime := time.Now()
		// check time difference
		a.GreaterOrEqual(endTime.Sub(startTime).Milliseconds(), int64(100))
		a.Error(err)

		startTime2 := time.Now()
		err = processorInstance.Process(`{
			"key": "onboard",
			"trace_id": "1234",
			"payload": {
				"user_id": "1234",
				"date_of_birth": "2021-01-01T00:00:00Z",
				"gender": "male"
			},
			"retries": 0
		}`, func(data Event[OnboardPayload]) error {
			return errors.New("error")
		})
		endTime2 := time.Now()

		a.Error(err)

		startTime3 := time.Now()
		err = processorInstance.Process(`{
			"key": "onboard",
			"trace_id": "1234",
			"payload": {
				"user_id": "1234",
				"date_of_birth": "2021-01-01T00:00:00Z",
				"gender": "male"
			},
			"retries": 0
		}`, func(data Event[OnboardPayload]) error {
			return errors.New("error")
		})
		endTime3 := time.Now()
		// check time difference
		a.GreaterOrEqual(endTime3.Sub(startTime3).Seconds(), endTime2.Sub(startTime2).Seconds())
		a.Error(err)

	})

}
