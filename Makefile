generate: mocks

mocks:
	go get go.uber.org/mock/mockgen/model
	go install go.uber.org/mock/mockgen@latest
	mockgen -destination=./mocks/publisher.go -package=mocks github.com/bludot/event-streaming-redis-demo/internal/services/publisher Publisher

