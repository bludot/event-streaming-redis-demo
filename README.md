# Event Streams with Redis and Golang example

### Important notes
Redis streams work very similarly to Kafka, but they are not exactly the same. One differnce is that Kafka supports 
partitions which Redis does not. The use of partitions in Kafka allows for high concurrency and throughput, supporting 
multiple consumers sharing the message processing load and multi-threading/multiple nodes on the server-side as well.

Partitions also give Kafka the ability to process events in-order, within each partition. Redis Streams can’t give this
guarantee, and the closest solution would be to use multiple streams/keys, with a single consumer per stream.

Kafka has better automatic management of consumer groups, including automatic rebalancing when consumers come and go. 
Both Redis Streams XREADGROUPS and Kafka support at-least-once-delivery, and exactly-once processing semantics (with 
explicit Redis Streams XACKS and manual offset commits in Kafa, rather than the default auto-commit).

Other limitations include the storing of data (mainly through the library used). With Redis, we are unable to properly 
store nested objects (or binary/base64 encoded data). Kafka supports proper storing and retrieval of nested objects, as
well as schema validation and evolution out of the box. It is possible with Redis, however requires more development 
work. 

## How messages are conusmed

Messages are consumed in the order they are received. Additionally, messages sent to the same consumer group are not 
resent which means that if you have multiple instances of a consumer with the same consumer group/id, you will receive a
message only once. Messages are consumed using [XREADGROUP](https://redis.io/commands/xreadgroup/). To read from the 
beginning of the stream, use `0` as the `last_id` parameter. To read from the end of the stream, use `>` as the 
`last_id`.

## How messages are produced

Producing messages is quite simeple. Messages are produced using [XADD](https://redis.io/commands/xadd/). There is a 
limitation due to the library that the message value sent can only be a depth of 1. This means that you cannot send 
nested objects, or binary/base64 encoded data.


## Testing the waters

Start docker `docker compose up`.

To start the consumer run: `go run main.go serve eventing`. This will start a consumer that will read from the stream 
`messages`. 

To send message run `go run publish.go`. This will send a message to the stream `messages`. You should see logs of 
messages being consumed. 


## TODO

- [X] Add support for nested objects
  - encoding depth 1 values to json strings and then base64, decoding on consumer (same package)
- [ ] Add support for reading from the beginning of the stream on consumer start (currently only reads from the end)
