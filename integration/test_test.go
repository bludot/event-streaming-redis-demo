package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestMain(m *testing.M) {

	ctx := context.Background()

	c, err := redis.RunContainer(
		ctx,
		testcontainers.WithImage("redis:7.2.3-alpine"),

		testcontainers.WithWaitStrategy(
			wait.ForLog(".*Ready to accept connections tcp.*\\n").AsRegexp().
				WithOccurrence(1).WithStartupTimeout(10*time.Second)),
	)
	if err != nil {
		fmt.Printf("failed to start container: %s", err)
		os.Exit(1)
	}

	ports, err := c.Ports(ctx)
	marshaled, _ := json.Marshal(ports)
	log.Println(string(marshaled[:]))
	log.Println(ports["6379/tcp"][0].HostPort)
	os.Setenv("REDIS_PORT", ports["6379/tcp"][0].HostPort)

	time.Sleep(1 * time.Second)
	code := m.Run()
	defer func() {
		if err := c.Terminate(ctx); err != nil {
			fmt.Printf("failed to terminate container: %s", err)
		}
	}()

	// shutdown()
	os.Exit(code)
}
