package engine

import (
	"fmt"
	"os"
	"sync"

	"github.com/go-redis/redis/v8"
)

var (
	clientsMu sync.Mutex
	clients   = map[int]*redis.Client{}
)

// GetEngineClient returns a redis client for the requested logical database.
//
// The previous version cached a single client for whatever db was requested first, so
// GetEngineClient(1) returned the DB 0 client: the cancellation/modification queues are
// published on DB 1 by the user-order service but were being read from DB 0, and those
// requests therefore never reached the engine at all.
func GetEngineClient(db int) *redis.Client {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	if client, exists := clients[db]; exists {
		return client
	}
	client := redis.NewClient(&redis.Options{
		Addr:     os.Getenv("redis_addr"),
		Password: os.Getenv("redis_password"),
		DB:       db,
	})
	clients[db] = client
	// fmt.Println("successfully created the redis client for db", db)
	return client
}

func custom(a, b int) {
	var mt = make(map[string]string)
	fmt.Println(mt)
}
