package projection

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisCache is the broker-facing order status cache. Keys carry a TTL and the Redis
// instance is expected to run volatile-lru, so entries expire or are evicted when memory
// is tight - whichever comes first. A cache miss is expected and handled by the reader
// falling back to the database.
type RedisCache struct {
	client *redis.Client
	ttl    time.Duration
	prefix string
}

// NewRedisCache wraps a redis client. ttl <= 0 disables expiry.
func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{client: client, ttl: ttl, prefix: "order:"}
}

type cachedStatus struct {
	Status    string `json:"status"`
	Remaining int    `json:"remq"`
	AvgPrice  int64  `json:"avgPrice"`
	UpdatedAt int64  `json:"updatedAt"`
}

// SetOrderStatus writes an order's current status. It is best-effort: callers log and
// continue on error so a Redis outage never blocks projection.
func (c *RedisCache) SetOrderStatus(orderID uint64, status string, remaining int, avgPrice int64) error {
	b, err := json.Marshal(cachedStatus{
		Status:    status,
		Remaining: remaining,
		AvgPrice:  avgPrice,
		UpdatedAt: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	key := c.prefix + strconv.FormatUint(orderID, 10)
	return c.client.Set(context.Background(), key, b, c.ttl).Err()
}
