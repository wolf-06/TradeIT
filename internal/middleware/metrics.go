package middleware

import (
	"strconv"
	"time"

	"TradeIT/internal/metrics"

	"github.com/gin-gonic/gin"
)

// Metrics records request counts and latency per matched route. Unmatched routes are
// grouped under "unmatched" so a scanner cannot explode label cardinality.
func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequests.WithLabelValues(route, c.Request.Method, strconv.Itoa(c.Writer.Status())).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	}
}
