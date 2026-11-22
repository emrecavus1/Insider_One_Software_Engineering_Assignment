package ratelimit

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
)

const window = time.Second

// The script atomically checks and reserves capacity in a rolling window
var slidingWindowScript = redis.NewScript(`
	local key = KEYS[1]
	local limit = tonumber(ARGV[1])
	local member = ARGV[2]

	local current = redis.call('TIME')
	local now = tonumber(current[1]) * 1000000
		+ tonumber(current[2])
	local cutoff = now - 1000000

	redis.call('ZREMRANGEBYSCORE', key, '-inf', cutoff)

	local count = redis.call('ZCARD', key)

	if count < limit then
		redis.call('ZADD', key, now, member)
		redis.call('PEXPIRE', key, 1000)
		return 1
	end

	local oldest = redis.call(
		'ZRANGE', key, 0, 0, 'WITHSCORES'
	)

	local waitMicroseconds =
		tonumber(oldest[2]) + 1000000 - now

	local waitMilliseconds = math.floor(
		(waitMicroseconds + 999) / 1000
	)

	if waitMilliseconds < 1 then
		waitMilliseconds = 1
	end

	return -waitMilliseconds
`)

type RedisLimiter struct {
	client    *redis.Client
	namespace string
	limit     int
}

// NewRedisLimiter creates a limiter shared by clients using the same namespace
func NewRedisLimiter(
	client *redis.Client,
	namespace string,
	limit int,
) (*RedisLimiter, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis client is required")
	}

	if strings.TrimSpace(namespace) == "" ||
		len(namespace) > 128 {
		return nil, fmt.Errorf("invalid rate limiter namespace")
	}

	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf(
			"channel limit must be between 1 and 100",
		)
	}

	return &RedisLimiter{
		client:    client,
		namespace: namespace,
		limit:     limit,
	}, nil
}

// Allow reserves one channel slot or returns the delay until capacity is available
func (l *RedisLimiter) Allow(
	ctx context.Context,
	channel notification.Channel,
) (allowed bool, retryAfter time.Duration, err error) {
	switch channel {
	case notification.ChannelSMS,
		notification.ChannelEmail,
		notification.ChannelPush:
	default:
		return false, 0, fmt.Errorf(
			"unsupported channel: %s", channel,
		)
	}

	key := l.namespace + ":" + string(channel)

	value, err := slidingWindowScript.Run(
		ctx,
		l.client,
		[]string{key},
		l.limit,
		uuid.NewString(),
	).Int64()

	if err != nil {
		return false, 0, fmt.Errorf(
			"evaluate Redis rate limit: %w", err,
		)
	}

	if value == 1 {
		return true, 0, nil
	}

	if value >= 0 {
		return false, 0, fmt.Errorf(
			"unexpected Redis rate limit result: %d", value,
		)
	}

	return false, time.Duration(-value) * time.Millisecond, nil
}
