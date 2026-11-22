package ratelimit_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/notification"
	"github.com/emrecavus1/Insider_One_Software_Engineering_Assignment/internal/ratelimit"
)

func testRedis(t *testing.T) (*redis.Client, string) {
	t.Helper()

	if os.Getenv("RUN_REDIS_TESTS") != "1" {
		t.Skip("Redis integration tests are disabled")
	}

	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "16379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(host, port),
	})

	ctx, cancel := context.WithTimeout(
		context.Background(), 3*time.Second,
	)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Fatalf("connect to Redis: %v", err)
	}

	namespace := "insider-one:test:" + uuid.NewString()

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 3*time.Second,
		)
		defer cleanupCancel()

		_ = client.Del(
			cleanupCtx,
			namespace+":sms",
			namespace+":email",
			namespace+":push",
		).Err()

		_ = client.Close()
	})

	return client, namespace
}

func TestRedisLimiterChannelIsolationIntegration(t *testing.T) {
	client, namespace := testRedis(t)

	limiter, err := ratelimit.NewRedisLimiter(
		client, namespace, 2,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	channels := []notification.Channel{
		notification.ChannelSMS,
		notification.ChannelEmail,
		notification.ChannelPush,
	}

	for _, channel := range channels {
		t.Run(string(channel), func(t *testing.T) {
			for i := 0; i < 2; i++ {
				allowed, _, err := limiter.Allow(ctx, channel)
				if err != nil || !allowed {
					t.Fatalf(
						"expected admission %d: %v",
						i+1, err,
					)
				}
			}

			allowed, delay, err := limiter.Allow(ctx, channel)
			if err != nil {
				t.Fatal(err)
			}

			if allowed || delay <= 0 || delay > time.Second {
				t.Errorf(
					"expected rate limit and retry delay, got allowed=%v delay=%v",
					allowed, delay,
				)
			}
		})
	}
}

func TestRedisLimiterConcurrentIntegration(t *testing.T) {
	clientA, namespace := testRedis(t)

	clientB := redis.NewClient(clientA.Options())
	defer clientB.Close()

	limiterA, err := ratelimit.NewRedisLimiter(
		clientA, namespace, 100,
	)
	if err != nil {
		t.Fatal(err)
	}

	limiterB, err := ratelimit.NewRedisLimiter(
		clientB, namespace, 100,
	)
	if err != nil {
		t.Fatal(err)
	}

	const requests = 200

	start := make(chan struct{})
	results := make(chan bool, requests)
	errs := make(chan error, requests)

	var wg sync.WaitGroup

	startedAt := time.Now()

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func(index int) {
			defer wg.Done()
			<-start

			limiter := limiterA
			if index%2 == 1 {
				limiter = limiterB
			}

			allowed, _, err := limiter.Allow(
				context.Background(),
				notification.ChannelSMS,
			)

			if err != nil {
				errs <- fmt.Errorf(
					"request %d: %w", index, err,
				)
				return
			}

			results <- allowed
		}(i)
	}

	close(start)

	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	allowedCount := 0
	deniedCount := 0

	for allowed := range results {
		if allowed {
			allowedCount++
		} else {
			deniedCount++
		}
	}

	elapsed := time.Since(startedAt)

	if elapsed < time.Second {
		if allowedCount != 100 || deniedCount != 100 {
			t.Errorf(
				"expected 100 allowed and 100 denied, got %d and %d",
				allowedCount, deniedCount,
			)
		}
	} else {
		t.Logf(
			"requests spanned multiple windows: %v, allowed=%d denied=%d",
			elapsed, allowedCount, deniedCount,
		)
	}
}

func TestRedisLimiterSlidingWindowIntegration(t *testing.T) {
	client, namespace := testRedis(t)

	limiter, err := ratelimit.NewRedisLimiter(
		client, namespace, 1,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	allowed, _, err := limiter.Allow(
		ctx, notification.ChannelEmail,
	)
	if err != nil || !allowed {
		t.Fatalf("first request should pass: %v", err)
	}

	allowed, delay, err := limiter.Allow(
		ctx, notification.ChannelEmail,
	)
	if err != nil || allowed {
		t.Fatalf("second request should be limited: %v", err)
	}

	if delay <= 0 {
		t.Fatal("expected positive retry delay")
	}

	time.Sleep(delay + 30*time.Millisecond)

	allowed, _, err = limiter.Allow(
		ctx, notification.ChannelEmail,
	)
	if err != nil || !allowed {
		t.Fatalf("capacity did not recover: %v", err)
	}
}

func TestRedisLimiterFailsClosed(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1",
	})
	defer client.Close()

	limiter, err := ratelimit.NewRedisLimiter(
		client, "insider-one:test:unavailable", 100,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 250*time.Millisecond,
	)
	defer cancel()

	allowed, _, err := limiter.Allow(
		ctx, notification.ChannelSMS,
	)

	if err == nil || allowed {
		t.Error("Redis failure must not grant admission")
	}
}
