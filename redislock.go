package redislock

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisClient is a minimal client interface.
type RedisClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
}

// Client wraps a redis client.
type Client struct {
	client RedisClient
}

// New creates a new Client instance with a custom namespace.
func New(client RedisClient) *Client {
	return &Client{client: client}
}

// Obtain tries to obtain a new lock using a key with the given TTL.
// May return ErrNotObtained if not successful.
func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (ILock, error) {
	if opt == nil {
		opt = &Options{}
	}
	if opt.IncrValue == 0 {
		opt.IncrValue = MaxKeyValue
	}
	if opt.LockId == "" {
		// Create a random token
		buf := make([]byte, 16)
		token, err := randomToken(buf)
		if err != nil {
			return nil, err
		}
		opt.LockId = token
	}

	retry := opt.getRetryStrategy()

	// make sure we don't retry forever
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(maxSpinLockInterval))
		defer cancel()
	}

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	var timer *time.Timer

	retryCount := 0
	for {
		if IsSharedLockContext(ctx) {
			retCode, err := incrBy.Run(ctx, c.client, []string{key, "id", "count"}, []interface{}{opt.LockId, opt.IncrValue, ttlVal}).Int()
			if err != nil {
				fmt.Printf("err to incrby: %s\n", err.Error())
				return nil, err
			}
			if retCode == redisLuaSuccRetCode {
				return &SLock{client: c, key: key, value: opt.LockId, m: sync.Mutex{}, opt: opt}, nil
			}
		} else {
			ok, err := c.obtain(ctx, key, opt.LockId, ttl)
			if err != nil {
				return nil, err
			} else if ok {
				return &Lock{client: c, key: key, value: opt.LockId, m: sync.Mutex{}, opt: opt}, nil
			}
		}

		backoff := retry.NextBackoff()
		if backoff < 1 {
			return nil, ErrNotObtained
		}
		retryCount += 1

		if isNumInArr(retryCount, []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 1024, 2048, 4096, 8192, 16384, 32768}) {
			fmt.Printf("retry to obtain lock, retry_count: %d\n", retryCount)
		}

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			fmt.Printf("exceeded max spin lock allowed time duration:(%v)\n", maxSpinLockInterval)
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

// --------------------------------------------------------------------

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opt *Options) (ILock, error) {
	return New(client).Obtain(ctx, key, ttl, opt)
}

func NewSharedLockContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, sharedDisLock, sharedDisLockCtxValue)
}

func NewLockContext(ctx context.Context) context.Context {
	return ctx
}

func IsSharedLockContext(ctx context.Context) bool {
	return fmt.Sprintf("%s", ctx.Value(sharedDisLock)) == sharedDisLockCtxValue
}
