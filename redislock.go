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
func (c *Client) Obtain(ctx context.Context, key string, ttl time.Duration, opt *Options) (*Lock, error) {
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
		ctx, cancel = context.WithDeadline(ctx, time.Now().Add(ttl))
		defer cancel()
	}

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	var timer *time.Timer
	for {
		if IsSharedLockContext(ctx) {
			retCode, err := incrBy.Run(ctx, c.client, []string{key, "id", "count"}, []interface{}{opt.LockId, opt.IncrValue, ttlVal}).Int()
			if err != nil {
				return nil, err
			}
			if retCode != redisLuaSuccRetCode {
				return nil, fmt.Errorf("failed to eval redis lua script, code: %d", retCode)
			}
			return &Lock{client: c, key: key, value: opt.LockId, m: sync.Mutex{}, opt: opt, isSharedLock: true}, nil
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

		if timer == nil {
			timer = time.NewTimer(backoff)
			defer timer.Stop()
		} else {
			timer.Reset(backoff)
		}

		select {
		case <-ctx.Done():
			return nil, ErrNotObtained
		case <-timer.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, ttl).Result()
}

// --------------------------------------------------------------------

// Lock represents an obtained, distributed lock.
type Lock struct {
	client       *Client
	key          string
	value        string
	m            sync.Mutex
	released     bool
	opt          *Options
	isSharedLock bool
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

// Obtain is a short-cut for New(...).Obtain(...).
func Obtain(ctx context.Context, client RedisClient, key string, ttl time.Duration, opt *Options) (*Lock, error) {
	return New(client).Obtain(ctx, key, ttl, opt)
}

// Key returns the redis key used by the lock.
func (l *Lock) Key() string {
	return l.key
}

// Token returns the token value set by the lock.
func (l *Lock) Token() string {
	return l.value
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := luaRefresh.Run(ctx, l.client.client, []string{l.key}, l.value, ttlVal).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *Lock) Release(ctx context.Context) error {
	l.m.Lock()
	if l.released == true {
		l.m.Unlock()
		return ErrDupUnlock
	}
	defer l.m.Unlock()

	if l.isSharedLock {
		retCode, err := incrBy.Run(ctx, l.client.client, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId, -l.opt.IncrValue, 100}).Int()
		if err != nil {
			return err
		}
		if retCode != redisLuaSuccRetCode {
			return fmt.Errorf("failed to eval redis lua script, code: %d", retCode)
		}
		l.released = true
		return nil
	} else {
		res, err := luaRelease.Run(ctx, l.client.client, []string{l.key}, l.value).Result()
		if err == redis.Nil {
			return ErrLockNotHeld
		} else if err != nil {
			return err
		}

		if i, ok := res.(int64); !ok || i != 1 {
			return ErrLockNotHeld
		}
		l.released = true
		return nil
	}
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	res, err := luaPTTL.Run(ctx, l.client.client, []string{l.key}, l.value).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	if num := res.(int64); num > 0 {
		return time.Duration(num) * time.Millisecond, nil
	}
	return 0, nil
}

// --------------------------------------------------------------------

func NewOptions(incrValue int, s RetryStrategy) *Options {
	return &Options{IncrValue: incrValue, RetryStrategy: s}
}

// Options describe the options for the lock
type Options struct {
	// RetryStrategy allows to customise the lock retry strategy.
	// Default: do not retry
	RetryStrategy RetryStrategy
	IncrValue     int // incrby when lock, decrby when release !!
	LockId        string
}

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}
