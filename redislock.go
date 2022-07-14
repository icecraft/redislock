package redislock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

const (
	reEnterantDisLock         = "re_enterant_dis_lock"
	reEnterantDisLockCtxValue = "yes"

	redisLuaSuccRetCode = 0
	MaxKeyValue         = 1024
	defaultIncrValue    = 1
)

var (
	ErrDupUnlock = errors.New("can not release lock twice")
	// ErrNotObtained is returned when a lock cannot be obtained.
	ErrNotObtained = errors.New("redislock: not obtained")

	// ErrLockNotHeld is returned when trying to release an inactive lock.
	ErrLockNotHeld = errors.New("redislock: lock not held")

	incrBy = redis.NewScript(`

	redis.replicate_commands()

	local key = KEYS[1]
	local key_id = KEYS[2]
	local key_count = KEYS[3]
	local key_id_value = ARGV[1]
	local incr_key_count_value = tonumber(ARGV[2])
	local expired_time = tonumber(ARGV[3])
	
	local is_count_existed = redis.call("HEXISTS", key, key_count)
	local is_id_existed  = redis.call("HEXISTS", key, key_id)
	
	if is_count_existed == 1 and is_id_existed == 1 then
	
		local count_value  = redis.call("HGET", key, key_count)
		local remote_id_value = redis.call("HGET", key, key_id)
	
		if remote_id_value ~= key_id_value then
			return 1
		end
	
		if count_value == "0" then
			return 2
		end
	
		if tonumber(count_value) + tonumber(incr_key_count_value) > 1024 then 
			return 3
		end

		redis.call("HINCRBY", key, key_count, incr_key_count_value)

		if expired_time > 0 then 
			redis.call("EXPIRE", key, expired_time)
		end
	
		local current_count  = redis.call("HGET", key, key_count)

		if current_count == "0" then
			redis.call("DEL", key)
		end

	elseif is_count_existed == 0 and is_id_existed == 0 then
		redis.call("HSET", key, key_count, incr_key_count_value)
		redis.call("HSET", key, key_id, key_id_value)
		redis.call("EXPIRE", key, expired_time)
	else
		return 4
	end
	
	return 0
	
`)
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
	tmp    []byte
	tmpMu  sync.Mutex
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
		if IsReEnterantLockContext(ctx) {
			retCode, err := incrBy.Run(ctx, c.client, []string{key, "id", "count"}, []interface{}{opt.LockId, opt.IncrValue, ttlVal}).Int()
			if err != nil {
				return nil, err
			}
			if retCode != redisLuaSuccRetCode {
				return nil, fmt.Errorf("failed to eval redis lua script, code: %d", retCode)
			}
			return &Lock{client: c, key: key, value: opt.LockId, isReEnterantLock: true, m: sync.Mutex{}, opt: opt}, nil
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
	client           *Client
	key              string
	value            string
	isReEnterantLock bool
	m                sync.Mutex
	released         bool
	opt              *Options
}

func NewReEnterantLockContext(ctx context.Context) (context.Context, error) {
	return context.WithValue(ctx, reEnterantDisLock, reEnterantDisLockCtxValue), nil
}

func NewLockContext(ctx context.Context) context.Context {
	return ctx
}

func IsReEnterantLockContext(ctx context.Context) bool {
	return fmt.Sprintf("%s", ctx.Value(reEnterantDisLock)) == reEnterantDisLockCtxValue
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

	if IsReEnterantLockContext(ctx) {
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

// --------------------------------------------------------------------

// RetryStrategy allows to customise the lock retry strategy.
type RetryStrategy interface {
	// NextBackoff returns the next backoff duration.
	NextBackoff() time.Duration
}

type linearBackoff time.Duration

// LinearBackoff allows retries regularly with customized intervals
func LinearBackoff(backoff time.Duration) RetryStrategy {
	return linearBackoff(backoff)
}

// NoRetry acquire the lock only once.
func NoRetry() RetryStrategy {
	return linearBackoff(0)
}

func (r linearBackoff) NextBackoff() time.Duration {
	return time.Duration(r)
}

type limitedRetry struct {
	s   RetryStrategy
	cnt int64
	max int64
}

// LimitRetry limits the number of retries to max attempts.
func LimitRetry(s RetryStrategy, max int) RetryStrategy {
	return &limitedRetry{s: s, max: int64(max)}
}

func (r *limitedRetry) NextBackoff() time.Duration {
	if atomic.LoadInt64(&r.cnt) >= r.max {
		return 0
	}
	atomic.AddInt64(&r.cnt, 1)
	return r.s.NextBackoff()
}

type exponentialBackoff struct {
	cnt uint64

	min, max time.Duration
}

// ExponentialBackoff strategy is an optimization strategy with a retry time of 2**n milliseconds (n means number of times).
// You can set a minimum and maximum value, the recommended minimum value is not less than 16ms.
func ExponentialBackoff(min, max time.Duration) RetryStrategy {
	return &exponentialBackoff{min: min, max: max}
}

func (r *exponentialBackoff) NextBackoff() time.Duration {
	cnt := atomic.AddUint64(&r.cnt, 1)

	ms := 2 << 25
	if cnt < 25 {
		ms = 2 << cnt
	}

	if d := time.Duration(ms) * time.Millisecond; d < r.min {
		return r.min
	} else if r.max != 0 && d > r.max {
		return r.max
	} else {
		return d
	}
}

func randomToken(buf []byte) (string, error) {
	if _, err := io.ReadFull(rand.Reader, buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
