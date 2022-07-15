package redislock

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// Lock represents an obtained, distributed lock.
type Lock struct {
	client   *Client
	key      string
	value    string
	m        sync.Mutex
	released bool
	opt      *Options
}

// is lock shared
func (l *Lock) IsSharedLock() bool {
	return false
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
	if l.released {
		l.m.Unlock()
		return ErrDupUnlock
	}
	defer l.m.Unlock()

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

// Lock represents an obtained, distributed lock.
type SLock struct {
	client   *Client
	key      string
	value    string
	m        sync.Mutex
	released bool
	opt      *Options
}

// is lock shared
func (l *SLock) IsSharedLock() bool {
	return true
}

// Key returns the redis key used by the lock.
func (l *SLock) Key() string {
	return l.key
}

// Token returns the token value set by the lock.
func (l *SLock) Token() string {
	return l.value
}

// Refresh extends the lock with a new TTL.
// May return ErrNotObtained if refresh is unsuccessful.
func (l *SLock) Refresh(ctx context.Context, ttl time.Duration, opt *Options) error {
	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := sLuaRefresh.Run(ctx, l.client.client, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId, ttlVal}).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}
	return ErrNotObtained
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *SLock) Release(ctx context.Context) error {
	l.m.Lock()
	if l.released {
		l.m.Unlock()
		return ErrDupUnlock
	}
	defer l.m.Unlock()
	retCode, err := incrBy.Run(ctx, l.client.client, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId, -l.opt.IncrValue, 100}).Int()
	if err != nil {
		return err
	}
	if retCode != redisLuaSuccRetCode {
		return fmt.Errorf("failed to eval redis lua script, code: %d", retCode)
	}
	l.released = true
	return nil
}

// TTL returns the remaining time-to-live. Returns 0 if the lock has expired.
func (l *SLock) TTL(ctx context.Context) (time.Duration, error) {
	res, err := sLuaPTTL.Run(ctx, l.client.client, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId}).Result()
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
