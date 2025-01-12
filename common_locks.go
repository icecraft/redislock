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
type SLock struct {
	client    *Client
	key       string
	value     string
	m         sync.Mutex
	released  bool
	opt       *Options
	createdAt int64
	refreshAt int64
}

// is lock shared
func (l *SLock) IsSharedLock() bool {
	return true
}

// is lock released 
func (l *SLock) IsReleased() bool {
	var IsReleased bool 
	l.m.Lock()
	IsReleased = l.released
	l.m.Unlock()
	return IsReleased
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
	sLuaRefreshOnce.Do(func() {
		l.client.client.ScriptLoad(context.TODO(), sLuaRefresh)
	})

	ttlVal := strconv.FormatInt(int64(ttl/time.Millisecond), 10)
	status, err := l.client.client.EvalSha(ctx, sLuaRefreshSha1, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId, ttlVal}).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		l.m.Lock()
		l.refreshAt = time.Now().UnixMilli()
		l.m.Unlock()
		return nil
	}

	existedVal, err := l.client.client.Exists(ctx, l.key).Result()
	if err != nil {
		fmt.Printf("failed to check key existed, key: %s, reason: %s\n",l.key, err.Error())
	}

	return fmt.Errorf("failed to refresh key, lua script ret_code: %d, created_at: %d, prev refresh_at: %d, current mill timestamp:%d, key existed code: %d", status, l.createdAt, l.refreshAt, time.Now().UnixMilli(), existedVal)
}

// Release manually releases the lock.
// May return ErrLockNotHeld.
func (l *SLock) Release(ctx context.Context) error {
	incrByOnce.Do(func() {
		l.client.client.ScriptLoad(context.TODO(), incrBy)
	})

	l.m.Lock()
	if l.released {
		l.m.Unlock()
		return ErrDupUnlock
	}
	defer l.m.Unlock()
	retCode, err := l.client.client.EvalSha(ctx, incrBySha1, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId, -l.opt.IncrValue, 100}).Int()
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
	sLuaPTTLOnce.Do(func() {
		l.client.client.ScriptLoad(context.TODO(), sLuaPTTL)
	})

	res, err := l.client.client.EvalSha(ctx, sLuaPTTLSha1, []string{l.key, "id", "count"}, []interface{}{l.opt.LockId}).Result()
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
