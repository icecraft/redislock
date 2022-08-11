package redislock_test

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

const lockKey = "__bsm_redislock_unit_test__"

var redisOpts = &redis.Options{
	Network: "tcp",
	Addr:    "127.0.0.1:6379",
}

func TestClient(t *testing.T) {
	ctx := NewSharedLockContext(context.TODO())
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	// init client
	client := New(rc)

	// obtain
	lock, err := client.Obtain(ctx, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(ctx)

	if exp, got := 22, len(lock.Token()); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// check TTL
	assertTTL(t, lock, time.Hour)

	// try to obtain again
	_, err = client.Obtain(ctx, lockKey, time.Hour, nil)
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	// manually unlock
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}

	// lock again
	lock, err = client.Obtain(ctx, lockKey, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release(ctx)
}

func TestObtain(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		ctx := NewSharedLockContext(context.TODO())

		ctx2 := NewSharedLockContext(context.TODO())

		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock1, err := Obtain(ctx, rc, "test1", 100*time.Millisecond, &Options{IncrValue: 1, LockId: "11"})
		assert.NoError(t, err)

		lock2, err := Obtain(ctx, rc, "test1", 100*time.Millisecond, &Options{IncrValue: 1, LockId: "11"})
		assert.NoError(t, err)

		_, err = Obtain(ctx2, rc, "test1", 100*time.Millisecond, &Options{IncrValue: 1, LockId: "12"})
		assert.Error(t, err)

		err = lock2.Release(context.TODO())
		assert.NoError(t, err)

		err = lock1.Release(context.TODO())
		assert.NoError(t, err)

		// dup released
		err = lock1.Release(context.TODO())
		assert.Error(t, err)
	})

	t.Run("SLock_with_max_value", func(t *testing.T) {
		ctx := NewSharedLockContext(context.TODO())

		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock1, err := Obtain(ctx, rc, "testM", 10000000*time.Millisecond, &Options{IncrValue: MaxKeyValue})
		assert.NoError(t, err)

		_, err = Obtain(ctx, rc, "testM", 10000000*time.Millisecond, nil)
		assert.Error(t, err)

		err = lock1.Release(context.TODO())
		assert.NoError(t, err)

		lock2, err := Obtain(ctx, rc, "testM2", 1000000*time.Millisecond, nil)
		assert.NoError(t, err)

		err = lock2.Release(context.TODO())
		assert.NoError(t, err)

	})

}

func TestObtain_retry_success(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		now := time.Now().UnixMilli()
		lock1, err := Obtain(ctx, rc, "test1", 1000*time.Millisecond, &Options{IncrValue: 1024, LockId: "11"})
		assert.NoError(t, err)

		go func() {
			lock2, err := Obtain(ctx, rc, "test1", 1000*time.Millisecond, &Options{IncrValue: 1024, LockId: "11", RetryStrategy: LimitRetry(LinearBackoff(100*time.Millisecond), 3)})
			wg.Done()
			assert.NoError(t, err)
			defer lock2.Release(context.TODO())
			timeElapsed := time.Now().UnixMilli() - now
			assert.True(t, timeElapsed > 200)
			assert.True(t, 400 > timeElapsed)
		}()

		time.Sleep(200 * time.Millisecond)
		lock1.Release(context.TODO())
		time.Sleep(10 * time.Millisecond)
	})
}

func TestObtain_retry_failure(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		// hack
		SetSpinLockInterval(150 * time.Millisecond)
		defer func() {
			SetSpinLockInterval(120 * time.Second)
		}()

		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock1, err := Obtain(ctx, rc, "test1", 1000*time.Millisecond, &Options{IncrValue: 1024, LockId: "11"})
		assert.NoError(t, err)

		go func() {
			_, err := Obtain(ctx, rc, "test1", 1000*time.Millisecond, &Options{IncrValue: 1024, LockId: "11", RetryStrategy: LimitRetry(LinearBackoff(100*time.Millisecond), 3)})
			wg.Done()
			assert.Error(t, err)
		}()

		time.Sleep(200 * time.Millisecond)
		lock1.Release(context.TODO())
	})
}

func TestObtain_concurrent(t *testing.T) {
	ctx := NewSharedLockContext(context.TODO())
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	numLocks := int32(0)
	numThreads := 100
	wg := new(sync.WaitGroup)
	errs := make(chan error, numThreads)
	for i := 0; i < numThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wait := rand.Int63n(int64(40 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			_, err := Obtain(ctx, rc, lockKey, time.Minute, nil)
			if err == ErrNotObtained {
				return
			} else if err != nil {
				errs <- err
			} else {
				atomic.AddInt32(&numLocks, 1)
			}
		}()
	}
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if exp, got := 1, int(numLocks); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Refresh(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)

		lock, err := Obtain(ctx, rc, "refresh:slock:test1", time.Hour, nil)
		assert.NoError(t, err)
		defer lock.Release(context.TODO())

		// check TTL
		assertTTL(t, lock, time.Hour)

		// update TTL
		if err := lock.Refresh(ctx, time.Minute, nil); err != nil {
			t.Fatal(err)
		}
		// check TTL again
		assertTTL(t, lock, time.Minute)
	})
}

func TestLock_Refresh_expired(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		key := "refresh:expired:slock:test1"
		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)

		lock, err := Obtain(ctx, rc, key, 50*time.Millisecond, nil)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		err = lock.Refresh(context.TODO(), 50*time.Millisecond, nil)
		assert.Error(t, err)
	})

}

func TestLock_Release_expired(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		key := "release:expired:slock:test1"
		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)

		lock, err := Obtain(ctx, rc, key, 50*time.Millisecond, nil)
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		err = lock.Release(context.TODO())
		assert.Error(t, err)
	})

}

func TestLock_Release_not_own(t *testing.T) {
	t.Run("SLock", func(t *testing.T) {
		key := "release:not_own:slock:test1"
		ctx := NewSharedLockContext(context.TODO())
		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock, err := Obtain(ctx, rc, key, 100*time.Millisecond, nil)
		assert.NoError(t, err)

		// manual transfer ownership
		err = rc.HSet(ctx, key, "id", "abc").Err()
		assert.NoError(t, err)
		err = lock.Release(context.TODO())
		assert.Error(t, err)

		// release key
		err = rc.Del(ctx, key).Err()
		assert.NoError(t, err)
	})
}

func assertTTL(t *testing.T, lock ILock, exp time.Duration) {
	t.Helper()

	ttl, err := lock.TTL(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	delta := ttl - exp
	if delta < 0 {
		delta = 1 - delta
	}
	if delta > time.Second {
		t.Fatalf("expected ~%v, got %v", exp, ttl)
	}
}

func teardown(t *testing.T, rc *redis.Client) {
	t.Helper()

	if err := rc.Del(context.Background(), lockKey).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}
