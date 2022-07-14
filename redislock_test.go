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
	ctx := context.Background()
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
	t.Run("obtain_non_reenterant_lock", func(t *testing.T) {
		ctx := context.Background()
		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock := quickObtain(t, rc, time.Hour)
		if err := lock.Release(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("obtain_reenterant_lock", func(t *testing.T) {
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

		err = lock2.Release(ctx)
		assert.NoError(t, err)

		err = lock1.Release(ctx)
		assert.NoError(t, err)

		// dup released
		err = lock1.Release(ctx)
		assert.Error(t, err)
	})

	t.Run("obtain_reenterant_lock_with_max_value", func(t *testing.T) {
		ctx := NewSharedLockContext(context.TODO())

		rc := redis.NewClient(redisOpts)
		defer teardown(t, rc)

		lock1, err := Obtain(ctx, rc, "testM", 10000000*time.Millisecond, &Options{IncrValue: MaxKeyValue})
		assert.NoError(t, err)

		_, err = Obtain(ctx, rc, "testM", 10000000*time.Millisecond, nil)
		assert.Error(t, err)

		err = lock1.Release(ctx)
		assert.NoError(t, err)

		lock2, err := Obtain(ctx, rc, "testM2", 1000000*time.Millisecond, nil)
		assert.NoError(t, err)

		err = lock2.Release(ctx)
		assert.NoError(t, err)

	})

}

func TestObtain_retry_success(t *testing.T) {
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	// obtain for 20ms
	lock1 := quickObtain(t, rc, 20*time.Millisecond)
	defer lock1.Release(ctx)

	// lock again with linar retry - 3x for 20ms
	lock2, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(20*time.Millisecond), 3),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lock2.Release(ctx)
}

func TestObtain_retry_failure(t *testing.T) {
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	// obtain for 50ms
	lock1 := quickObtain(t, rc, 200*time.Millisecond)
	defer lock1.Release(ctx)

	// lock again with linar retry - 2x for 5ms
	_, err := Obtain(ctx, rc, lockKey, time.Hour, &Options{
		RetryStrategy: LimitRetry(LinearBackoff(25*time.Millisecond), 2),
	})
	if exp, got := ErrNotObtained, err; !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestObtain_concurrent(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	lock := quickObtain(t, rc, time.Hour)
	defer lock.Release(ctx)

	// check TTL
	assertTTL(t, lock, time.Hour)

	// update TTL
	if err := lock.Refresh(ctx, time.Minute, nil); err != nil {
		t.Fatal(err)
	}

	// check TTL again
	assertTTL(t, lock, time.Minute)
}

func TestLock_Refresh_expired(t *testing.T) {
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	lock := quickObtain(t, rc, 40*time.Millisecond)
	defer lock.Release(ctx)

	// try releasing
	time.Sleep(50 * time.Millisecond)
	if exp, got := ErrNotObtained, lock.Refresh(ctx, time.Minute, nil); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_expired(t *testing.T) {
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	lock := quickObtain(t, rc, 40*time.Millisecond)
	defer lock.Release(ctx)

	// try releasing
	time.Sleep(40 * time.Millisecond)
	if exp, got := ErrLockNotHeld, lock.Release(ctx); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func TestLock_Release_not_own(t *testing.T) {
	ctx := context.Background()
	rc := redis.NewClient(redisOpts)
	defer teardown(t, rc)

	lock := quickObtain(t, rc, time.Hour)
	defer lock.Release(ctx)

	// manually transfer ownership
	if err := rc.Set(ctx, lockKey, "ABCD", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// try releasing
	if exp, got := ErrLockNotHeld, lock.Release(ctx); !errors.Is(got, exp) {
		t.Fatalf("expected %v, got %v", exp, got)
	}
}

func quickObtain(t *testing.T, rc *redis.Client, ttl time.Duration) *Lock {
	t.Helper()

	lock, err := Obtain(context.Background(), rc, lockKey, ttl, nil)
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func assertTTL(t *testing.T, lock *Lock, exp time.Duration) {
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
