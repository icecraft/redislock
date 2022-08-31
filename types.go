package redislock

import (
	"context"
	"time"
)

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
	MaxSpinInterval time.Duration
}

func (o *Options) getRetryStrategy() RetryStrategy {
	if o != nil && o.RetryStrategy != nil {
		return o.RetryStrategy
	}
	return NoRetry()
}

//
type ILock interface {
	Key() string
	Token() string
	Refresh(context.Context, time.Duration, *Options) error
	Release(context.Context) error
	TTL(context.Context) (time.Duration, error)
	IsSharedLock() bool
	IsReleased() bool
}
