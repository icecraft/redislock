package redislock

import (
	"errors"

	"github.com/go-redis/redis/v8"
)

const (
	sharedDisLock         = "shared_dis_lock"
	sharedDisLockCtxValue = "yes"

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
)

// Lock related lua scripts
var (
	luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

// SLock related lua scripts
var (
	sLuaPTTL = redis.NewScript(`
		redis.replicate_commands()
	
		local key = KEYS[1]
		local key_id = KEYS[2]
		local key_count = KEYS[3]
		local key_id_value = ARGV[1]

		
		local is_count_existed = redis.call("HEXISTS", key, key_count)
		local is_id_existed  = redis.call("HEXISTS", key, key_id)
		
		if is_count_existed == 1 and is_id_existed == 1 then
			if redis.call("HGET", key, key_id) == key_id_value then 
				return redis.call("PTTL", key) 
			else 
				return -100
			end 
		end
		return -200
`)

	sLuaRefresh = redis.NewScript(`
		redis.replicate_commands()
	
		local key = KEYS[1]
		local key_id = KEYS[2]
		local key_count = KEYS[3]
		local key_id_value = ARGV[1]
		local refresh_time = ARGV[2]
		
		local is_count_existed = redis.call("HEXISTS", key, key_count)
		local is_id_existed  = redis.call("HEXISTS", key, key_id)
		
		if is_count_existed == 1 and is_id_existed == 1 then
			if redis.call("HGET", key, key_id) == key_id_value then 
				return redis.call("PEXPIRE", key, refresh_time)
			else 
				return 1
			end 
		end
		return 0
`)

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
			redis.call("HSET", "ret_code_3", "debug_current_count", count_value)
			redis.call("HSET", "ret_code_3", "debug_incrby", incr_key_count_value)
			return 3
		end

		redis.call("HINCRBY", key, key_count, incr_key_count_value)

		if expired_time > 0 then 
			redis.call("PEXPIRE", key, expired_time)
		end
	
		local current_count  = redis.call("HGET", key, key_count)

		if current_count == "0" then
			redis.call("DEL", key)
		end

	elseif is_count_existed == 0 and is_id_existed == 0 then
		redis.call("HSET", key, key_count, incr_key_count_value)
		redis.call("HSET", key, key_id, key_id_value)
		redis.call("PEXPIRE", key, expired_time)
	else
		return 4
	end
	
	return 0
`)
)
