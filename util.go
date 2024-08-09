package zencache

import (
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"runtime/debug"
	"time"
)

func now() int64 {
	return time.Now().Unix()
}

func runLua(ctx context.Context, rdb redis.Scripter, script *redis.Script, keys []string, args []interface{}) (interface{}, error) {
	r := script.EvalSha(ctx, rdb, keys, args)
	if redis.HasErrorPrefix(r.Err(), "NOSCRIPT") {
		if err := script.Load(ctx, rdb).Err(); err != nil {
			r = script.Eval(ctx, rdb, keys, args)
		} else {
			r = script.EvalSha(ctx, rdb, keys, args)
		}
	}
	v, err := r.Result()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	return v, err
}

func withRecover(f func()) {
	defer func() {
		if r := recover(); r != nil {
			debug.PrintStack()
		}
	}()
	f()
}
