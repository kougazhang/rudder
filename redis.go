package rudder

import (
	"fmt"
	"github.com/go-redis/redis"
	"strconv"
	"strings"
	"time"
)

type RedisAddr string

func (r RedisAddr) newClient() (*redis.Client, error) {
	split := strings.Split(string(r), "/")
	if len(split) != 2 {
		return nil, fmt.Errorf("invalid format %s", r)

	}
	addr, raw := split[0], split[1]
	db, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil, err
	}

	return redis.NewClient(&redis.Options{
		Addr:            addr,
		DB:              int(db),
		MaxRetries:      5,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: time.Second,
	}), nil
}
