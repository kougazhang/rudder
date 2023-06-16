package rudder

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

type TimRangeConfig struct {
	RedisAddr                    RedisAddr
	Unit, StartOffset, EndOffset string
}

func NewTimeRange(cfg *TimRangeConfig) (trange TimeRange, err error) {
	trange.addr = cfg.RedisAddr

	val, e := time.ParseDuration(cfg.Unit)
	if e != nil {
		err = e
		return
	}
	trange.unit = int64(val.Seconds())

	val, e = time.ParseDuration(cfg.StartOffset)
	if e != nil {
		err = e
		return
	}
	trange.startOffset = int64(val.Seconds())

	val, e = time.ParseDuration(cfg.EndOffset)
	if e != nil {
		err = e
		return
	}
	trange.endOffset = int64(val.Seconds())

	return
}

// TimeRange the range time of the task
type TimeRange struct {
	addr                         RedisAddr
	unit, startOffset, endOffset int64
}

func (t TimeRange) Race(ticket Ticket) (start, end int64, err error) {
	rds, err := t.addr.newClient()
	if err != nil {
		return
	}
	defer rds.Close()

	// assign the start
	start, err = t.incrby(t.startKey(ticket), t.unit)
	if err != nil {
		return
	}
	// assign the end
	// 1st query the end from redis
	if end, err = t.get(t.endKey(ticket)); err != nil && !errors.Is(err, redis.Nil) {
		return
	}
	// 2nd calculate the end by now
	if t.endOffset == 0 {
		err = fmt.Errorf("endOffset is 0")
		return
	}
	if end, err = t.offsetNow(t.endOffset); err != nil {
		return
	}
	return
}

func (t TimeRange) cleanRedis(ticket Ticket) error {
	rds, err := t.addr.newClient()
	if err != nil {
		return err
	}
	defer rds.Close()

	keys := []string{
		t.startKey(ticket),
		t.endKey(ticket),
		t.lockKey(t.startKey(ticket)),
		t.lockKey(t.endKey(ticket)),
	}
	_, err = rds.Del(keys...).Result()
	return err
}

func (t TimeRange) lockKey(flag string) string {
	return flag + ":lock"
}

func (t TimeRange) startKey(ticket Ticket) string {
	return string("rudder:" + ticket + ":start")
}

func (t TimeRange) endKey(ticket Ticket) string {
	return string("rudder:" + ticket + ":end")
}

func (t TimeRange) offsetNow(offset int64) (int64, error) {
	now := time.Now()
	minute, second := now.Minute(), now.Second()
	switch t.unit {
	case 300:
		return now.Unix()/300*300 - offset, nil
	case 3600:
		return now.Unix() - int64(second+minute*60) - offset, nil
	default:
		return 0, fmt.Errorf("unsupport unit %d", t.unit)
	}
}

func (t TimeRange) lock(flag string) error {
	key := t.lockKey(flag)
	rds, err := t.addr.newClient()
	if err != nil {
		return err
	}
	defer rds.Close()

	var maxErr int
	for maxErr < 10 {
		ok, err := rds.SetNX(key, 1, time.Second*3).Result()
		if err != nil && err != redis.Nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				maxErr++
				continue
			}
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(time.Millisecond * 200)
	}
	return nil
}

func (t TimeRange) unlock(flag string) error {
	key := flag + ":locker"
	rds, err := t.addr.newClient()
	if err != nil {
		return err
	}
	defer rds.Close()
	_, err = rds.Del(key).Result()
	return err
}

func (t TimeRange) get(key string) (int64, error) {
	rds, err := t.addr.newClient()
	if err != nil {
		return 0, err
	}
	defer rds.Close()
	return rds.Get(key).Int64()
}

func (t TimeRange) incrby(key string, increment int64) (int64, error) {
	lg := log.WithField("func", "TimeRange.incrby")
	if err := t.lock(key); err != nil {
		return 0, err
	}
	defer func() {
		if err := t.unlock(key); err != nil {
			lg.Errorf("%v", err)
		}
	}()
	rds, err := t.addr.newClient()
	if err != nil {
		return 0, err
	}
	defer rds.Close()

	start, err := rds.Get(key).Int64()
	if errors.Is(err, redis.Nil) {
		if t.startOffset == 0 {
			return 0, err
		}
		if nstart, err := t.offsetNow(t.startOffset); err != nil {
			return 0, err
		} else {
			start = nstart
		}
	}

	_, err = rds.Set(key, start+increment, time.Hour*24).Result()
	return start, err
}

func timeFormat(unix int64) string {
	return time.Unix(unix, 0).Format("2006-01-02T15-04")
}
