package rudder

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	iredis "github.com/kougazhang/redis"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// TimRangeConfig communicates task progress for difference processes
type TimRangeConfig struct {
	// RedisAddr is used for communicating progress for difference processes
	RedisAddr iredis.Addr
	// Unit the time of start goes foreword a unit
	// StartOffset time.Now() add startOffset if start was not found in redis
	// EndOffset time.Now() add EndOffset if start was not found in redis
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
	addr                         iredis.Addr
	unit, startOffset, endOffset int64
}

func (t TimeRange) Race(ticket Ticket) (start, end int64, err error) {
	rds, err := t.addr.NewClient()
	if err != nil {
		return
	}
	defer func() {
		_ = rds.Close()
	}()

	// assign the start
	start, err = t.incrby(t.startKey(ticket), t.unit)
	if err != nil {
		return
	}
	// assign the end
	// 1st query the end from redis
	end, err = t.get(t.endKey(ticket))
	if err == nil {
		return
	}
	if !errors.Is(err, redis.Nil) {
		log.Debugf("the end key of job %s", t.endKey(ticket))
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

// SetStart sets start to redis
func (t TimeRange) SetStart(ticket Ticket, start time.Time) (err error) {
	return t.set(t.startKey(ticket), start)
}

// SetEnd sets end to redis
func (t TimeRange) SetEnd(ticket Ticket, end time.Time) (err error) {
	return t.set(t.endKey(ticket), end)
}

func (t TimeRange) set(key string, tm time.Time) (err error) {
	rds, err := t.addr.NewClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = rds.Close()
	}()
	_, err = rds.Set(key, tm.Unix(), time.Hour*24).Result()
	return
}

func (t TimeRange) cleanRedis(ticket Ticket) error {
	rds, err := t.addr.NewClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = rds.Close()
	}()

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
	rds, err := t.addr.NewClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = rds.Close()
	}()

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
	key := t.lockKey(flag)
	rds, err := t.addr.NewClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = rds.Close()
	}()
	_, err = rds.Del(key).Result()
	return err
}

func (t TimeRange) get(key string) (int64, error) {
	rds, err := t.addr.NewClient()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rds.Close()
	}()
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
	rds, err := t.addr.NewClient()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rds.Close()
	}()

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
