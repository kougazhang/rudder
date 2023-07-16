package rudder

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	iredis "github.com/kougazhang/redis"
	itime "github.com/kougazhang/time"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// TimRangeConfig communicates task progress for difference processes
type TimRangeConfig struct {
	// RedisAddr is used for communicating progress for difference processes
	RedisAddr iredis.Addr
	// Unit the time of start goes foreword a unit
	Unit string
	// Dynamic is used of DynamicRace
	Dynamic *Dynamic
}

type Dynamic struct {
	// Offset the raw of time.Duration
	Offset string
}

func NewTimeRange(cfg *TimRangeConfig) (trange TimeRange, err error) {
	trange.addr = cfg.RedisAddr
	trange.unit, err = itime.ParseDuration(cfg.Unit)
	if err != nil {
		return
	}
	if cfg.Dynamic != nil {
		trange.dynamic.offset, err = itime.ParseDuration(cfg.Dynamic.Offset)
	}
	return
}

// TimeRange the range time of the task
type TimeRange struct {
	addr    iredis.Addr
	unit    time.Duration
	dynamic dynamic
}

type dynamic struct {
	offset time.Duration
}

func (t TimeRange) PushToQueue(ticket Ticket, jobStart int64) error {
	rds, err := t.addr.NewClient()
	if err != nil {
		return err
	}
	defer func() {
		_ = rds.Close()
	}()

	_, err = rds.RPush(t.TicketQueue(ticket), jobStart).Result()
	if err == nil || err == redis.Nil {
		return nil
	}
	return err
}

func (t TimeRange) PopFromQueue(ticket Ticket) (int64, error) {
	rds, err := t.addr.NewClient()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = rds.Close()
	}()

	return rds.LPop(t.TicketQueue(ticket)).Int64()
}

// DynamicRace Checks the current time meets the offset of start or not
// If true, ok is true
// If not, ok is false
// If the start didn't exist, calculate it by offset.
// bugfix: using jobStart to avoid if too many long time costed tickets, start calculated by offset is wrong.
func (t TimeRange) DynamicRace(ticket Ticket, jobStart int64) (start int64, ok bool, err error) {
	lg := log.WithField("func", "TimeRange.DynamicRace")
	rds, err := t.addr.NewClient()
	if err != nil {
		return
	}
	defer func() {
		_ = rds.Close()
	}()

	// lock
	key := t.startKey(ticket)
	lockKey := t.lockKey(key + ":dynamic")
	if err = t.lock(lockKey); err != nil {
		return
	}
	defer func() {
		if err := t.unlock(key); err != nil {
			lg.Errorf("%v", err)
		}
	}()

	// get start from redis or using jobStart
	start, err = t.get(key)
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return
		}
		// calculate it by offset if start does not exist in redis
		start = jobStart
	}

	// Checks the current time meets the offset of start or not
	now, err := t.offsetNow()
	if err != nil {
		return 0, false, err
	}
	// if true, increase the start
	if start <= now {
		ok = true
		// increase the start
		err = t.set(key, time.Unix(start, 0).Add(t.unit))
		if err != nil {
			err = fmt.Errorf("set: %w", err)
		}
		return
	}
	// If not, ok is false
	return
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
	start, err = t.incrby(t.startKey(ticket))
	if err != nil {
		return
	}
	// assign the end
	// 1st query the end from redis
	end, err = t.get(t.endKey(ticket))

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
	_, err = rds.Set(key, tm.Unix(), time.Hour*24*30).Result()
	return
}

func (t TimeRange) CleanRedis(ticket Ticket) error {
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
		t.TicketQueue(ticket),
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

func (t TimeRange) TicketQueue(ticket Ticket) string {
	return string("rudder:" + ticket + ":queue")
}

func (t TimeRange) endKey(ticket Ticket) string {
	return string("rudder:" + ticket + ":end")
}

func (t TimeRange) offsetNow() (int64, error) {
	now := time.Now()
	minute, second := now.Minute(), now.Second()
	offset := int64(t.dynamic.offset.Seconds())
	switch t.unit {
	case time.Minute * 5:
		return now.Unix()/300*300 - offset, nil
	case time.Hour:
		return now.Unix() - int64(second+minute*60) - offset, nil
	default:
		return 0, fmt.Errorf("unsupport unit %s", t.unit)
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

// incrby the key auto increases increment, and return the old value
// if the key didn't exist, return redis.Nil error
func (t TimeRange) incrby(key string) (old int64, err error) {
	lg := log.WithField("func", "TimeRange.incrby")
	lockKey := t.lockKey(key + ":incrby")
	if err := t.lock(lockKey); err != nil {
		err = fmt.Errorf("lock: %w", err)
		return 0, err
	}
	defer func() {
		if err := t.unlock(lockKey); err != nil {
			lg.Errorf("%v", err)
		}
	}()
	rds, err := t.addr.NewClient()
	if err != nil {
		err = fmt.Errorf("newClient: %w", err)
		return 0, err
	}
	defer func() {
		_ = rds.Close()
	}()

	old, err = rds.Get(key).Int64()
	if err != nil {
		err = fmt.Errorf("get key %s: %w", key, err)
		return
	}

	err = t.set(key, time.Unix(old, 0).Add(t.unit))
	if err != nil {
		err = fmt.Errorf("set: %w", err)
	}
	return
}

func timeFormat(unix int64) string {
	return time.Unix(unix, 0).Format("2006-01-02T15-04")
}
