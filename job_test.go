package rudder

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"testing"
	"time"
)

type task struct{}

func (t task) Run(ctx context.Context, domain Ticket, pvds []Param, start int64) error {
	taskUID, ok := ctx.Value(TaskUIDCtx).(string)
	if !ok {
		log.Fatal("taskUID is invalid")
	}
	log.Println("run domain: ", domain, taskUID, start)
	return nil
}

func TestJob_Run(t *testing.T) {
	trange := TimeRange{
		addr: "127.0.0.1:6379/0",
		unit: 5 * time.Minute,
		dynamic: dynamic{
			offset: time.Second,
		},
	}
	log.SetLevel(log.DebugLevel)

	var domain Ticket = "www.baidu.com"
	if err := trange.CleanRedis(domain); err != nil {
		log.Fatal(err)
	}
	if err := trange.SetStart(domain, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := trange.SetEnd(domain, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	job := Job{
		Bucket: map[Ticket][]Param{
			domain: nil,
		},
		Task:      task{},
		Interval:  time.Second * 5,
		TimeRange: trange,
		//IsFixedTime: false,
		IsFixedTime: true,
		//Mode:        CronMode,
	}
	job.BeforeTaskRun = []TaskRunFn{
		job.ConsumeState,
		job.AddState,
	}
	job.AfterTaskRun = []TaskRunFn{
		job.DelState,
	}
	job.AfterJobRun = []func(ctx context.Context) error{
		func(ctx context.Context) error {
			job.Bucket["www.hello.world"] = nil
			log.Println("do the AfterJobRun")
			return nil
		},
	}
	err := job.TimeRange.PushToQueue(domain, time.Now().Add(time.Hour*24).Unix())
	if err != nil {
		t.Fatal(err)
	}
	if err := job.Run(); err != nil {
		t.Fatal(err)
	}
	if err := trange.CleanRedis(domain); err != nil {
		log.Fatal(err)
	}
}

func TestNewTimeRange(t *testing.T) {
	trange, err := NewTimeRange(&TimRangeConfig{
		Unit: "5m",
		Dynamic: &Dynamic{
			Offset: "0",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(trange)
}
