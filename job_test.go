package rudder

import (
	"context"
	"log"
	"testing"
	"time"
)

type task struct{}

func (t task) Run(ctx context.Context, domain Ticket, pvds []Param, start int64) error {
	taskUID, ok := ctx.Value(TaskUIDCtx).(string)
	if !ok {
		log.Fatal("taskUID is invalid")
	}
	log.Println("run domain: ", domain, taskUID)
	return nil
}

func TestJob_Run(t *testing.T) {
	trange := TimeRange{
		addr: "127.0.0.1:6379/0",
		unit: 300,
		dynamic: dynamic{
			offset: time.Second,
		},
	}

	var domain Ticket = "www.baidu.com"
	if err := trange.CleanRedis(domain); err != nil {
		log.Fatal(err)
	}
	if err := trange.SetStart(domain, time.Now()); err != nil {
		t.Fatal(err)
	}
	job := Job{
		Bucket: map[Ticket][]Param{
			domain: nil,
		},
		Task:      task{},
		Interval:  time.Second,
		TimeRange: trange,
	}
	if err := job.Run(); err != nil {
		t.Fatal(err)
	}
	if err := trange.CleanRedis(domain); err != nil {
		log.Fatal(err)
	}
}
