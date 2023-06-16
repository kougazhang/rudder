package rudder

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

type (
	Ticket string
	Pvd    string
)

// Job runs a batch of task in the bucket.
type Job struct {
	// Bucket there are tasks in the bucket; Pvd is the data source of the task.
	Bucket map[Ticket][]Pvd
	// Mode a job can have many modes.
	Mode Mode
	// Task runs a ticket fetching from the bucket.
	Task Task
	// BeforeRun runs a list of functions before the job
	BeforeRun []func() error
	// Interval must be configured if cronMode is open
	Interval time.Duration
	// TimeRange is a range of time about a task
	TimeRange TimeRange
}

// Task handles a period time of a ticket
type Task interface {
	// Run the entrance of task
	Run(ctx context.Context, ticket Ticket, pvds []Pvd, start, unit int64) error
}

// Run the job entrance
func (j Job) Run() error {
	lg := log.WithField("func", "Job.Run")
	// before run
	if j.BeforeRun != nil {
		for _, fn := range j.BeforeRun {
			if err := fn(); err != nil {
				return err
			}
		}
	}
	// run
	for {
		var err error
		if err = j.run(); err != nil {
			lg.Errorf("%v", err)
		}
		if j.Mode.HasMode(CronMode) {
			lg.Infof("run next task after %s", j.Interval)
			time.Sleep(j.Interval)
		} else {
			return err
		}
	}
}

const (
	JobCtx     = "job"
	TaskUIDCtx = "taskUID"
)

func (j Job) run() error {
	lg := log.WithField("func", "Job.run")
	for ticket, pvds := range j.Bucket {
		for {
			start, end, err := j.TimeRange.Race(ticket)
			if err != nil {
				return err
			}
			if start >= end {
				lg.Infof("start %s is equal or after end %s, stop to run", timeFormat(start), timeFormat(end))
				return nil
			}
			// run the task
			ctx := context.WithValue(context.Background(), JobCtx, j)
			ctx = context.WithValue(ctx, TaskUIDCtx, fmt.Sprintf("%s:%s", ticket, timeFormat(start)))
			if err := j.Task.Run(ctx, ticket, pvds, start, j.TimeRange.unit); err != nil {
				return err
			}
		}
	}
	return nil
}
