package rudder

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

type (
	Ticket string
	Param  any
)

// Job runs a batch of task in the bucket.
type Job struct {
	// Bucket there are tasks in the bucket; Param is the param of the task.
	Bucket map[Ticket][]Param
	// Mode a job can have many modes.
	Mode Mode
	// Task runs a ticket fetching from the bucket.
	Task Task
	// BeforeRun runs a list of functions before the job
	BeforeRun []func(ctx context.Context) error
	// BeforeTaskRun runs a list of functions before per task
	BeforeTaskRun []func(ctx context.Context) error
	// AfterTaskRun runs a list of functions after per task
	// The lifetime of Task is as long as the job,
	// so it's necessary to clean to avoid leaked some variables specially in the cron mode.
	AfterTaskRun []func(ctx context.Context) error
	// Interval must be configured if cronMode is open
	Interval time.Duration
	// TimeRange is a range of time about a task
	TimeRange TimeRange
}

// Task handles a ticket
type Task interface {
	// Run the entrance of task
	Run(ctx context.Context, ticket Ticket, params []Param, start int64) error
}

// Run the job entrance
// notice: the race will increase the start even though the task was finished.
func (j Job) Run() error {
	lg := log.WithField("func", "Job.Run")
	ctx := context.Background()
	// before run
	if j.BeforeRun != nil {
		for _, fn := range j.BeforeRun {
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
	// run
	for {
		var err error
		if err = j.run(ctx); err != nil {
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

func (j Job) run(ctx context.Context) error {
	lg := log.WithField("func", "Job.run")
	for ticket, params := range j.Bucket {
		for {
			start, end, err := j.TimeRange.Race(ticket)
			if err != nil {
				return err
			}
			if start >= end {
				lg.Infof("start %s is equal or after end %s, stop to run", timeFormat(start), timeFormat(end))
				return nil
			}
			ctx = context.WithValue(ctx, JobCtx, j)
			ctx = context.WithValue(ctx, TaskUIDCtx, fmt.Sprintf("%s:%s", ticket, timeFormat(start)))
			// before task
			for _, fn := range j.BeforeTaskRun {
				if err := fn(ctx); err != nil {
					return err
				}
			}
			// run the task
			if err := j.Task.Run(ctx, ticket, params, start); err != nil {
				return err
			}
			// after task
			for _, fn := range j.AfterTaskRun {
				if err := fn(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
