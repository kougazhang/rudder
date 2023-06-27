package rudder

import (
	"context"
	"errors"
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
	// IsFixedTime the timeRange of the job is fixed or endless
	// case 1: start and fix are fixed
	// case 2: generate start and end dynamically by delay
	IsFixedTime bool
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

func (j Job) run(ctx context.Context) (err error) {
	lg := log.WithField("func", "Job.dynamicTimeRun")
	var start int64
	for ticket, params := range j.Bucket {
		for {
			if j.IsFixedTime {
				start, err = j.fixedTimeStart(ticket)
				if err != nil {
					if err == completedErr {
						lg.Infof("the job is completed, start %s, stop to run", timeFormat(start))
						return nil
					}
					return err
				}
			} else {
				start, err = j.dynamicTimeStart(ticket)
				if err != nil {
					if err == timeIsEarlyErr {
						lg.Infof("the start %s is too early, continue to wait", timeFormat(start))
						return nil
					}
					return err
				}
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

var (
	completedErr   = errors.New("reach the end")
	timeIsEarlyErr = errors.New("the time is too early")
)

func (j Job) dynamicTimeStart(ticket Ticket) (start int64, err error) {
	var ok bool
	start, ok, err = j.TimeRange.DynamicRace(ticket)
	if err != nil {
		return
	}
	if !ok {
		err = timeIsEarlyErr
		return
	}
	return
}

func (j Job) fixedTimeStart(ticket Ticket) (start int64, err error) {
	var end int64
	start, end, err = j.TimeRange.Race(ticket)
	if err != nil {
		return
	}
	if start >= end {
		err = completedErr
		return
	}
	return
}
