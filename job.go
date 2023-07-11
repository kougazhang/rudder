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
	// InitJob runs a list of functions before the job
	// InitJob just does once.
	InitJob []func(ctx context.Context) error
	// BeforeJobRun does each period before the job run.
	BeforeJobRun []func(ctx context.Context) error
	// AfterJobRun does each period after the job run.
	AfterJobRun []func(ctx context.Context) error
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
	if j.InitJob != nil {
		for _, fn := range j.InitJob {
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
	// run
	for { // for cron
		var err error
		// before the job
		if j.BeforeJobRun != nil {
			for _, fn := range j.BeforeJobRun {
				if err := fn(ctx); err != nil {
					return err
				}
			}
		}
		// run the job
		if err = j.run(ctx); err != nil {
			lg.Errorf("%v", err)
		}
		// after the job
		if j.AfterJobRun != nil {
			for _, fn := range j.AfterJobRun {
				if err := fn(ctx); err != nil {
					return err
				}
			}
		}
		if j.Mode.HasMode(CronMode) {
			lg.Infof("run the job after %s", j.Interval)
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

// run prefers ticket than time first
func (j Job) run(ctx context.Context) (err error) {
	lg := log.WithField("func", "Job.run")
	for ticket, params := range j.Bucket {
		if err = j.ticket(ctx, ticket, params); err != nil {
			lg.Errorf("ticket:%v, err %v", ticket, err)
		}
	}
	return nil
}

func (j Job) ticket(ctx context.Context, ticket Ticket, params []Param) (err error) {
	lg := log.WithField("func", "Job.ticket")
	lg = lg.WithField("ticket", ticket)

	var start int64
	for {
		if j.IsFixedTime {
			lg.Infof("try to get fixedTime start")
			start, err = j.fixedTimeStart(ticket)
			if err != nil {
				if err == completedErr {
					lg.Infof("the job is completed, start %s, stop to run", timeFormat(start))
					return nil
				}
				return err
			}
		} else {
			lg.Infof("try to get dynamicTime start")
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

var (
	completedErr   = errors.New("reach the end")
	timeIsEarlyErr = errors.New("the time is too early")
)

func (j Job) dynamicTimeStart(ticket Ticket) (start int64, err error) {
	var ok bool
	start, ok, err = j.TimeRange.DynamicRace(ticket)
	if err != nil {
		err = fmt.Errorf("dynamicRace: %w", err)
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
