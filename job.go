// Package rudder contains some important conceptions like Job, Task etc.
// Job runs a batch of task in a range of time in the bucket.
// Task handles a ticket of specified time point start
package rudder

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
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
	BeforeTaskRun []TaskRunFn
	// AfterTaskRun runs a list of functions after per task
	// The lifetime of Task is as long as the job,
	// so it's necessary to clean to avoid leaked some variables specially in the cron mode.
	AfterTaskRun []TaskRunFn
	// Interval must be configured if cronMode is open
	Interval time.Duration
	// TimeRange is a range of time about a task
	TimeRange TimeRange
	// IsFixedTime the timeRange of the job is fixed or endless
	// case 1: start and fix are fixed
	// case 2: generate start and end dynamically by delay
	IsFixedTime bool
}

type TaskRunFn func(ctx context.Context, ticket Ticket, params []Param, start int64) error

// Task handles a ticket of specified time point start
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
func (j Job) run(ctx context.Context) error {
	lg := log.WithField("func", "Job.run")
	jobStart, err := j.TimeRange.offsetNow()
	if err != nil {
		return err
	}
	lg.Debugf("fixedTime is %v", j.IsFixedTime)
	for ticket, params := range j.Bucket {
		if err = j.runTicket(ctx, ticket, params, jobStart); err != nil {
			lg.Errorf("ticket:%v, err %v", ticket, err)
		}
		// run the specified time points
		if err = j.runTicketFromQueue(ctx, ticket, params); err != nil {
			lg.Errorf("runTicketFromQueue:%v, err %v", ticket, err)
		}
	}
	return nil
}

// runTicketFromQueue runs the specified time points and ticket
func (j Job) runTicketFromQueue(ctx context.Context, ticket Ticket, params []Param) (err error) {
	lg := log.WithField("func", "Job.runTicketFromQueue")
	lg = lg.WithField("ticket", ticket)

	for {
		// get specified starts from redis
		start, err := j.TimeRange.PopFromTicketQueue(ticket)
		if err != nil {
			if err == redis.Nil {
				lg.Debugf("the queue %s is empty", j.TimeRange.ticketQueue(ticket))
				return nil
			}
			return err
		}
		if start == 0 {
			return nil
		}
		// run the task
		ctx = j.setTaskUID(ctx, ticket, start)
		lg.Infof("runTicketFromQueue: taskUID %s", ctx.Value(TaskUIDCtx))
		if err := j.Task.Run(ctx, ticket, params, start); err != nil {
			return err
		}
	}
}

func (j Job) runTicket(ctx context.Context, ticket Ticket, params []Param, jobStart int64) (err error) {
	lg := log.WithField("func", "Job.ticket")
	lg = lg.WithField("ticket", ticket)

	var start int64
	for {
		if j.IsFixedTime {
			start, err = j.fixedTimeStart(ticket)
			if err != nil {
				// here is breaking the dead loop  in fix start
				if err == completedErr {
					lg.Infof("the job is completed, start %s, stop to run", timeFormat(start))
					return nil
				}
				return err
			}
		} else {
			start, err = j.dynamicTimeStart(ticket, jobStart)
			if err != nil {
				// here is breaking the dead loop in dynamic start
				if err == timeIsEarlyErr {
					lg.Infof("the start %s is too early, continue to wait", timeFormat(start))
					return nil
				}
				return err
			}
		}
		// before task
		for _, fn := range j.BeforeTaskRun {
			if err := fn(ctx, ticket, params, start); err != nil {
				return err
			}
		}
		// run the task
		ctx = context.WithValue(ctx, JobCtx, j)
		ctx = j.setTaskUID(ctx, ticket, start)
		if err := j.Task.Run(ctx, ticket, params, start); err != nil {
			return err
		}
		// after task
		for _, fn := range j.AfterTaskRun {
			if err := fn(ctx, ticket, params, start); err != nil {
				return err
			}
		}
	}
}

// SetState adds the lasting state before task running
// SetState consumes the existed state of the task
func (j Job) SetState(ctx context.Context, ticket Ticket, params []Param, jobStart int64) error {
	// add new data to queue
	ticketParam := TicketParam{
		Ticket:      ticket,
		Params:      params,
		JobStart:    jobStart,
		MarshalTime: time.Now(),
	}
	log.Debugf("setState: ticketParam %v", ticketParam)
	if err := j.TimeRange.rpushToStateQueue(ticketParam); err != nil {
		return err
	}
	// the length of 1 means no expired data
	length, err := j.TimeRange.lenStateQueue(ticket)
	if err != nil {
		return err
	}
	if length <= 1 {
		return nil
	}
	// consume the expired data
	return j.consumeState(ctx, ticket)
}

// DelState deletes the state added before the task running
func (j Job) DelState(_ context.Context, ticket Ticket, _ []Param, _ int64) error {
	_, err := j.TimeRange.rpopFromStateQueue(ticket)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil
		}
		return err
	}
	return err
}

// consumeState consumes the existed state of the task
// The expiration of task state is 12 hours
func (j Job) consumeState(ctx context.Context, ticket Ticket) error {
	var index int64
	for {
		param, err := j.TimeRange.lindexStateQueue(ticket, index)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return err
		}
		// drop the expired state
		if time.Now().Sub(param.MarshalTime) >= time.Hour*12 {
			log.Infof("consumeState: drop expire task state %v", param)
			_, err = j.TimeRange.lpopFromStateQueue(ticket)
			if err != nil {
				return err
			}
			continue
		}
		// consume the task
		ctx = j.setTaskUID(ctx, param.Ticket, param.JobStart)
		log.Infof("consumeState: consume task, taskUID %s", ctx.Value(TaskUIDCtx).(string))
		err = j.Task.Run(ctx, param.Ticket, param.Params, param.JobStart)
		if err != nil {
			return err
		}
		// pop it from the queue
		_, err = j.TimeRange.lpopFromStateQueue(ticket)
		if err != nil {
			return err
		}
	}
}

func (j Job) setTaskUID(ctx context.Context, ticket Ticket, start int64) context.Context {
	return context.WithValue(ctx, TaskUIDCtx, fmt.Sprintf("%s:%s", ticket, timeFormat(start)))
}

var (
	completedErr   = errors.New("reach the end")
	timeIsEarlyErr = errors.New("the time is too early")
)

func (j Job) dynamicTimeStart(ticket Ticket, jobStart int64) (start int64, err error) {
	var ok bool
	start, ok, err = j.TimeRange.DynamicRace(ticket, jobStart)
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

// TicketParam is the params of method Job.ticket
type TicketParam struct {
	Ticket   Ticket  `json:"ticket"`
	Params   []Param `json:"params"`
	JobStart int64   `json:"job_start"`
	// MarshalTime is only used for state task
	MarshalTime time.Time `json:"marshal_time"`
}
