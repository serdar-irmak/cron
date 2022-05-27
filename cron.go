package cron

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries        EntryHeap
	chain          Chain
	stop           chan struct{}
	add            chan *Entry
	remove         chan EntryID
	scheduleUpdate chan scheduleUpdateInfo
	snapshot       chan chan []Entry
	running        bool
	logger         Logger
	runningMu      sync.Mutex
	location       *time.Location
	parser         ScheduleParser
	jobWaiter      sync.WaitGroup
	context        context.Context
	idGenerator    IDGenerator
}

type IDGenerator interface {
	Generator() EntryID
}

type IntIdGenerator struct {
	nextID int
}

func (r *IntIdGenerator) Generator() EntryID {
	r.nextID++
	return r.nextID
}

// scheduleUpdateInfo encapsulates the information required to update the
// schedule of a job.
type scheduleUpdateInfo struct {
	id       EntryID
	schedule Schedule
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

type JobOption struct {
	id      EntryID
	Context context.Context
}

func (jo JobOption) GetID() EntryID {
	return jo.id
}

type OptionJob interface {
	Job
	RunWithOption(*JobOption)
}

// Schedule describes a job's duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time

	// Prev returns the previous activation time, earlier than the given time.
	Prev(time.Time) time.Time
	IsOnce() bool
}

// EntryID identifies an entry within a Cron instance
type EntryID interface{}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// ID is the cron-assigned ID of this entry, which may be used to look up a
	// snapshot or remove it.
	ID EntryID

	// Schedule on which this job should be run.
	Schedule Schedule

	// Next time the job will run, or the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// Prev is the last time this job was run, or the zero time if never.
	Prev time.Time

	// WrappedJob is the thing to run when the Schedule is activated.
	WrappedJob Job

	// Job is the thing that was submitted to cron.
	// It is kept around so that user code that needs to get at the job later,
	// e.g. via Entries() can do so.
	Job Job

	// Paused is a flag to indicate that whether the job is currently paused.
	Paused   bool
	PausedMu sync.Mutex

	// cancelContext is used cancel for current job work
	Timeout    time.Duration
	cancelFunc context.CancelFunc
}

// Valid returns true if this is not the zero entry.
func (e Entry) Valid() bool { return e.ID != 0 }

// ScheduleFirst is used for the initial scheduling. If a Prev value has been
// included with the Entry, it will be used in place of "now" to allow schedules
// to be preserved across process restarts.
func (e Entry) ScheduleFirst(now time.Time) time.Time {
	if !e.Prev.IsZero() {
		return e.Schedule.Next(e.Prev)
	} else {
		return e.Schedule.Next(now)
	}
}

func (e *Entry) isPaused() bool {
	e.PausedMu.Lock()
	defer e.PausedMu.Unlock()
	return e.Paused
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner, modified by the given options.
//
// Available Settings
//
//   Time Zone
//     Description: The time zone in which schedules are interpreted
//     Default:     time.Local
//
//   Parser
//     Description: Parser converts cron spec strings into cron.Schedules.
//     Default:     Accepts this spec: https://en.wikipedia.org/wiki/Cron
//
//   Chain
//     Description: Wrap submitted jobs to customize behavior.
//     Default:     A chain that recovers panics and logs them to stderr.
//
// See "cron.With*" to modify the default behavior.
func New(opts ...Option) *Cron {
	c := &Cron{
		entries:        nil,
		chain:          NewChain(),
		add:            make(chan *Entry),
		stop:           make(chan struct{}),
		snapshot:       make(chan chan []Entry),
		remove:         make(chan EntryID),
		scheduleUpdate: make(chan scheduleUpdateInfo),
		running:        false,
		runningMu:      sync.Mutex{},
		logger:         DefaultLogger,
		location:       time.Local,
		parser:         standardParser,
		context:        context.Background(),
		idGenerator:    &IntIdGenerator{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func()
type FuncOptionJob func(*JobOption)

// Run will satisfies interface declaration
func (f FuncJob) Run() { f() }

func (f FuncOptionJob) Run()                       { f(&JobOption{}) }
func (f FuncOptionJob) RunWithOption(o *JobOption) { f(o) }

// AddFunc adds a func to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddFunc(spec string, cmd func(), entryOpts ...EntryOption) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd), entryOpts...)
}

func (c *Cron) AddOptionFunc(spec string, cmd func(*JobOption), entryOpts ...EntryOption) (EntryID, error) {
	return c.AddOptionJob(spec, FuncOptionJob(cmd), entryOpts...)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
// The spec is parsed using the time zone of this Cron instance as the default.
// An opaque ID is returned that can be used to later remove it.
func (c *Cron) AddJob(spec string, cmd Job, entryOpts ...EntryOption) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)

	if err != nil {
		return 0, err
	}
	return c.Schedule(schedule, cmd, entryOpts...), nil
}

func (c *Cron) AddOptionJob(spec string, cmd OptionJob, entryOpts ...EntryOption) (EntryID, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return 0, err
	}
	return c.ScheduleOptionJob(schedule, cmd, entryOpts...), nil
}

func (c *Cron) ScheduleOptionJob(schedule Schedule, cmd OptionJob, entryOpts ...EntryOption) EntryID {
	return c.Schedule(schedule, cmd, entryOpts...)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job, entryOpts ...EntryOption) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	entry := &Entry{
		Schedule:   schedule,
		WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
		Paused:     false,
	}
	for _, fn := range entryOpts {
		fn(entry)
	}
	if entry.ID == nil {
		entry.ID = c.idGenerator.Generator()
	}
	if !c.running {
		heap.Push(&c.entries, entry)
	} else {
		c.add <- entry
	}
	return entry.ID
}

// UpdateScheduleWithSpec updates the schedule of the job, corresponding to the given id,
// with the provided spec string.
func (c *Cron) UpdateScheduleWithSpec(id EntryID, spec string) error {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return errors.New(fmt.Sprint("could not update schedule for job: ", err))
	}
	return c.UpdateSchedule(id, schedule)
}

// UpdateSchedule updates the schedule of a job, corresponding to the given id,
// with the provided schedule.
func (c *Cron) UpdateSchedule(id EntryID, schedule Schedule) error {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	for _, entry := range c.entries {
		if entry.ID == id {
			if c.running {
				c.scheduleUpdate <- scheduleUpdateInfo{
					id:       id,
					schedule: schedule,
				}
			} else {
				entry.Schedule = schedule
				// Updating entry.Next as it might now be stale.
				entry.Next = entry.Schedule.Next(c.now())
			}
			return nil
		}
	}
	return errors.New(fmt.Sprintf("invalid ID provided: %d", id))
}

// EntryOption is a hook which allows the Entry to be altered before being
// committed internally.
type EntryOption func(*Entry)

// EntryPrev allows setting the Prev time to allow interval-based schedules to
// preserve their timeline even in the face of process restarts.
func WithPrev(prev time.Time) EntryOption {
	return func(e *Entry) {
		e.Prev = prev
	}
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		replyChan := make(chan []Entry, 1)
		c.snapshot <- replyChan
		return <-replyChan
	}
	return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// timeTillEarliestEntry returns the time remaining until the first
// execution of the earliest entry to run.
func (c *Cron) timeTillEarliestEntry(now time.Time) time.Duration {
	if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
		// If there are no entries yet, just sleep - it still handles new entries
		// and stop requests.
		return 100000 * time.Hour
	}
	return c.entries[0].Next.Sub(now)
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	sortedEntries := new(EntryHeap)
	for len(c.entries) > 0 {
		entry := heap.Pop(&c.entries).(*Entry)
		entry.Next = entry.ScheduleFirst(now)
		heap.Push(sortedEntries, entry)
		c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	c.entries = *sortedEntries

	for {
		// timer to timeout when it is time for the next entry to run.
		timer := time.NewTimer(c.timeTillEarliestEntry(now))

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
				c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for {
					e := c.entries.Peek()
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}

					if e.isPaused() {
						// Updating Next and Prev so that the schedule continues to be maintained.
						// This will help us proceed once the job is continued.
						e = heap.Pop(&c.entries).(*Entry)
						e.Prev = e.Next
						e.Next = e.Schedule.Next(now)
						c.logger.Info("paused,skip that schedule", "now", now, "entry", e.ID, "next", e.Next)
						heap.Push(&c.entries, e)
						continue
					}

					e = heap.Pop(&c.entries).(*Entry)

					c.startJob(e.WrappedJob, e)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
					c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
					if !e.Schedule.IsOnce() {
						heap.Push(&c.entries, e)
					} else {
						c.logger.Info("job run once,not add again", "now", now, "entry", e.ID, "once", e.Schedule.IsOnce())
					}
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.ScheduleFirst(now)
				heap.Push(&c.entries, newEntry)
				c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

			case <-c.stop:
				timer.Stop()
				c.logger.Info("stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
				c.logger.Info("removed", "entry", id)

			case newScheduleUpdateInfo := <-c.scheduleUpdate:
				now = c.now()
				// Update the next execution time of the entry using the updated schedule.
				scheduleUpdated := false
				now = c.now()
				// Update the next execution time of the entry using the updated schedule.
				var scheduleUpdatedEntry *Entry

				for _, e := range c.entries {
					if e.ID == newScheduleUpdateInfo.id {
						e.Schedule = newScheduleUpdateInfo.schedule
						e.Next = e.Schedule.Next(now)
						scheduleUpdatedEntry = e
						scheduleUpdated = true
						break
					}
				}

				// entries[0] might no longer correspond to the next entry to run.
				// Reinitialize the timer.
				if scheduleUpdated {
					timer.Stop()
					now = c.now()
					c.removeEntry(newScheduleUpdateInfo.id)
					heap.Push(&c.entries, scheduleUpdatedEntry)
					c.logger.Info("updated", "entry", scheduleUpdatedEntry.ID, "next", scheduleUpdatedEntry.Next)
					timer = time.NewTimer(c.timeTillEarliestEntry(now))
				}
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(j Job, entry *Entry) {
	c.jobWaiter.Add(1)
	ctx := c.generateCancelSignalContext(entry)
	go func() {
		defer c.jobWaiter.Done()

		switch j := j.(type) {
		case OptionJob:
			j.RunWithOption(&JobOption{
				entry.ID,
				ctx,
			})
			entry.cancelFunc = nil
		case Job:
			j.Run()
		}
	}()
}

func (c *Cron) CancelEntry(id EntryID) {
	entry := c.Entry(id)
	c.logger.Info("cancel", "id", id)

	if entry.cancelFunc != nil {
		entry.cancelFunc()
	}
}

func (c *Cron) generateCancelSignalContext(e *Entry) context.Context {
	var ctx context.Context
	var cancelFunc context.CancelFunc
	if e.Timeout == 0 {
		ctx, cancelFunc = context.WithCancel(c.context)
	} else {
		ctx, cancelFunc = context.WithTimeout(c.context, e.Timeout)
	}
	e.cancelFunc = cancelFunc
	return ctx

}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// Stop stops the cron scheduler if it is running; otherwise it does nothing.
// A context is returned so the caller can wait for running jobs to complete.
func (c *Cron) Stop() context.Context {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.stop <- struct{}{}
		c.running = false
	}
	ctx, cancel := context.WithCancel(c.context)
	go func() {
		c.jobWaiter.Wait()
		cancel()
	}()
	return ctx
}

// Pause the cron job corresponding to the given id.
// This would result in a no-op if the job is currently paused.
func (c *Cron) Pause(id EntryID) error {
	var validId = false
	for _, entry := range c.entries {
		if entry.ID == id {
			entry.PausedMu.Lock()
			entry.Paused = true
			validId = true
			entry.PausedMu.Unlock()
			break
		}
	}
	if !validId {
		return errors.New("invalid entry id")
	}
	return nil
}

// Continue the cron job corresponding to the given id.
// This would result in a no-op if the job is not currently in a paused state.
func (c *Cron) Continue(id EntryID) error {
	var validId = false
	for _, entry := range c.entries {
		if entry.ID == id {
			entry.PausedMu.Lock()
			entry.Paused = false
			validId = true
			entry.PausedMu.Unlock()
			break
		}
	}
	if !validId {
		return errors.New("invalid entry id")
	}
	return nil
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

// removeEntry removes the entry corresponding to the given ID from the entry list.
func (c *Cron) removeEntry(id EntryID) {
	for idx, e := range c.entries {
		if e.ID == id {
			heap.Remove(&c.entries, idx)
			return
		}
	}
}

func (c *Cron) hasEntry(id EntryID) bool {

	for _, e := range c.entries {
		if e.ID == id {
			return true
		}
	}

	return false
}
