package cron

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const OneSecond = 1*time.Second + 50*time.Millisecond

type testIdGenerator struct {
	test string
}

func (t *testIdGenerator) Generator() EntryID {
	return t.test
}

type syncWriter struct {
	wr bytes.Buffer
	m  sync.Mutex
}

func (sw *syncWriter) Write(data []byte) (n int, err error) {
	sw.m.Lock()
	n, err = sw.wr.Write(data)
	sw.m.Unlock()
	return
}

func (sw *syncWriter) String() string {
	sw.m.Lock()
	defer sw.m.Unlock()
	return sw.wr.String()
}

func newBufLogger(sw *syncWriter) Logger {
	return PrintfLogger(log.New(sw, "", log.LstdFlags))
}

func TestFuncPanicRecovery(t *testing.T) {
	var buf syncWriter
	cron := New(WithParser(secondParser),
		WithChain(Recover(newBufLogger(&buf))))
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() {
		panic("YOLO")
	})

	select {
	case <-time.After(OneSecond):
		if !strings.Contains(buf.String(), "YOLO") {
			t.Error("expected a panic to be logged, got none")
		}
		return
	}
}

type DummyJob struct{}

func (d DummyJob) Run() {
	panic("YOLO")
}

func TestJobPanicRecovery(t *testing.T) {
	var job DummyJob

	var buf syncWriter
	cron := New(WithParser(secondParser),
		WithChain(Recover(newBufLogger(&buf))))
	cron.Start()
	defer cron.Stop()
	cron.AddJob("* * * * * ?", job)

	select {
	case <-time.After(OneSecond):
		if !strings.Contains(buf.String(), "YOLO") {
			t.Error("expected a panic to be logged, got none")
		}
		return
	}
}

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := newWithSeconds()
	cron.Start()

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected cron will be stopped immediately")
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		// No job ran!
	case <-wait(wg):
		t.Fatal("expected stopped cron does not run any job")
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	select {
	case <-time.After(OneSecond):
		t.Fatal("expected job runs")
	case <-wait(wg):
	}
}

// Test for #34. Adding a job after calling start results in multiple job invocations
func TestAddWhileRunningWithDelay(t *testing.T) {
	cron := newWithSeconds()
	cron.Start()
	defer cron.Stop()
	time.Sleep(5 * time.Second)
	var calls int64
	cron.AddFunc("* * * * * *", func() { atomic.AddInt64(&calls, 1) })

	<-time.After(OneSecond)
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

// Test cancel simple job,cancel op invalid
func TestCancelSimpleJob(t *testing.T) {
	cron := New(WithParser(secondParser), WithChain())
	cron.Start()
	defer cron.Stop()
	time.Sleep(5 * time.Second)
	var calls int64
	id, err := cron.AddFunc("@every 5s", func() {
		time.Sleep(3)
		atomic.AddInt64(&calls, 1)
	})
	if err != nil {
		t.Errorf("unexpected error %\n", err)
	}
	<-time.After(OneSecond * 6)
	cron.CancelEntry(id)
	<-time.After(OneSecond * 4)
	if atomic.LoadInt64(&calls) != 2 {
		t.Errorf("called %d times, expected 2\n", calls)
	}
}

// Test cancel option job,cancel op valid
func TestCancelOptionJob(t *testing.T) {
	cron := newWithSeconds()
	cron.Start()
	defer cron.Stop()
	var calls int64

	id, err := cron.AddOptionFunc("@every 5s", func(option *JobOption) {
		ctx := option.Context
		select {
		case <-time.After(OneSecond * 3):
			atomic.AddInt64(&calls, 1)
			return
		case <-ctx.Done():
			return
		}
	})

	if err != nil {
		t.Errorf("unexpected error %\n", err)
	}
	<-time.After(OneSecond * 6)
	cron.CancelEntry(id)
	<-time.After(OneSecond * 8)
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}

	<-time.After(OneSecond * 5)
	if atomic.LoadInt64(&calls) != 2 {
		t.Errorf("called %d times, expected 2\n", calls)
	}
}

// Test timeout option job,cancel op valid
func TestTimeoutOptionJob(t *testing.T) {
	testTimeoutForSimple := func() {
		cron := newWithSeconds()
		cron.Start()
		defer cron.Stop()
		var calls int64

		_, err := cron.AddOptionFunc("@every 5s", func(option *JobOption) {
			ctx := option.Context
			select {
			case <-time.After(OneSecond * 3):
				atomic.AddInt64(&calls, 1)
				return
			case <-ctx.Done():
				return
			}
		}, func(entry *Entry) {
			entry.Timeout = OneSecond * 2
		})

		if err != nil {
			t.Errorf("unexpected error %\n", err)
		}
		<-time.After(OneSecond * 8)
		if atomic.LoadInt64(&calls) != 0 {
			t.Errorf("called %d times, expected 0\n", calls)
		}
	}

	testTimeoutForCancel := func() {
		cron := newWithSeconds()
		cron.Start()
		defer cron.Stop()
		var calls int64

		id, err := cron.AddOptionFunc("@every 5s", func(option *JobOption) {
			ctx := option.Context
			select {
			case <-time.After(OneSecond * 2):
				atomic.AddInt64(&calls, 1)
				return
			case <-ctx.Done():
				return
			}
		}, func(entry *Entry) {
			entry.Timeout = OneSecond * 3
		})

		if err != nil {
			t.Errorf("unexpected error %\n", err)
		}
		<-time.After(OneSecond * 1)
		cron.CancelEntry(id)
		if atomic.LoadInt64(&calls) != 0 {
			t.Errorf("called %d times, expected 0\n", calls)
		}
	}

	t.Run("test for timeout", func(t *testing.T) {
		t.Run("simple timeout case", func(t *testing.T) {
			testTimeoutForSimple()
		})

		t.Run("Cancel ahead of time without timeout", func(t *testing.T) {
			testTimeoutForCancel()
		})

	})

}

func TestDisabledIdGenerator(t *testing.T) {
	cron := New(WithParser(secondParser), DisabledIdGenerator(), WithChain())
	cron.Start()
	defer cron.Stop()
	var calls int64

	id, err := cron.AddOptionFunc("* * * * * ?", func(o *JobOption) {
		atomic.AddInt64(&calls, 1)
	}, func(entry *Entry) {
		entry.ID = "123"
	})

	if err != nil {
		t.Fatal(err)
	}
	<-time.After(OneSecond)
	if id != "123" {
		t.Errorf("id is %s, expected \"123\"\n", id)
	}
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func TestForIdGenerator(t *testing.T) {
	cron := New(WithParser(secondParser), WithIdGenerator(&testIdGenerator{
		test: "321",
	}), WithChain())
	cron.Start()
	defer cron.Stop()
	var calls int64

	id, err := cron.AddOptionFunc("* * * * * ?", func(o *JobOption) {
		atomic.AddInt64(&calls, 1)
	})

	if err != nil {
		t.Fatal(err)
	}
	<-time.After(OneSecond)
	if id != "321" {
		t.Errorf("id is %s, expected \"321\"\n", id)
	}
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func TestRemoveOptionJob(t *testing.T) {
	cron := newWithSeconds()
	cron.Start()
	defer cron.Stop()

	id, err := cron.AddOptionFunc("* * * * * ?", func(o *JobOption) {
		cron.Remove(o.GetID())
	})
	if err != nil {
		t.Fatal(err)
	}

	for range time.After(OneSecond) {
		if ok := cron.hasEntry(id); ok {
			t.Errorf("test remove option job: want removed(true), got not removed(false)")
		} else {
			return
		}
	}
}

// Add a job, remove a job, start cron, expect nothing runs.
func TestRemoveBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	id, _ := cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.Remove(id)
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		// Success, shouldn't run
	case <-wait(wg):
		t.FailNow()
	}
}

// Start cron, add a job, remove it, expect it doesn't run.
func TestRemoveWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.Remove(id)

	select {
	case <-time.After(OneSecond):
	case <-wait(wg):
		t.FailNow()
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("@every 2s", func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(OneSecond):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(OneSecond):
		t.Error("expected job runs at 2 second mark")
	case <-wait(wg):
	}
}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	id1, _ := cron.AddFunc("* * * * * ?", func() { t.Fatal() })
	id2, _ := cron.AddFunc("* * * * * ?", func() { t.Fatal() })
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	cron.Remove(id1)
	cron.Start()
	cron.Remove(id2)
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job run in proper order")
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := newWithSeconds()
	cron.AddFunc("0 0 0 1 1 ?", func() {})
	cron.AddFunc("0 0 0 31 12 ?", func() {})
	cron.AddFunc("* * * * * ?", func() { wg.Done() })
	cron.Schedule(Every(time.Minute), FuncJob(func() {}))
	cron.Schedule(Every(time.Second), FuncJob(func() { wg.Done() }))
	cron.Schedule(Every(time.Hour), FuncJob(func() {}))

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * OneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	now := time.Now()
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= 58 {
		time.Sleep(2 * time.Second)
		now = time.Now()
	}
	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := newWithSeconds()
	cron.AddFunc(spec, func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that the cron is run in the given time zone (as opposed to local).
func TestNonLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	loc, err := time.LoadLocation("Atlantic/Cape_Verde")
	if err != nil {
		fmt.Printf("Failed to load time zone Atlantic/Cape_Verde: %+v", err)
		t.Fail()
	}

	now := time.Now().In(loc)
	// FIX: Issue #205
	// This calculation doesn't work in seconds 58 or 59.
	// Take the easy way out and sleep.
	if now.Second() >= 58 {
		time.Sleep(2 * time.Second)
		now = time.Now().In(loc)
	}
	spec := fmt.Sprintf("%d,%d %d %d %d %d ?",
		now.Second()+1, now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New(WithLocation(loc), WithParser(secondParser))
	cron.AddFunc(spec, func() { wg.Done() })
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond * 2):
		t.Error("expected job fires 2 times")
	case <-wait(wg):
	}
}

// Test that calling stop before start silently returns without
// blocking the stop channel.
func TestStopWithoutStart(t *testing.T) {
	cron := New()
	cron.Stop()
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
}

// Test that adding an invalid job spec returns an error
func TestInvalidJobSpec(t *testing.T) {
	cron := New()
	_, err := cron.AddJob("this will not parse", nil)
	if err == nil {
		t.Errorf("expected an error with invalid spec, got nil")
	}
}

// Test blocking run method behaves as Start()
func TestBlockingRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.AddFunc("* * * * * ?", func() { wg.Done() })

	var unblockChan = make(chan struct{})

	go func() {
		cron.Run()
		close(unblockChan)
	}()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.Error("expected job fires")
	case <-unblockChan:
		t.Error("expected that Run() blocks")
	case <-wait(wg):
	}
}

// Test that double-running is a no-op
func TestStartNoop(t *testing.T) {
	var tickChan = make(chan struct{}, 2)

	cron := newWithSeconds()
	cron.AddFunc("* * * * * ?", func() {
		tickChan <- struct{}{}
	})

	cron.Start()
	defer cron.Stop()

	// Wait for the first firing to ensure the runner is going
	<-tickChan

	cron.Start()

	<-tickChan

	// Fail if this job fires again in a short period, indicating a double-run
	select {
	case <-time.After(time.Millisecond):
	case <-tickChan:
		t.Error("expected job fires exactly twice")
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := newWithSeconds()
	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"})
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"})
	job2, _ := cron.AddJob("* * * * * ?", testJob{wg, "job2"})
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"})
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"})
	job5 := cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"})

	// Test getting an Entry pre-Start.
	if actualName := cron.Entry(job2).Job.(testJob).name; actualName != "job2" {
		t.Error("wrong job retrieved:", actualName)
	}
	if actualName := cron.Entry(job5).Job.(testJob).name; actualName != "job5" {
		t.Error("wrong job retrieved:", actualName)
	}

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(OneSecond):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string

	clone := new(EntryHeap)
	for len(cron.entries) > 0 {
		entry := heap.Pop(&cron.entries).(*Entry)
		actuals = append(actuals, entry.Job.(testJob).name)
		heap.Push(clone, entry)
	}
	cron.entries = *clone

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Fatalf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
		}
	}

	// Test getting Entries.
	if actualName := cron.Entry(job2).Job.(testJob).name; actualName != "job2" {
		t.Error("wrong job retrieved:", actualName)
	}
	if actualName := cron.Entry(job5).Job.(testJob).name; actualName != "job5" {
		t.Error("wrong job retrieved:", actualName)
	}
}

// Issue #206
// Ensure that the next run of a job after removing an entry is accurate.
func TestScheduleAfterRemoval(t *testing.T) {
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	wg1.Add(1)
	wg2.Add(1)

	// The first time this job is run, set a timer and remove the other job
	// 750ms later. Correct behavior would be to still run the job again in
	// 250ms, but the bug would cause it to run instead 1s later.

	var calls int
	var mu sync.Mutex

	cron := newWithSeconds()
	hourJob := cron.Schedule(Every(time.Hour), FuncJob(func() {}))
	cron.Schedule(Every(time.Second), FuncJob(func() {
		mu.Lock()
		defer mu.Unlock()
		switch calls {
		case 0:
			wg1.Done()
			calls++
		case 1:
			time.Sleep(750 * time.Millisecond)
			cron.Remove(hourJob)
			calls++
		case 2:
			calls++
			wg2.Done()
		case 3:
			panic("unexpected 3rd call")
		}
	}))

	cron.Start()
	defer cron.Stop()

	// the first run might be any length of time 0 - 1s, since the schedule
	// rounds to the second. wait for the first run to true up.
	wg1.Wait()

	select {
	case <-time.After(2 * OneSecond):
		t.Error("expected job fires 2 times")
	case <-wait(&wg2):
	}
}

type ZeroSchedule struct{}

func (*ZeroSchedule) Next(time.Time) time.Time {
	return time.Time{}
}

func (*ZeroSchedule) Prev(time.Time) time.Time {
	return time.Time{}
}

func (*ZeroSchedule) IsOnce() bool {
	return false
}

// Tests that job without time does not run
func TestJobWithZeroTimeDoesNotRun(t *testing.T) {
	cron := newWithSeconds()
	var calls int64
	cron.AddFunc("* * * * * *", func() { atomic.AddInt64(&calls, 1) })
	cron.Schedule(new(ZeroSchedule), FuncJob(func() { t.Error("expected zero task will not run") }))
	cron.Start()
	defer cron.Stop()
	<-time.After(OneSecond)
	if atomic.LoadInt64(&calls) != 1 {
		t.Errorf("called %d times, expected 1\n", calls)
	}
}

func TestStopAndWait(t *testing.T) {
	t.Run("nothing running, returns immediately", func(t *testing.T) {
		cron := newWithSeconds()
		cron.Start()
		ctx := cron.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(time.Millisecond):
			t.Error("context was not done immediately")
		}
	})

	t.Run("repeated calls to Stop", func(t *testing.T) {
		cron := newWithSeconds()
		cron.Start()
		_ = cron.Stop()
		time.Sleep(time.Millisecond)
		ctx := cron.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(time.Millisecond):
			t.Error("context was not done immediately")
		}
	})

	t.Run("a couple fast jobs added, still returns immediately", func(t *testing.T) {
		cron := newWithSeconds()
		cron.AddFunc("* * * * * *", func() {})
		cron.Start()
		cron.AddFunc("* * * * * *", func() {})
		cron.AddFunc("* * * * * *", func() {})
		cron.AddFunc("* * * * * *", func() {})
		time.Sleep(time.Second)
		ctx := cron.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(time.Millisecond):
			t.Error("context was not done immediately")
		}
	})

	t.Run("a couple fast jobs and a slow job added, waits for slow job", func(t *testing.T) {
		cron := newWithSeconds()
		cron.AddFunc("* * * * * *", func() {})
		cron.Start()
		cron.AddFunc("* * * * * *", func() { time.Sleep(2 * time.Second) })
		cron.AddFunc("* * * * * *", func() {})
		time.Sleep(time.Second)

		ctx := cron.Stop()

		// Verify that it is not done for at least 750ms
		select {
		case <-ctx.Done():
			t.Error("context was done too quickly immediately")
		case <-time.After(750 * time.Millisecond):
			// expected, because the job sleeping for 1 second is still running
		}

		// Verify that it IS done in the next 500ms (giving 250ms buffer)
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(1500 * time.Millisecond):
			t.Error("context not done after job should have completed")
		}
	})

	t.Run("repeated calls to stop, waiting for completion and after", func(t *testing.T) {
		cron := newWithSeconds()
		cron.AddFunc("* * * * * *", func() {})
		cron.AddFunc("* * * * * *", func() { time.Sleep(2 * time.Second) })
		cron.Start()
		cron.AddFunc("* * * * * *", func() {})
		time.Sleep(time.Second)
		ctx := cron.Stop()
		ctx2 := cron.Stop()

		// Verify that it is not done for at least 1500ms
		select {
		case <-ctx.Done():
			t.Error("context was done too quickly immediately")
		case <-ctx2.Done():
			t.Error("context2 was done too quickly immediately")
		case <-time.After(1500 * time.Millisecond):
			// expected, because the job sleeping for 2 seconds is still running
		}

		// Verify that it IS done in the next 1s (giving 500ms buffer)
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(time.Second):
			t.Error("context not done after job should have completed")
		}

		// Verify that ctx2 is also done.
		select {
		case <-ctx2.Done():
			// expected
		case <-time.After(time.Millisecond):
			t.Error("context2 not done even though context1 is")
		}

		// Verify that a new context retrieved from stop is immediately done.
		ctx3 := cron.Stop()
		select {
		case <-ctx3.Done():
			// expected
		case <-time.After(time.Millisecond):
			t.Error("context not done even when cron Stop is completed")
		}

	})
}

func TestJobWithCustomPrev(t *testing.T) {
	cron := New()
	var calls int64
	// running every 3s, but starting 2s in the past
	// expected timeline: 1s ... 4s ... stop (2 calls)
	// if prev was ignored, the func would only be called once (at 3s)
	cron.AddFunc("@every 3s", func() { atomic.AddInt64(&calls, 1) }, WithPrev(time.Now().Add(-2*time.Second)))
	cron.Start()
	time.Sleep(5 * time.Second)
	if atomic.LoadInt64(&calls) != 2 {
		t.Errorf("called %d times, expected 2\n", calls)
	}
}

func TestMultiThreadedStartAndStop(t *testing.T) {
	cron := New()
	go cron.Run()
	time.Sleep(2 * time.Millisecond)
	cron.Stop()
}

func TestCron_UpdateSchedule(t *testing.T) {
	executionInterval := func(entry Entry) time.Duration {
		// To get a more accurate measurement of the execution interval, finding the time
		// difference between the next execution time and the one that follows.
		next := entry.Next
		return entry.Schedule.Next(next).Sub(next)
	}

	checkNoError := func(err error) {
		if err != nil {
			t.Error(err)
		}
	}

	testScheduleUpdateBeforeStartingCronJob := func(usingSpecString bool) {
		oldSpec := "?/10 * * * * *"
		newSpec := "?/5 * * * * *"

		cron := New(WithSeconds())
		id, err := cron.AddFunc(oldSpec, func() {})
		checkNoError(err)
		executionIntervalOldSpec := executionInterval(cron.Entries()[0])

		// Updating schedule.
		if usingSpecString {
			checkNoError(cron.UpdateScheduleWithSpec(id, newSpec))
		} else {
			newSpecSchedule, err := cron.parser.Parse(newSpec)
			checkNoError(err)
			checkNoError(cron.UpdateSchedule(id, newSpecSchedule))
		}
		executionIntervalNewSpec := executionInterval(cron.Entries()[0])

		if executionIntervalOldSpec.Seconds() != (executionIntervalNewSpec.Seconds() * 2) {
			t.Fatal("failed to update schedule")
		}
	}

	testScheduleUpdateAfterStartingCronJob := func(usingSpecString bool) {
		oldSpec := "@every 50s"
		newSpec := "@every 5s"

		cron := New(WithSeconds())
		executionCounterOldSpec := int64(0)
		executionCounterNewSpec := int64(0)
		updated := make(chan struct{})

		id, err := cron.AddFunc(oldSpec, func() {
			select {
			case <-updated:
				atomic.AddInt64(&executionCounterNewSpec, 1)
			default:
				atomic.AddInt64(&executionCounterOldSpec, 1)
			}
		})

		checkNoError(err)
		cron.Start()

		select {
		case <-time.After(5 * time.Second):
			// Notifying the job to increment executionCounterNewSpec.
			close(updated)

			if usingSpecString {
				// Updating schedule.
				checkNoError(cron.UpdateScheduleWithSpec(id, newSpec))
			} else {
				newSpecSchedule, err := cron.parser.Parse(newSpec)
				checkNoError(err)
				checkNoError(cron.UpdateSchedule(id, newSpecSchedule))
			}
		}

		// Allow at least 1 execution of the job.
		select {
		case <-time.After(10 * time.Second):
			cron.Stop()
		}

		// Given the old schedule (old spec), the job should not have executed
		// at all.
		if atomic.LoadInt64(&executionCounterOldSpec) != 0 {
			t.Fatal("job ran sooner than it was supposed to")
		}

		// With the updated schedule (new spec), the job should have executed
		// at least once.
		if atomic.LoadInt64(&executionCounterNewSpec) < 1 {
			t.Fatal("failed to update schedule for job")
		}
	}

	t.Run("updating schedule before starting cron job", func(t *testing.T) {
		t.Run("providing spec string", func(t *testing.T) {
			testScheduleUpdateBeforeStartingCronJob(true)
		})

		t.Run("providing schedule", func(t *testing.T) {
			testScheduleUpdateBeforeStartingCronJob(false)
		})
	})

	t.Run("updating schedule after starting cron job", func(t *testing.T) {
		t.Run("providing spec string", func(t *testing.T) {
			testScheduleUpdateAfterStartingCronJob(true)
		})

		t.Run("providing schedule", func(t *testing.T) {
			testScheduleUpdateAfterStartingCronJob(false)
		})
	})

}

func TestCron_PauseAndContinue(t *testing.T) {
	checkNoError := func(err error) {
		if err != nil {
			t.Error(err)
		}
	}

	t.Run("pause invalid entry", func(t *testing.T) {
		spec := "?/5 * * * * *" // every 5 seconds.
		cron := New(WithSeconds())
		ticks := 0
		id, err := cron.AddFunc(spec, func() { ticks++ })
		checkNoError(err)
		if err := cron.Pause(id.(int) + 1); err == nil {
			t.Error("was able to pause invalid entry")
		}
	})

	t.Run("pause after single execution and continue after 2 cycles", func(t *testing.T) {
		spec := "?/5 * * * * *" // every 5 seconds.
		cron := New(WithSeconds())
		var ticks int64 = 0
		id, err := cron.AddFunc(spec, func() { atomic.AddInt64(&ticks, 1) })
		checkNoError(err)
		cron.Start()

		for atomic.LoadInt64(&ticks) == 0 {
		} // waiting for single execution of job.
		checkNoError(cron.Pause(id))

		<-time.After(12 * time.Second)     // waiting 2 execution cycles + some buffer time (2 sec).
		if atomic.LoadInt64(&ticks) != 1 { // the job should have just run once.
			t.Error("failed to correctly pause job")
		}
		checkNoError(cron.Continue(id))
		// next execution would be in approx 3 seconds as we had a 2 second buffer.
		<-time.After(4 * time.Second)      // waiting for one more execution of job (1sec buffer).
		if atomic.LoadInt64(&ticks) != 2 { // the job should have run twice.
			t.Error("failed to correctly continue job")
		}
		cron.Stop()
	})

	t.Run("pause and continue with multiple jobs", func(t *testing.T) {
		spec := "?/5 * * * * *" // every 5 seconds.
		cron := New(WithSeconds())
		var tick1, tick2 int64
		var id1, id2 EntryID
		var err error
		id1, err = cron.AddFunc(spec, func() {
			atomic.AddInt64(&tick1, 1)
		})
		checkNoError(err)

		id2, err = cron.AddFunc(spec, func() {
			atomic.AddInt64(&tick2, 1)
		})
		checkNoError(err)

		cron.Start()

		for (atomic.LoadInt64(&tick1) == 0) && (atomic.LoadInt64(&tick2) == 0) {
		} // waiting for both jobs to execute once.
		checkNoError(cron.Pause(id1))

		<-time.After(12 * time.Second)     // waiting 2 execution cycles + some buffer time (2 sec).
		if atomic.LoadInt64(&tick1) != 1 { // tick1 should not have changed as corresponding job was paused.
			t.Error("failed to pause job with id = ", id1)
		}
		if atomic.LoadInt64(&tick2) != 3 {
			// should not be here.
			t.Error("job with id = ", id2, " did not execute required number of times")
		}
		checkNoError(cron.Pause(id2))
		checkNoError(cron.Continue(id1))

		<-time.After(4 * time.Second) // waiting for one more execution of job1 (1sec buffer).
		if atomic.LoadInt64(&tick2) != 3 {
			t.Error("failed to pause job with id = ", id2)
		}
		if atomic.LoadInt64(&tick1) != 2 {
			t.Error("continued job with id = ", id1, " executed more times than required")
		}
		cron.Stop()
	})
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}

// newWithSeconds returns a Cron with the seconds field enabled.
func newWithSeconds() *Cron {
	return New(WithParser(secondParser), WithChain())
}
