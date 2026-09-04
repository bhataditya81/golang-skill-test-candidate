package jobs

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCreateAndGet(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected queued, got %s", job.Status)
	}

	got, ok := s.Get(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatalf("job not found")
	}
}

func TestFailedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()

	job, err := s.Create(context.Background(), "please fail")
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		got, _ := s.Get(job.ID)
		if got.Status == StatusFailed {
			if got.Error == "" {
				t.Fatal("expected failure message")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not fail")
}

func TestConcurrentCreate(t *testing.T) {
	s := NewService(4, 100)
	defer s.Stop()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Create(context.Background(), "hello"); err != nil {
				t.Errorf("create: %v", err)
			}
		}()
	}
	wg.Wait()
}

// TestConcurrentCreateStopNoPanic: Create racing Stop must never panic on a closed queue channel.
func TestConcurrentCreateStopNoPanic(t *testing.T) {
	var panics atomic.Int64
	const attempts = 400
	const goroutines = 8
	const sendsEach = 5

	for range attempts {
		s := NewService(4, 1000)
		var wg sync.WaitGroup

		for range goroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range sendsEach {
					func() {
						defer func() {
							if r := recover(); r != nil {
								panics.Add(1)
							}
						}()
						s.Create(context.Background(), "hello")
					}()
				}
			}()
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stop()
		}()

		wg.Wait()
	}

	if p := panics.Load(); p > 0 {
		t.Fatalf("Create panicked %d times racing against Stop (send on closed channel)", p)
	}
}

// TestQueueFullDoesNotOrphanJob: a job rejected for a full queue must not remain in the job store.
func TestQueueFullDoesNotOrphanJob(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	s := NewService(1, 1)
	s.processor = ProcessorFunc(func(ctx context.Context, payload string) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	defer func() {
		close(release)
		s.Stop()
	}()

	// job-1: picked up by the single worker immediately, freeing the 1-slot buffer.
	if _, err := s.Create(context.Background(), "first"); err != nil {
		t.Fatalf("create job-1: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker never started processing job-1")
	}

	// job-2: fills the now-empty buffer.
	if _, err := s.Create(context.Background(), "second"); err != nil {
		t.Fatalf("create job-2: %v", err)
	}

	// job-3: buffer full and worker still busy on job-1 -> must be rejected.
	_, err := s.Create(context.Background(), "third")
	if err == nil {
		t.Fatal("expected job-3 to be rejected as queue full")
	}
	s.mu.RLock()
	_, exists := s.jobs["job-3"]
	s.mu.RUnlock()
	if exists {
		t.Fatal("rejected job-3 was left orphaned in the job store")
	}
}
