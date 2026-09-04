package jobs

import (
	"context"
	"sync"
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
