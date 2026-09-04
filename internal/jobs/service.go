package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID        string    `json:"id"`
	Payload   string    `json:"payload"`
	Status    Status    `json:"status"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type Processor interface {
	Process(ctx context.Context, payload string) error
}

var (
	ErrQueueFull = errors.New("job queue is full")
	ErrStopping  = errors.New("service is stopping")
)

type Service struct {
	mu   sync.RWMutex
	jobs map[string]*Job

	// stopMu guards stopping and the close of queue, so Create's
	// check-then-send can never race a concurrent Stop closing the channel.
	stopMu   sync.RWMutex
	stopping bool

	queue     chan string
	workers   int
	processor Processor
	wg        sync.WaitGroup
	sequence  atomic.Uint64

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

func NewService(workers, queueCapacity int) *Service {
	if workers < 1 {
		workers = 1
	}
	if queueCapacity < 1 {
		queueCapacity = 1
	}

	shutdownCtx, cancel := context.WithCancel(context.Background())
	s := &Service{
		jobs:           make(map[string]*Job),
		queue:          make(chan string, queueCapacity),
		workers:        workers,
		processor:      ProcessorFunc(defaultProcessor),
		shutdownCtx:    shutdownCtx,
		shutdownCancel: cancel,
	}
	s.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

type ProcessorFunc func(context.Context, string) error

func (f ProcessorFunc) Process(ctx context.Context, payload string) error {
	return f(ctx, payload)
}

func defaultProcessor(ctx context.Context, payload string) error {
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	if payload == "" {
		return errors.New("empty payload")
	}
	if strings.Contains(payload, "fail") {
		return errors.New("simulated processing failure")
	}
	return nil
}

func (s *Service) nextID() string {
	return fmt.Sprintf("job-%d", s.sequence.Add(1))
}

func (s *Service) Create(ctx context.Context, payload string) (*Job, error) {
	// Held for the whole check-then-send below so Stop cannot close the
	// queue out from under us: close only happens under stopMu's write lock.
	s.stopMu.RLock()
	defer s.stopMu.RUnlock()
	if s.stopping {
		return nil, ErrStopping
	}

	id := s.nextID()
	job := &Job{
		ID:        id,
		Payload:   payload,
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.mu.Unlock()

	select {
	case s.queue <- id:
		return cloneJob(job), nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.jobs, id)
		s.mu.Unlock()
		return nil, ctx.Err()
	default:
		s.mu.Lock()
		delete(s.jobs, id)
		s.mu.Unlock()
		return nil, ErrQueueFull
	}
}

func (s *Service) Get(id string) (*Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	return cloneJob(job), true
}

func (s *Service) worker() {
	defer s.wg.Done()
	for id := range s.queue {
		s.process(id)
	}
}

func (s *Service) process(id string) {
	s.mu.Lock()
	job, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	job.Status = StatusProcessing
	s.mu.Unlock()

	err := s.processor.Process(s.shutdownCtx, job.Payload)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		job.Status = StatusFailed
		job.Error = err.Error()
		return
	}
	job.Status = StatusCompleted
}

func (s *Service) Stop() {
	s.stopMu.Lock()
	if s.stopping {
		s.stopMu.Unlock()
		return
	}
	s.stopping = true
	close(s.queue)
	s.stopMu.Unlock()
	s.shutdownCancel()
	s.wg.Wait()
}

func cloneJob(j *Job) *Job {
	copy := *j
	return &copy
}
