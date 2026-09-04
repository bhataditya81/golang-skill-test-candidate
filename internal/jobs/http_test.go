package jobs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestMux(s *Service) *http.ServeMux {
	mux := http.NewServeMux()
	RegisterHandlers(mux, s)
	return mux
}

func TestPostJobsCreatesQueuedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"payload":"hello"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	var job Job
	if err := json.NewDecoder(rec.Body).Decode(&job); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if job.ID == "" {
		t.Fatal("expected non-empty job id")
	}
	if job.Status != StatusQueued {
		t.Fatalf("expected status queued, got %s", job.Status)
	}
}

func TestPostJobsRejectsBlankPayload(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"payload":"   "}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostJobsRejectsMalformedJSON(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`not json`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestGetJobsReturnsCreatedJob(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	mux := newTestMux(s)

	postReq := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"payload":"hello"}`))
	postRec := httptest.NewRecorder()
	mux.ServeHTTP(postRec, postReq)

	var created Job
	if err := json.NewDecoder(postRec.Body).Decode(&created); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+created.ID, nil)
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", getRec.Code, getRec.Body.String())
	}
	var got Job
	if err := json.NewDecoder(getRec.Body).Decode(&got); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("expected job %s, got %s", created.ID, got.ID)
	}
}

func TestGetJobsNotFound(t *testing.T) {
	s := NewService(1, 4)
	defer s.Stop()
	mux := newTestMux(s)

	req := httptest.NewRequest(http.MethodGet, "/jobs/does-not-exist", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPostJobsReturns503WhenQueueFull(t *testing.T) {
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
	mux := newTestMux(s)

	post := func(payload string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"payload":"`+payload+`"}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec
	}

	// job-1: picked up by the single worker immediately, freeing the 1-slot buffer.
	if rec := post("first"); rec.Code != http.StatusCreated {
		t.Fatalf("create job-1: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker never started processing job-1")
	}

	// job-2: fills the now-empty buffer.
	if rec := post("second"); rec.Code != http.StatusCreated {
		t.Fatalf("create job-2: expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	// job-3: buffer full and worker still busy on job-1 -> must be rejected.
	rec := post("third")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when queue is full, got %d: %s", rec.Code, rec.Body.String())
	}
}
