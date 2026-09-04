# Bugs

- `Create` vs `Stop` race: non-blocking send on `s.queue` after a stale `stopping.Load()` check panics with "send on closed channel" once `Stop` closes the channel mid-flight. Reproduced: 50-611 panics per stress run.
- Queue-full rejection leaks the job: `Create` inserts into `s.jobs` before the enqueue attempt, but only removes it on `ctx.Done()`, not on the `default` (full) branch. Rejected jobs sit as `"queued"` forever.
- Shutdown ignores timeouts: `main.go` gives `server.Shutdown` a 5s context but calls `service.Stop()` with none — it blocks until the queue fully drains, however long that takes.
- Cancellation is wired but dead: `defaultProcessor` checks `ctx.Done()`, but `process()` always calls it with `context.Background()`, so in-flight work can never be cancelled on shutdown.
- `http.go`'s queue-full branch and its `else` do the same thing — dead conditional, and matching on `err.Error()` string content is fragile.
- `containsFail` hand-rolls what `strings.Contains` already does — not a bug, just noise.
- Data race in `Create`'s success path: `cloneJob(job)` was called after `s.queue <- id` but without holding `s.mu`, reading `job.Status`/`job.Error` while a worker could already be writing them from `process()`.