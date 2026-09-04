# Enhancements

- Swap string-matched errors for sentinels (`ErrQueueFull`, `ErrStopping`) checked with `errors.Is`.
- Thread a shutdown context through `Stop()` into `process()` so in-flight jobs can actually be cancelled, not just drained.
- Cap job map growth — completed/failed jobs live forever right now; add TTL eviction or a max size.
- Move hardcoded values (worker count, queue capacity, port, etc.) out of `main.go` into a `config.go` loaded via envconfig.
- Add a request body size limit on `POST /jobs` to avoid unbounded reads.
- Add `/healthz` and `/readyz` so shutdown/readiness is observable, not just inferred.
- Add structured logging (job id, status transitions) instead of relying on default `log`.
- Add an `http_test.go` — handler layer currently has zero direct test coverage.
- Wire `go vet`, `gofmt -l`, and `go test -race` into CI so regressions get caught before review.
