package jobs

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/db"
)

type Job struct {
	Id             int64
	Kind           string
	SubmissionId   string
	Attempt        int
	TimeoutRetries int
}

type outcomeKind int

const (
	outcomeDone outcomeKind = iota
	outcomeRetry
	outcomeDead
)

type Outcome struct {
	kind           outcomeKind
	runAt          time.Time
	attempt        int
	timeoutRetries int
	reason         string
}

func Done() Outcome {
	return Outcome{kind: outcomeDone}
}

func RetryAt(runAt time.Time, attempt, timeoutRetries int, reason string) Outcome {
	return Outcome{kind: outcomeRetry, runAt: runAt, attempt: attempt, timeoutRetries: timeoutRetries, reason: reason}
}

func Dead(reason string) Outcome {
	return Outcome{kind: outcomeDead, reason: reason}
}

func (o Outcome) IsDone() bool {
	return o.kind == outcomeDone
}

func (o Outcome) IsRetry() bool {
	return o.kind == outcomeRetry
}

func (o Outcome) RetryTime() time.Time {
	return o.runAt
}

func (o Outcome) NextAttempt() int {
	return o.attempt
}

type Handler func(ctx context.Context, j Job) Outcome

type Queue struct {
	pool     *pgxpool.Pool
	workerId string
	handlers map[string]Handler
	wake     chan struct{}
	mu       sync.Mutex
	inflight map[int64]struct{}
}

func NewQueue(pool *pgxpool.Pool, workerId string) *Queue {
	return &Queue{pool: pool, workerId: workerId, handlers: map[string]Handler{}, wake: make(chan struct{}, 1), inflight: map[int64]struct{}{}}
}

func (q *Queue) track(id int64) {
	q.mu.Lock()
	q.inflight[id] = struct{}{}
	q.mu.Unlock()
}

func (q *Queue) untrack(id int64) {
	q.mu.Lock()
	delete(q.inflight, id)
	q.mu.Unlock()
}

func (q *Queue) inflightCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.inflight)
}

func (q *Queue) inflightIds() []int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	ids := make([]int64, 0, len(q.inflight))
	for id := range q.inflight {
		ids = append(ids, id)
	}
	return ids
}

func (q *Queue) Register(kind string, h Handler) {
	q.handlers[kind] = h
}

// Enqueue schedules a job; a live (due or running) twin for the same
// (kind, submission) makes this a no-op via the partial unique index.
func (q *Queue) Enqueue(ctx context.Context, kind, submissionId string, runAt time.Time) error {
	return EnqueueTx(ctx, q.pool, kind, submissionId, runAt)
}

// execer is satisfied by both *pgxpool.Pool and pgx.Tx, so EnqueueTx can schedule
// a job either standalone or atomically inside a caller's transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// EnqueueTx schedules a job on the given executor. Passing a pgx.Tx makes the
// enqueue commit atomically with the row that triggered it, so an accepted ship
// can never be persisted without the job that drives it. Same idempotency as
// Enqueue: a live (due or running) twin is a no-op via the partial unique index.
func EnqueueTx(ctx context.Context, db execer, kind, submissionId string, runAt time.Time) error {
	_, err := db.Exec(ctx, `
		insert into ariw.job (kind, "submissionId", "runAt")
		values ($1, $2, $3)
		on conflict (kind, "submissionId") where status in ('due', 'running') do nothing`,
		kind, submissionId, runAt.UTC())
	return err
}

// Wake nudges the poll loop (e.g. right after an ingest enqueues followups).
func (q *Queue) Wake() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *Queue) claim(ctx context.Context, limit int) ([]Job, error) {
	rows, err := q.pool.Query(ctx, `
		update ariw.job set status = 'running', "claimedAt" = now(), "claimedBy" = $1
		where id in (
			select id from ariw.job
			where status = 'due' and "runAt" <= now()
			order by "runAt"
			limit $2
			for update skip locked
		)
		returning id, kind, "submissionId", attempt, "timeoutRetries"`,
		q.workerId, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.Id, &j.Kind, &j.SubmissionId, &j.Attempt, &j.TimeoutRetries); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (q *Queue) settle(ctx context.Context, j Job, o Outcome) {
	var err error
	switch o.kind {
	case outcomeDone:
		_, err = q.pool.Exec(ctx, `
			update ariw.job set status = 'done', "finishedAt" = now()
			where id = $1 and status = 'running' and "claimedBy" = $2`,
			j.Id, q.workerId)
	case outcomeRetry:
		_, err = q.pool.Exec(ctx, `
			update ariw.job set status = 'due', "runAt" = $2, attempt = $3, "timeoutRetries" = $4, "lastError" = nullif($5, '')
			where id = $1 and status = 'running' and "claimedBy" = $6`,
			j.Id, o.runAt.UTC(), o.attempt, o.timeoutRetries, o.reason, q.workerId)
	case outcomeDead:
		_, err = q.pool.Exec(ctx, `
			update ariw.job set status = 'dead', "finishedAt" = now(), "lastError" = $2
			where id = $1 and status = 'running' and "claimedBy" = $3`,
			j.Id, o.reason, q.workerId)
	}
	if err != nil {
		slog.Error("job settle failed; the running reaper will re-drive it", "job", j.Id, "kind", j.Kind, "err", err)
	}
}

func (q *Queue) runOne(ctx context.Context, j Job) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute) // must never keep a worker slot forever and should be generous to the next worst enrich
	defer cancel()
	outcome := func() (o Outcome) {
		defer func() {
			if p := recover(); p != nil { // one bad submission must never kill the worker
				slog.Error("job panic", "job", j.Id, "kind", j.Kind, "submission", j.SubmissionId, "panic", p, "stack", string(debug.Stack()))
				if j.Attempt >= 7 { // hard cap so a panic cannot loop forever and get us stuck
					o = Dead(fmt.Sprintf("panic: %v", p))
				} else {
					o = RetryAt(time.Now().Add(5*time.Minute), j.Attempt+1, j.TimeoutRetries, fmt.Sprintf("panic: %v", p))
				}
			}
		}()
		archived, err := db.SubmissionArchived(ctx, q.pool, j.SubmissionId)
		if err != nil {
			return RetryAt(time.Now().Add(time.Minute), j.Attempt, j.TimeoutRetries, "program status lookup failed: "+err.Error()) // one minute, and no attempt spent: the lookup failed, not the job
		}
		if archived { // an archived program is paused: its ships get no scans, checks or deliveries, and restoring it picks the job back up
			return RetryAt(time.Now().Add(10*time.Minute), j.Attempt, j.TimeoutRetries, "program archived") // re-checked every 10 minutes with no attempt spent, so a long archive never kills the job
		}
		h, ok := q.handlers[j.Kind]
		if !ok {
			return Dead("no handler registered for kind " + j.Kind)
		}
		return h(ctx, j)
	}()

	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	q.settle(settleCtx, j, outcome)
}

func (q *Queue) Run(ctx context.Context, pollEvery time.Duration, workers int) {
	var wg sync.WaitGroup
	var lastReap, lastBeat, lastPurge time.Time
	for ctx.Err() == nil {
		if time.Since(lastBeat) > time.Minute {
			q.heartbeatInflight(ctx)
			lastBeat = time.Now()
		}
		if time.Since(lastReap) > time.Minute {
			q.reapStuckRunning(ctx)
			lastReap = time.Now()
		}
		if time.Since(lastPurge) > time.Hour {
			q.purgeSettled(ctx)
			lastPurge = time.Now()
		}
		if free := workers - q.inflightCount(); free > 0 {
			claimed, err := q.claim(ctx, free)
			if err != nil && ctx.Err() == nil {
				slog.Warn("job claim failed", "err", err)
			}
			for _, j := range claimed {
				q.track(j.Id)
				wg.Add(1)
				go func(j Job) {
					defer wg.Done()
					defer q.Wake() // claim the next due job without waiting out the poll tick
					defer q.untrack(j.Id)
					q.runOne(ctx, j)
				}(j)
			}
		}
		select {
		case <-ctx.Done():
		case <-q.wake:
		case <-time.After(pollEvery):
		}
	}
	wg.Wait()
}

func (q *Queue) heartbeatInflight(ctx context.Context) {
	ids := q.inflightIds()
	if len(ids) == 0 {
		return
	}
	_, err := q.pool.Exec(ctx, `
		update ariw.job set "claimedAt" = now()
		where id = any($1) and status = 'running' and "claimedBy" = $2`,
		ids, q.workerId)
	if err != nil && ctx.Err() == nil {
		slog.Warn("job heartbeat failed", "err", err)
	}
}

func (q *Queue) reapStuckRunning(ctx context.Context) {
	tag, err := q.pool.Exec(ctx, `
		update ariw.job set status = 'due', "runAt" = now()
		where status = 'running' and "claimedAt" < now() - interval '15 minutes'`)
	if err != nil && ctx.Err() == nil {
		slog.Warn("running reaper failed", "err", err)
	} else if err == nil && tag.RowsAffected() > 0 {
		slog.Info("re-drove stuck running jobs", "count", tag.RowsAffected())
	}
}

func (q *Queue) purgeSettled(ctx context.Context) {
	tag, err := q.pool.Exec(ctx,
		`delete from ariw.job where status in ('done', 'dead') and "finishedAt" < now() - interval '30 days'`)
	if err != nil && ctx.Err() == nil {
		slog.Warn("job purge failed", "err", err)
	} else if err == nil && tag.RowsAffected() > 0 {
		slog.Info("purged settled jobs", "count", tag.RowsAffected())
	}
}
