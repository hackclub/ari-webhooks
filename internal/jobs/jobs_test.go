package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hackclub/ari-webhooks/internal/testdb"
)

func TestEnqueueDedupesLiveJobs(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()
	for range 3 {
		if err := q.Enqueue(ctx, "enrich", "sub1", time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `select count(*) from ariw.job`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 live job, got %d", n)
	}
}

func TestClaimRespectsRunAtAndSettles(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()

	if err := q.Enqueue(ctx, "enrich", "later", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, "enrich", "due", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].SubmissionId != "due" {
		t.Fatalf("claimed %+v, want only the due job", claimed)
	}

	q.settle(ctx, claimed[0], RetryAt(time.Now().Add(-time.Millisecond), 2, 1, "transient"))
	claimed, err = q.claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].Attempt != 2 || claimed[0].TimeoutRetries != 1 {
		t.Fatalf("retry did not carry state: %+v", claimed)
	}

	q.settle(ctx, claimed[0], Done())
	var status string
	if err := pool.QueryRow(ctx, `select status from ariw.job where "submissionId" = 'due'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("status %s, want done", status)
	}
}

func TestArchivedProgramHoldsItsJobsUntilRestored(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()
	for _, seed := range []string{
		`insert into "Program" (id, name, color, status) values ('program1', 'P', '#000', 'ARCHIVED')`,
		`insert into "Maker" (id, email) values ('maker1', 'user1@example.com')`,
		`insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours")
		 values ('ship1', 'program1', 'project1', 'maker1', 'T', 'https://github.com/a/b', 0)`,
	} {
		if _, err := pool.Exec(ctx, seed); err != nil {
			t.Fatal(err)
		}
	}
	var ran atomic.Int32
	q.Register("screen", func(ctx context.Context, j Job) Outcome {
		ran.Add(1)
		return Done()
	})
	if err := q.Enqueue(ctx, "screen", "ship1", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}

	claimed, err := q.claim(ctx, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	q.runOne(ctx, claimed[0])
	var status, lastError string
	var attempt int
	var deferred bool
	if err := pool.QueryRow(ctx, `
		select status, attempt, coalesce("lastError", ''), "runAt" > now() + interval '5 minutes'
		from ariw.job where "submissionId" = 'ship1'`).Scan(&status, &attempt, &lastError, &deferred); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 0 || status != "due" || attempt != 0 || lastError != "program archived" || !deferred {
		t.Fatalf("archived program must hold the job without running it: ran=%d status=%s attempt=%d lastError=%q deferred=%v",
			ran.Load(), status, attempt, lastError, deferred)
	}

	if _, err := pool.Exec(ctx, `update "Program" set status = 'ACTIVE' where id = 'program1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `update ariw.job set "runAt" = now() where "submissionId" = 'ship1'`); err != nil {
		t.Fatal(err)
	}
	claimed, err = q.claim(ctx, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim after restore: %v %v", claimed, err)
	}
	q.runOne(ctx, claimed[0])
	if err := pool.QueryRow(ctx, `select status from ariw.job where "submissionId" = 'ship1'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if ran.Load() != 1 || status != "done" {
		t.Fatalf("restored program must run the held job: ran=%d status=%s", ran.Load(), status)
	}
}

func TestRunExecutesAndSurvivesPanic(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var ran atomic.Int32
	done := make(chan struct{})
	q.Register("boom", func(ctx context.Context, j Job) Outcome {
		panic("kaboom")
	})
	q.Register("ok", func(ctx context.Context, j Job) Outcome {
		if ran.Add(1) == 1 {
			close(done)
		}
		return Done()
	})
	if err := q.Enqueue(ctx, "boom", "s1", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(ctx, "ok", "s2", time.Now()); err != nil {
		t.Fatal(err)
	}

	go q.Run(ctx, 50*time.Millisecond, 2)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ok job never ran; panic in sibling job killed the loop?")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		var status, lastError string
		err := pool.QueryRow(ctx, `select status, coalesce("lastError", '') from ariw.job where kind = 'boom'`).Scan(&status, &lastError)
		if err == nil && status == "due" && lastError == "panic: kaboom" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("panicked job not rescheduled: status=%s lastError=%s err=%v", status, lastError, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
}

func TestRunClaimsOnlyFreeWorkerSlots(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	started := make(chan struct{}, 8)
	q.Register("slow", func(ctx context.Context, j Job) Outcome {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return Done()
	})
	for _, id := range []string{"s1", "s2", "s3", "s4"} {
		if err := q.Enqueue(ctx, "slow", id, time.Now().Add(-time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	go q.Run(ctx, 20*time.Millisecond, 2)

	for range 2 {
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("workers never started")
		}
	}
	time.Sleep(300 * time.Millisecond) // several poll ticks: time enough to over-claim if Run still did
	var running int
	if err := pool.QueryRow(ctx, `select count(*) from ariw.job where status = 'running'`).Scan(&running); err != nil {
		t.Fatal(err)
	}
	if running != 2 {
		t.Fatalf("%d jobs marked running while 2 workers are busy; claims must match free slots", running)
	}

	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		var done int
		if err := pool.QueryRow(ctx, `select count(*) from ariw.job where status = 'done'`).Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backlog never drained: %d of 4 done", done)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHeartbeatShieldsLiveJobsFromReaper(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()

	if err := q.Enqueue(ctx, "enrich", "longrun", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claimed, err := q.claim(ctx, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %+v", err, claimed)
	}
	q.track(claimed[0].Id)
	defer q.untrack(claimed[0].Id)

	if _, err := pool.Exec(ctx,
		`update ariw.job set "claimedAt" = now() - interval '20 minutes' where id = $1`, claimed[0].Id); err != nil {
		t.Fatal(err)
	}
	q.heartbeatInflight(ctx)
	q.reapStuckRunning(ctx)

	var status string
	if err := pool.QueryRow(ctx, `select status from ariw.job where id = $1`, claimed[0].Id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("live heartbeated job was re-driven to %q; it would have run twice", status)
	}
}

func TestStaleSettleCannotStompReclaimedJob(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	a := NewQueue(pool, "worker-a")
	b := NewQueue(pool, "worker-b")

	if err := a.Enqueue(ctx, "enrich", "s1", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	claimedA, err := a.claim(ctx, 1)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("claim a: %v %+v", err, claimedA)
	}
	if _, err := pool.Exec(ctx, // simulate the reaper re-driving worker a's job
		`update ariw.job set status = 'due', "runAt" = now() where id = $1`, claimedA[0].Id); err != nil {
		t.Fatal(err)
	}
	claimedB, err := b.claim(ctx, 1)
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("claim b: %v %+v", err, claimedB)
	}

	a.settle(ctx, claimedA[0], RetryAt(time.Now().Add(time.Hour), 5, 0, "stale"))
	var status string
	var attempt int
	if err := pool.QueryRow(ctx, `select status, attempt from ariw.job where id = $1`, claimedA[0].Id).Scan(&status, &attempt); err != nil {
		t.Fatal(err)
	}
	if status != "running" || attempt != 0 {
		t.Fatalf("stale settle stomped the reclaimed job: status=%s attempt=%d", status, attempt)
	}

	b.settle(ctx, claimedB[0], Done())
	if err := pool.QueryRow(ctx, `select status from ariw.job where id = $1`, claimedB[0].Id).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("live owner's settle did not land: status=%s", status)
	}
}

func TestPurgeSettledKeepsRecentRows(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into ariw.job (kind, "submissionId", "runAt", status, "finishedAt") values
		('enrich', 'old', now(), 'done', now() - interval '31 days'),
		('enrich', 'recent', now(), 'done', now() - interval '1 day'),
		('enrich', 'live', now(), 'due', null)`)
	if err != nil {
		t.Fatal(err)
	}
	q.purgeSettled(ctx)
	var remaining int
	if err := pool.QueryRow(ctx, `select count(*) from ariw.job`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("expected the old settled row purged and 2 kept, got %d remaining", remaining)
	}
}

func TestReapStuckRunning(t *testing.T) {
	pool := testdb.New(t)
	q := NewQueue(pool, "test")
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into ariw.job (kind, "submissionId", "runAt", status, "claimedAt")
		values ('enrich', 'crashed', now() - interval '1 hour', 'running', now() - interval '20 minutes')`)
	if err != nil {
		t.Fatal(err)
	}
	q.reapStuckRunning(ctx)
	claimed, err := q.claim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].SubmissionId != "crashed" {
		t.Fatalf("stuck running job not re-driven: %+v", claimed)
	}
}
