package reingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/jobs"
	"github.com/hackclub/ari-webhooks/internal/testdb"
)

type queuedJob struct {
	kind, submissionId string
}

type fakeQueue struct {
	mu   sync.Mutex
	jobs []queuedJob
}

func (q *fakeQueue) Enqueue(_ context.Context, kind, submissionId string, _ time.Time) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, queuedJob{kind, submissionId})
	return nil
}

func (q *fakeQueue) Wake() {}

type fixture struct {
	pool      *pgxpool.Pool
	programId string
	makerId   string
}

func setup(t *testing.T) fixture {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	programId, makerId := ids.Cuid(), ids.Cuid()
	if _, err := pool.Exec(ctx,
		`insert into "Program" (id, name, color) values ($1, 'P', '#000')`, programId); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`insert into "Maker" (id, email, name) values ($1, $2, 'Mia')`, makerId, makerId+"@x.com"); err != nil {
		t.Fatal(err)
	}
	return fixture{pool: pool, programId: programId, makerId: makerId}
}

func (f fixture) seedShip(t *testing.T, id, status string, evidenceVersion int) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `
		insert into "Submission"
			(id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status, "ingestVersion")
		values ($1, $2, $1, $3, $1, 'https://github.com/a/b', 0, $4::"SubmissionStatus", 7)`,
		id, f.programId, f.makerId, status); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into ariw."submissionEvidenceVersion" ("submissionId", version)
		values ($1, $2)`, id, evidenceVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "HoursBreakdown" ("submissionId", "hackatimeMinutes", "afterLastCommitMinutes")
		values ($1, 60, 0)`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "Commit" (id, "submissionId", hash, message, "committedAt", additions, deletions, "codingSeconds")
		values ($1, $2, 'abc1234', 'ship', now(), 1, 0, 3600)`, id+"-commit", id); err != nil {
		t.Fatal(err)
	}
}

func TestSweepQueuesEveryStaleSettledShip(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "stale-pending", "pending", 1)
	f.seedShip(t, "stale-approved", "approved", 1)
	f.seedShip(t, "still-processing", "processing", 1)
	f.seedShip(t, "current", "pending", 2)
	if _, err := f.pool.Exec(context.Background(), `
		delete from ariw."submissionEvidenceVersion" where "submissionId" = 'stale-approved'`); err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}
	service := &Service{Pool: f.pool, Queue: queue, TargetEvidenceVersion: 2}

	if err := service.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.jobs) != 2 {
		t.Fatalf("queued jobs: %+v", queue.jobs)
	}
	seen := map[string]bool{}
	for _, job := range queue.jobs {
		if job.kind != "reingest" {
			t.Fatalf("unexpected job: %+v", job)
		}
		seen[job.submissionId] = true
	}
	if !seen["stale-pending"] || !seen["stale-approved"] {
		t.Fatalf("stale submissions were not both queued: %+v", queue.jobs)
	}
}

func TestSweepSkipsArchivedPrograms(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "stale-pending", "pending", 1)
	if _, err := f.pool.Exec(context.Background(),
		`update "Program" set status = 'ARCHIVED' where id = $1`, f.programId); err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}
	service := &Service{Pool: f.pool, Queue: queue, TargetEvidenceVersion: 2}

	if err := service.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.jobs) != 0 {
		t.Fatalf("an archived program must not be reprocessed: %+v", queue.jobs)
	}
}

func TestSweepQueuesProjectRevisionsOldestFirst(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "old", "approved", 1)
	f.seedShip(t, "new", "approved", 1)
	if _, err := f.pool.Exec(context.Background(), `
		update "Submission"
		set "externalId" = 'same-project', version = case id when 'old' then 1 else 2 end
		where id in ('old', 'new')`); err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{}
	service := &Service{Pool: f.pool, Queue: queue, TargetEvidenceVersion: 2}

	if err := service.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.jobs) != 1 || queue.jobs[0].submissionId != "old" {
		t.Fatalf("project revisions must be queued oldest-first: %+v", queue.jobs)
	}
}

func TestHandleAdvancesEvidenceVersionWithoutRollingBackUnchangedDecision(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "held", "secondpass", 1)
	if _, err := f.pool.Exec(context.Background(), `
		insert into "Review"
			(id, "submissionId", "reviewerId", decision, "noteToMaker", "auditNote", "fieldValues", checklist)
		values ('held-review', 'held', 'system', 'approved', '', '', '{}'::jsonb, '[]'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		Pool:                  f.pool,
		TargetEvidenceVersion: 2,
		Capture: func(context.Context, string) (bool, error) {
			return true, nil
		},
	}

	outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "held"})
	if !outcome.IsDone() {
		t.Fatalf("outcome: %+v", outcome)
	}
	var evidenceVersion, publicIngestVersion int
	var status string
	if err := f.pool.QueryRow(context.Background(), `
		select ev.version, s."ingestVersion", s.status::text
		from "Submission" s
		join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where s.id = 'held'`).Scan(&evidenceVersion, &publicIngestVersion, &status); err != nil {
		t.Fatal(err)
	}
	if evidenceVersion != 2 || publicIngestVersion != 7 || status != "secondpass" {
		t.Fatalf("evidenceVersion=%d publicIngestVersion=%d status=%s",
			evidenceVersion, publicIngestVersion, status)
	}
	var hoursChanged bool
	var fromEvidenceVersion, toEvidenceVersion int
	if err := f.pool.QueryRow(context.Background(), `
		select (meta->>'hoursChanged')::boolean,
		       (meta->>'fromEvidenceVersion')::int,
		       (meta->>'toEvidenceVersion')::int
		from "ActivityEvent"
		where "submissionId" = 'held' and meta->>'op' = 'evidence_algorithm_updated'`).
		Scan(&hoursChanged, &fromEvidenceVersion, &toEvidenceVersion); err != nil {
		t.Fatal(err)
	}
	if hoursChanged || fromEvidenceVersion != 1 || toEvidenceVersion != 2 {
		t.Fatalf("activity checkpoint: changed=%v from=%d to=%d",
			hoursChanged, fromEvidenceVersion, toEvidenceVersion)
	}
}

func TestHandleRequeuesChangedHeldDecisionAndKeepsAudit(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "changed-held", "secondpass", 1)
	if _, err := f.pool.Exec(context.Background(), `
		insert into "Review"
			(id, "submissionId", "reviewerId", decision, "noteToMaker", "auditNote", "fieldValues", checklist)
		values ('changed-review', 'changed-held', 'system', 'approved', '', '', '{}'::jsonb, '[]'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		Pool:                  f.pool,
		TargetEvidenceVersion: 2,
		Capture: func(ctx context.Context, submissionId string) (bool, error) {
			_, err := f.pool.Exec(ctx, `
				update "Commit" set "codingSeconds" = 7200 where "submissionId" = $1`, submissionId)
			return true, err
		},
	}

	if outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "changed-held"}); !outcome.IsDone() {
		t.Fatalf("outcome: %+v", outcome)
	}
	var evidenceVersion, publicIngestVersion, reviews int
	var status string
	if err := f.pool.QueryRow(context.Background(), `
		select ev.version, s."ingestVersion", s.status::text,
		       (select count(*) from "Review" where "submissionId" = s.id)
		from "Submission" s
		join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where s.id = 'changed-held'`).Scan(&evidenceVersion, &publicIngestVersion, &status, &reviews); err != nil {
		t.Fatal(err)
	}
	if evidenceVersion != 2 || publicIngestVersion != 7 || status != "pending" || reviews != 1 {
		t.Fatalf("evidenceVersion=%d publicIngestVersion=%d status=%s reviews=%d",
			evidenceVersion, publicIngestVersion, status, reviews)
	}
	var kind, fromStatus, toStatus string
	if err := f.pool.QueryRow(context.Background(), `
		select kind::text, meta->>'fromStatus', meta->>'toStatus'
		from "ActivityEvent"
		where "submissionId" = 'changed-held' and meta->>'op' = 'evidence_algorithm_updated'`).
		Scan(&kind, &fromStatus, &toStatus); err != nil {
		t.Fatal(err)
	}
	if kind != "REVERT" || fromStatus != "secondpass" || toStatus != "pending" {
		t.Fatalf("activity: kind=%s from=%s to=%s", kind, fromStatus, toStatus)
	}
}

func TestHandleAnnouncesRollbackOfDeliveredDecision(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "changed-approved", "approved", 1)
	var gotProgram, gotSubmission, gotStatus string
	service := &Service{
		Pool:                  f.pool,
		TargetEvidenceVersion: 2,
		Capture: func(ctx context.Context, submissionId string) (bool, error) {
			_, err := f.pool.Exec(ctx, `
				update "HoursBreakdown" set "afterLastCommitMinutes" = 30 where "submissionId" = $1`, submissionId)
			return true, err
		},
		OnDecisionInvalidated: func(_ context.Context, in DecisionInvalidation) error {
			gotProgram, gotSubmission, gotStatus = in.ProgramId, in.SubmissionId, in.PriorStatus
			if in.Event != "review.requeued" || in.TargetStatus != "pending" || in.ActivityEventId == "" {
				t.Fatalf("invalidation: %+v", in)
			}
			return nil
		},
	}

	if outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "changed-approved"}); !outcome.IsDone() {
		t.Fatalf("outcome: %+v", outcome)
	}
	if gotProgram != f.programId || gotSubmission != "changed-approved" || gotStatus != "approved" {
		t.Fatalf("rollback callback: %q %q %q", gotProgram, gotSubmission, gotStatus)
	}
}

func TestHandleRevertsChangedDecisionWhenAnotherRevisionIsOpen(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "old-approved", "approved", 1)
	f.seedShip(t, "new-open", "pending", 2)
	if _, err := f.pool.Exec(context.Background(), `
		update "Submission"
		set "externalId" = 'same-project', version = case id when 'old-approved' then 1 else 2 end
		where id in ('old-approved', 'new-open')`); err != nil {
		t.Fatal(err)
	}
	var invalidation DecisionInvalidation
	service := &Service{
		Pool:                  f.pool,
		TargetEvidenceVersion: 2,
		Capture: func(ctx context.Context, submissionId string) (bool, error) {
			_, err := f.pool.Exec(ctx, `
				update "HoursBreakdown" set "afterLastCommitMinutes" = 30 where "submissionId" = $1`, submissionId)
			return true, err
		},
		OnDecisionInvalidated: func(_ context.Context, in DecisionInvalidation) error {
			invalidation = in
			return nil
		},
	}

	if outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "old-approved"}); !outcome.IsDone() {
		t.Fatalf("outcome: %+v", outcome)
	}
	var evidenceVersion, publicIngestVersion int
	var status string
	if err := f.pool.QueryRow(context.Background(), `
		select ev.version, s."ingestVersion", s.status::text
		from "Submission" s
		join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where s.id = 'old-approved'`).
		Scan(&evidenceVersion, &publicIngestVersion, &status); err != nil {
		t.Fatal(err)
	}
	if evidenceVersion != 2 || publicIngestVersion != 7 || status != "reverted" {
		t.Fatalf("evidenceVersion=%d publicIngestVersion=%d status=%s",
			evidenceVersion, publicIngestVersion, status)
	}
	if invalidation.Event != "review.reverted" || invalidation.TargetStatus != "reverted" {
		t.Fatalf("invalidation: %+v", invalidation)
	}
}

func TestSweepRetriesDecisionNotificationCheckpoint(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "notify", "approved", 1)
	callbackCalls := 0
	service := &Service{
		Pool:                  f.pool,
		Queue:                 &fakeQueue{},
		TargetEvidenceVersion: 2,
		Capture: func(ctx context.Context, submissionId string) (bool, error) {
			_, err := f.pool.Exec(ctx, `
				update "HoursBreakdown" set "afterLastCommitMinutes" = 30 where "submissionId" = $1`, submissionId)
			return true, err
		},
		OnDecisionInvalidated: func(context.Context, DecisionInvalidation) error {
			callbackCalls++
			return errors.New("outbox unavailable")
		},
	}

	if outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "notify"}); !outcome.IsDone() {
		t.Fatalf("outcome: %+v", outcome)
	}
	var complete bool
	if err := f.pool.QueryRow(context.Background(), `
		select (meta->>'outboundComplete')::boolean from "ActivityEvent"
		where "submissionId" = 'notify' and meta->>'op' = 'evidence_algorithm_updated'`).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete || callbackCalls != 1 {
		t.Fatalf("complete=%v callbackCalls=%d", complete, callbackCalls)
	}

	service.OnDecisionInvalidated = func(_ context.Context, in DecisionInvalidation) error {
		callbackCalls++
		if in.Event != "review.requeued" || in.ActivityEventId == "" {
			t.Fatalf("retry invalidation: %+v", in)
		}
		return nil
	}
	if err := service.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.QueryRow(context.Background(), `
		select (meta->>'outboundComplete')::boolean from "ActivityEvent"
		where "submissionId" = 'notify' and meta->>'op' = 'evidence_algorithm_updated'`).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if !complete || callbackCalls != 2 {
		t.Fatalf("complete=%v callbackCalls=%d", complete, callbackCalls)
	}
}

func TestHandleRetriesIncompleteCaptureWithoutAdvancingEvidenceVersion(t *testing.T) {
	f := setup(t)
	f.seedShip(t, "incomplete", "pending", 1)
	service := &Service{
		Pool:                  f.pool,
		TargetEvidenceVersion: 2,
		Capture: func(context.Context, string) (bool, error) {
			return false, nil
		},
	}

	outcome := service.Handle(context.Background(), jobs.Job{SubmissionId: "incomplete"})
	if !outcome.IsRetry() {
		t.Fatalf("outcome: %+v", outcome)
	}
	var evidenceVersion, publicIngestVersion int
	if err := f.pool.QueryRow(context.Background(),
		`select ev.version, s."ingestVersion"
		 from "Submission" s
		 join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		 where s.id = 'incomplete'`).Scan(&evidenceVersion, &publicIngestVersion); err != nil {
		t.Fatal(err)
	}
	if evidenceVersion != 1 || publicIngestVersion != 7 {
		t.Fatalf("incomplete capture changed versions: evidence=%d public=%d",
			evidenceVersion, publicIngestVersion)
	}
}

func TestFingerprintComparesMinutesLeavingVersionOneAndSecondsAfter(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	gainTwentySeconds := func(ctx context.Context, submissionId string) (bool, error) {
		_, err := f.pool.Exec(ctx, `
			update "HoursBreakdown" set "hackatimeSeconds" = "hackatimeSeconds" + 20 where "submissionId" = $1`, submissionId)
		return true, err
	}
	statusAfter := func(id string, fromVersion int) string {
		f.seedShip(t, id, "secondpass", fromVersion)
		if _, err := f.pool.Exec(ctx, `
			update "HoursBreakdown" set "hackatimeSeconds" = 3600 where "submissionId" = $1`, id); err != nil {
			t.Fatal(err)
		}
		service := &Service{Pool: f.pool, TargetEvidenceVersion: fromVersion + 1, Capture: gainTwentySeconds}
		if outcome := service.Handle(ctx, jobs.Job{SubmissionId: id}); !outcome.IsDone() {
			t.Fatalf("outcome: %+v", outcome)
		}
		var status string
		if err := f.pool.QueryRow(ctx, `select status::text from "Submission" where id = $1`, id).Scan(&status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	if status := statusAfter("minute-era", 1); status != "secondpass" {
		t.Fatalf("a version 1 ship whose minutes held must keep its decision: %s", status)
	}
	if status := statusAfter("second-era", 2); status != "pending" {
		t.Fatalf("a version 2 ship that gained seconds must return to review: %s", status)
	}
}
