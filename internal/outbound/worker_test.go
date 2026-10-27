package outbound

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/cryptobox"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/testdb"
)

type outboxFixture struct {
	pool      *pgxpool.Pool
	worker    *Worker
	programId string
	secret    string
}

func setupOutbox(t *testing.T, endpointUrl string) outboxFixture {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()
	codec, err := cryptobox.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	programId := ids.Cuid()
	if _, err := pool.Exec(ctx,
		`insert into "Program" (id, name, color) values ($1, 'P', '#000')`, programId); err != nil {
		t.Fatal(err)
	}
	secret := "whsec_outbound-test"
	enc, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		insert into "OutboundEndpoint" (id, "programId", url, enabled, "secretEnc", "updatedAt")
		values ($1, $2, $3, true, $4, now())`,
		ids.Cuid(), programId, endpointUrl, enc); err != nil {
		t.Fatal(err)
	}
	worker := NewWorker(pool, codec, "test-worker")
	worker.AllowPrivateDestinations = true // httptest receivers listen on 127.0.0.1, which the guard rightly blocks
	return outboxFixture{pool: pool, worker: worker, programId: programId, secret: secret}
}

func (f outboxFixture) insertDelivery(t *testing.T, payload string, ageMinutes int) string {
	t.Helper()
	id := ids.Cuid()
	var url string
	if err := f.pool.QueryRow(context.Background(),
		`select url from "OutboundEndpoint" where "programId" = $1`, f.programId).Scan(&url); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(context.Background(), `
		insert into "OutboundDelivery" (id, "programId", event, "submissionId", url, status, payload, "createdAt")
		values ($1, $2, 'review.approved', 'TEST-0000', $3, 'PENDING', $4, now() - $5 * interval '1 minute')`,
		id, f.programId, url, payload, ageMinutes); err != nil {
		t.Fatal(err)
	}
	return id
}

func (f outboxFixture) deliveryState(t *testing.T, id string) (status string, attempts int, payload *string) {
	t.Helper()
	if err := f.pool.QueryRow(context.Background(),
		`select status::text, attempts, payload from "OutboundDelivery" where id = $1`, id).
		Scan(&status, &attempts, &payload); err != nil {
		t.Fatal(err)
	}
	return
}

func TestWorkerDeliversWithValidSignature(t *testing.T) {
	body := `{"event":"review.approved","hello":"world"}`
	var gotSig, gotTs, gotId string
	var gotBody []byte
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSig = r.Header.Get("x-ari-signature")
		gotTs = r.Header.Get("x-ari-timestamp")
		gotId = r.Header.Get("x-ari-delivery-id")
		gotBody, _ = io.ReadAll(r.Body)
		close(received)
	}))
	defer srv.Close()

	f := setupOutbox(t, srv.URL)
	id := f.insertDelivery(t, body, 6) // old enough to clear the 5-minute transition horizon
	ctx := context.Background()

	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 || claimed[0] != id {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)
	select {
	case <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("receiver never got the delivery")
	}

	if string(gotBody) != body {
		t.Fatalf("body must be sent verbatim: %q", gotBody)
	}
	if gotId != id {
		t.Fatalf("delivery id header: %q", gotId)
	}
	mac := hmac.New(sha256.New, []byte(f.secret))
	mac.Write([]byte(gotTs + "." + gotId + "." + string(gotBody)))
	if gotSig != hex.EncodeToString(mac.Sum(nil)) {
		t.Fatal("signature did not verify against {ts}.{id}.{body}")
	}

	status, attempts, payload := f.deliveryState(t, id)
	if status != "DELIVERED" || attempts != 1 || payload != nil {
		t.Fatalf("state after delivery: %s attempts=%d payloadNil=%v", status, attempts, payload == nil)
	}
	var scheduleRows, deliveredEvents int
	if err := f.pool.QueryRow(ctx, `select count(*) from ariw."outboundSchedule"`).Scan(&scheduleRows); err != nil || scheduleRows != 0 {
		t.Fatalf("schedule row must be dropped after delivery: %d", scheduleRows)
	}
	if err := f.pool.QueryRow(ctx,
		`select count(*) from "ActivityEvent" where kind = 'DELIVERY' and meta->>'status' = 'DELIVERED' and meta->>'test' = 'true'`).
		Scan(&deliveredEvents); err != nil || deliveredEvents != 1 {
		t.Fatalf("delivery audit event: %d err=%v", deliveredEvents, err)
	}
}

func TestWorkerRetriesThenFails(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(500)
		w.Write([]byte(`{"error": "downstream exploded"}`))
	}))
	defer srv.Close()

	f := setupOutbox(t, srv.URL)
	id := f.insertDelivery(t, `{"x":1}`, 6)
	ctx := context.Background()

	for range 10 {
		if _, err := f.pool.Exec(ctx,
			`update ariw."outboundSchedule" set "nextAttemptAt" = now(), "leaseUntil" = '-infinity' where "deliveryId" = $1`, id); err != nil {
			t.Fatal(err)
		}
		claimed, err := f.worker.claimDue(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(claimed) == 0 && calls.Load() == 0 {
			t.Fatal("first claim found nothing")
		}
		for _, c := range claimed {
			f.worker.processDelivery(ctx, c)
		}
	}

	if calls.Load() != 10 {
		t.Fatalf("expected exactly 10 attempts, got %d", calls.Load())
	}
	status, attempts, payload := f.deliveryState(t, id)
	if status != "FAILED" || attempts != 10 || payload != nil {
		t.Fatalf("terminal state: %s attempts=%d payloadNil=%v", status, attempts, payload == nil)
	}
	var detail string
	if err := f.pool.QueryRow(ctx, `select "errorDetail" from "OutboundDelivery" where id = $1`, id).Scan(&detail); err != nil {
		t.Fatal(err)
	}
	if detail != `http 500 - {"error": "downstream exploded"}` {
		t.Fatalf("error detail must carry status and body snippet: %q", detail)
	}
	var retrying, failed int
	f.pool.QueryRow(ctx, `select count(*) from "ActivityEvent" where meta->>'status' = 'RETRYING'`).Scan(&retrying)
	f.pool.QueryRow(ctx, `select count(*) from "ActivityEvent" where meta->>'status' = 'FAILED'`).Scan(&failed)
	if retrying != 9 || failed != 1 {
		t.Fatalf("audit trail: %d retrying, %d failed", retrying, failed)
	}
}

func TestClientErrorIsTerminal(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(404)
	}))
	defer srv.Close()

	f := setupOutbox(t, srv.URL)
	id := f.insertDelivery(t, `{"x":1}`, 6)
	ctx := context.Background()

	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)

	if calls.Load() != 1 {
		t.Fatalf("a 4xx must not be retried, got %d attempts", calls.Load())
	}
	status, attempts, _ := f.deliveryState(t, id)
	if status != "FAILED" || attempts != 1 {
		t.Fatalf("4xx terminal state: %s attempts=%d", status, attempts)
	}
}

func TestRateLimitIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	f := setupOutbox(t, srv.URL)
	id := f.insertDelivery(t, `{"x":1}`, 6)
	ctx := context.Background()

	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)

	status, attempts, _ := f.deliveryState(t, id)
	if status != "PENDING" || attempts != 1 {
		t.Fatalf("429 must stay retryable: %s attempts=%d", status, attempts)
	}
}

func TestFreshRowsWaitForTransitionHorizon(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	f.insertDelivery(t, `{"x":1}`, 0) // just created: ari's in-process chain may still own it
	claimed, err := f.worker.claimDue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("fresh unscheduled rows must not be claimed for 5 minutes, got %v", claimed)
	}
}

func TestNotifySchedulesImmediateClaim(t *testing.T) {
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	defer srv.Close()

	f := setupOutbox(t, srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go f.worker.Listen(ctx, testdb.ConnString(f.pool))
	go f.worker.Run(ctx)
	time.Sleep(300 * time.Millisecond) // let LISTEN subscribe before notifying

	id := f.insertDelivery(t, `{"x":1}`, 0)
	if _, err := f.pool.Exec(ctx, `select pg_notify($1, $2)`, NotifyChannel, id); err != nil {
		t.Fatal(err)
	}

	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("notified delivery was not claimed immediately")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, _, _ := f.deliveryState(t, id)
		if status == "DELIVERED" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivery never settled: %s", status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestUnavailableEndpointHoldsThenFails(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `update "OutboundEndpoint" set enabled = false`); err != nil {
		t.Fatal(err)
	}

	// Within the horizon the delivery is held, not failed: re-enabling the
	// endpoint must resume queued deliveries.
	id := f.insertDelivery(t, `{"x":1}`, 6)
	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)
	status, attempts, _ := f.deliveryState(t, id)
	if status != "PENDING" || attempts != 0 {
		t.Fatalf("disabled endpoint must hold the delivery: %s attempts=%d", status, attempts)
	}
	var deferred bool
	if err := f.pool.QueryRow(ctx,
		`select "nextAttemptAt" > now() + interval '5 minutes' from ariw."outboundSchedule" where "deliveryId" = $1`, id).
		Scan(&deferred); err != nil || !deferred {
		t.Fatalf("held delivery must be rescheduled into the future: deferred=%v err=%v", deferred, err)
	}

	// Past the horizon the hold gives up.
	old := f.insertDelivery(t, `{"x":2}`, 13*60)
	if _, err := f.worker.claimDue(ctx); err != nil {
		t.Fatal(err)
	}
	f.worker.processDelivery(ctx, old)
	status, _, _ = f.deliveryState(t, old)
	if status != "FAILED" {
		t.Fatalf("disabled endpoint past the horizon must fail, got %s", status)
	}
	var detail string
	f.pool.QueryRow(ctx, `select "errorDetail" from "OutboundDelivery" where id = $1`, old).Scan(&detail)
	if detail != "endpoint unavailable on resume" {
		t.Fatalf("detail: %q", detail)
	}
}

func TestArchivedProgramHoldsDeliveriesUntilRestored(t *testing.T) {
	var hits atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()
	f := setupOutbox(t, receiver.URL)
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `update "Program" set status = 'ARCHIVED' where id = $1`, f.programId); err != nil {
		t.Fatal(err)
	}

	id := f.insertDelivery(t, `{"x":1}`, 6)
	if _, err := f.worker.claimDue(ctx); err != nil {
		t.Fatal(err)
	}
	f.worker.processDelivery(ctx, id)
	status, attempts, _ := f.deliveryState(t, id)
	if hits.Load() != 0 || status != "PENDING" || attempts != 0 {
		t.Fatalf("archived program must send nothing and hold the delivery: hits=%d %s attempts=%d", hits.Load(), status, attempts)
	}

	if _, err := f.pool.Exec(ctx, `update "Program" set status = 'ACTIVE' where id = $1`, f.programId); err != nil {
		t.Fatal(err)
	}
	f.worker.processDelivery(ctx, id)
	status, _, _ = f.deliveryState(t, id)
	if hits.Load() != 1 || status != "DELIVERED" {
		t.Fatalf("restored program must deliver the held row: hits=%d %s", hits.Load(), status)
	}
}

func TestPrivateDestinationHoldsWhenGuardIsOn(t *testing.T) {
	f := setupOutbox(t, "http://10.0.0.8/hook")
	f.worker.AllowPrivateDestinations = false // exercise the real production guard
	ctx := context.Background()
	id := f.insertDelivery(t, `{"x":1}`, 6)
	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)
	status, attempts, _ := f.deliveryState(t, id)
	if status != "PENDING" || attempts != 0 {
		t.Fatalf("private destination must be held without any send, got %s attempts=%d", status, attempts)
	}
}

func TestAbandonedPastRetryHorizon(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	ctx := context.Background()
	id := f.insertDelivery(t, `{"x":1}`, 13*60)
	// One attempt was already made; past the horizon the chain is cut without
	// another send.
	if _, err := f.pool.Exec(ctx, `update "OutboundDelivery" set attempts = 1 where id = $1`, id); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)
	status, attempts, payload := f.deliveryState(t, id)
	if status != "FAILED" || attempts != 1 || payload != nil {
		t.Fatalf("12h-old attempted rows must be abandoned: %s attempts=%d", status, attempts)
	}
}

func TestOldNeverAttemptedRowStillDelivers(t *testing.T) {
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
	}))
	defer srv.Close()

	// An outbox outage longer than the horizon must not mass-fail queued
	// deliveries that were never attempted at all.
	f := setupOutbox(t, srv.URL)
	id := f.insertDelivery(t, `{"x":1}`, 13*60)
	ctx := context.Background()
	claimed, err := f.worker.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	f.worker.processDelivery(ctx, id)
	select {
	case <-received:
	case <-time.After(15 * time.Second):
		t.Fatal("never-attempted old delivery was not sent")
	}
	status, attempts, _ := f.deliveryState(t, id)
	if status != "DELIVERED" || attempts != 1 {
		t.Fatalf("state: %s attempts=%d", status, attempts)
	}
}

func TestDispatchReviewCreatesNotifiedDelivery(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	ctx := context.Background()

	makerId := ids.Cuid()
	if _, err := f.pool.Exec(ctx,
		`insert into "Maker" (id, email, name, "slackId") values ($1, 'm@x.com', 'Mia Maker', 'U77')`, makerId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, description,
		                          "repoUrl", "demoUrl", "thumbnailUrl", "hackatimeProjects",
		                          "authorNameOverrides", track, "claimedHours", status)
		values ('abcd1234', $1, 'p9', $2, 'Tide Clock', 'A tiny desk clock that shows the local tide.',
		        'https://github.com/a/tide', 'https://tide.example.com', 'https://cdn.example.com/tide.png',
		        array['tide-clock', 'tide-ui'], '{"m@x.com":"Mia Corrected"}'::jsonb,
		        'hardware', 0, 'rejected')`,
		f.programId, makerId); err != nil {
		t.Fatal(err)
	}
	decision := "rejected"
	zero := 0
	deliveryId := ids.Cuid()
	input := DispatchInput{
		DeliveryId:      deliveryId,
		Event:           "review.rejected",
		Decision:        &decision,
		ProgramId:       f.programId,
		SubmissionId:    "abcd1234",
		ReviewerId:      "system",
		Note:            "Demo URL was unreachable.",
		ApprovedMinutes: &zero,
	}
	if err := f.worker.DispatchReviewResult(ctx, input); err != nil {
		t.Fatal(err)
	}
	// A durable caller may retry after the first transaction committed but before
	// its own checkpoint did. The stable id must make that retry a no-op.
	if err := f.worker.DispatchReviewResult(ctx, input); err != nil {
		t.Fatal(err)
	}

	var event, storedId string
	var payload *string
	var scheduled bool
	err := f.pool.QueryRow(ctx, `
		select d.id, d.event, d.payload, s."deliveryId" is not null
		from "OutboundDelivery" d
		left join ariw."outboundSchedule" s on s."deliveryId" = d.id
		where d."submissionId" = 'abcd1234'`).Scan(&storedId, &event, &payload, &scheduled)
	if err != nil {
		t.Fatal(err)
	}
	if storedId != deliveryId || event != "review.rejected" || payload == nil || !scheduled {
		t.Fatalf("dispatch row: id=%s event=%s payloadNil=%v scheduled=%v", storedId, event, payload == nil, scheduled)
	}
	want := `{"event":"review.rejected","decision":"rejected","id":"abcd1234","external_id":"p9","maker":{"email":"m@x.com","name":"Mia Corrected","slack_id":"U77"},"ship":{"title":"Tide Clock","description":"A tiny desk clock that shows the local tide.","track":"hardware","thumbnail_url":"https://cdn.example.com/tide.png","authors":[{"email":"m@x.com","name":"Mia Corrected"}],"repo_url":"https://github.com/a/tide","demo_url":"https://tide.example.com","hackatime_projects":["tide-clock","tide-ui"]},"review":{"note_to_maker":"Demo URL was unreachable.","reviewer":{"email":"system@ari","slack_id":null},"approved_minutes":0,"approved_hours":0,"approved_seconds":0,"justification":{"hackatime_projects":"tide-clock, tide-ui"}}}`
	if *payload != want {
		t.Fatalf("payload shape:\n got %s\nwant %s", *payload, want)
	}
}

func TestDispatchReviewUsesCollaboratorsAsShipAuthors(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	ctx := context.Background()

	if _, err := f.pool.Exec(ctx, `
		insert into "Maker" (id, email, name) values
			('submitter', 'submitter@x.com', 'Submitter'),
			('maker-a', 'alpha@x.com', 'Alpha'),
			('maker-b', 'beta@x.com', null)`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status)
		values ('collabship', $1, 'collab-ext', 'submitter', 'Shared ship', 'https://github.com/a/shared', 0, 'rejected')`,
		f.programId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "SubmissionCollaborator" (id, "submissionId", "makerId") values
			('collab-a', 'collabship', 'maker-a'),
			('collab-b', 'collabship', 'maker-b')`); err != nil {
		t.Fatal(err)
	}

	decision := "rejected"
	f.worker.DispatchReview(ctx, DispatchInput{
		Event:        "review.rejected",
		Decision:     &decision,
		ProgramId:    f.programId,
		SubmissionId: "collabship",
		ReviewerId:   "system",
		Note:         "Not ready yet.",
	})

	var raw string
	if err := f.pool.QueryRow(ctx,
		`select payload from "OutboundDelivery" where "submissionId" = 'collabship'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var payload webhookPayload
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatal(err)
	}
	want := []shipAuthorRef{
		{Email: "alpha@x.com", Name: "Alpha"},
		{Email: "beta@x.com", Name: "beta@x.com"},
	}
	if !reflect.DeepEqual(payload.Ship.Authors, want) {
		t.Fatalf("ship authors: got %#v want %#v", payload.Ship.Authors, want)
	}
	if len(payload.Collaborators) != 2 || payload.Collaborators[1].Name != "beta@x.com" {
		t.Fatalf("collaborator names: %#v", payload.Collaborators)
	}
}

func dispatchedBlocks(t *testing.T, f outboxFixture, input DispatchInput) (review, collaborators string) {
	t.Helper()
	ctx := context.Background()
	if _, err := f.pool.Exec(ctx, `
		insert into "Maker" (id, email, name) values
			('submitter', 'submitter@x.com', 'Submitter'),
			('maker-a', 'alpha@x.com', 'Alpha'),
			('maker-b', 'beta@x.com', 'Beta')`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status)
		values ('settled', $1, 'settled-ext', 'submitter', 'Settled ship', 'https://github.com/a/settled', 0, 'approved')`,
		f.programId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "SubmissionCollaborator" (id, "submissionId", "makerId") values
			('collab-a', 'settled', 'maker-a'),
			('collab-b', 'settled', 'maker-b')`); err != nil {
		t.Fatal(err)
	}
	decision := "approved"
	input.Event = "review.approved"
	input.Decision = &decision
	input.ProgramId = f.programId
	input.SubmissionId = "settled"
	input.ReviewerId = "system"
	if err := f.worker.DispatchReviewResult(ctx, input); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := f.pool.QueryRow(ctx,
		`select payload from "OutboundDelivery" where "submissionId" = 'settled'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var blocks map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &blocks); err != nil {
		t.Fatal(err)
	}
	return string(blocks["review"]), string(blocks["collaborators"])
}

func TestDispatchSecondsSettlementDerivesLegacyFields(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	approvedSeconds := 7288
	ignoredMinutes := 999
	review, collaborators := dispatchedBlocks(t, f, DispatchInput{
		ApprovedMinutes:  &ignoredMinutes,
		ApprovedSeconds:  &approvedSeconds,
		SecondsBreakdown: &SourceSeconds{Hackatime: 5429, Journals: 1830, Lapse: 29, Program: 0},
		CollaboratorSeconds: map[string]CollaboratorSeconds{
			"maker-a": {Total: 5459, Sources: SourceSeconds{Hackatime: 5429, Journals: 30}},
			"maker-b": {Total: 1829, Sources: SourceSeconds{Journals: 1800, Lapse: 29}},
		},
	})
	wantReview := `{"note_to_maker":"","reviewer":{"email":"system@ari","slack_id":null},"approved_minutes":121,"approved_hours":2,"approved_seconds":7288,"minutes_breakdown":{"hackatime":90,"journals":30,"lapse":1,"program":0},"seconds_breakdown":{"hackatime":5429,"journals":1830,"lapse":29,"program":0}}`
	if review != wantReview {
		t.Fatalf("review block:\n got %s\nwant %s", review, wantReview)
	}
	wantCollaborators := `[{"email":"alpha@x.com","name":"Alpha","slack_id":null,"hackatime_id":null,"approved_minutes":91,"approved_hours":1.5,"approved_seconds":5459,"minutes_breakdown":{"hackatime":90,"journals":1,"lapse":0,"program":0},"seconds_breakdown":{"hackatime":5429,"journals":30,"lapse":0,"program":0}},` +
		`{"email":"beta@x.com","name":"Beta","slack_id":null,"hackatime_id":null,"approved_minutes":30,"approved_hours":0.5,"approved_seconds":1829,"minutes_breakdown":{"hackatime":0,"journals":30,"lapse":0,"program":0},"seconds_breakdown":{"hackatime":0,"journals":1800,"lapse":29,"program":0}}]`
	if collaborators != wantCollaborators {
		t.Fatalf("collaborators:\n got %s\nwant %s", collaborators, wantCollaborators)
	}
}

func TestDispatchMinuteSettlementOnlyGainsSeconds(t *testing.T) {
	f := setupOutbox(t, "https://hooks.example.com/x")
	approvedMinutes := 121
	review, collaborators := dispatchedBlocks(t, f, DispatchInput{
		ApprovedMinutes:  &approvedMinutes,
		MinutesBreakdown: map[string]int{"hackatime": 90, "journals": 30, "lapse": 1, "program": 0},
		CollaboratorMinutes: map[string]CollaboratorMinutes{
			"maker-a": {Total: 91, Hackatime: 90, Journals: 1},
			"maker-b": {Total: 30, Journals: 29, Lapse: 1},
		},
	})
	wantReview := `{"note_to_maker":"","reviewer":{"email":"system@ari","slack_id":null},"approved_minutes":121,"approved_hours":2,"approved_seconds":7260,"minutes_breakdown":{"hackatime":90,"journals":30,"lapse":1,"program":0},"seconds_breakdown":{"hackatime":5400,"journals":1800,"lapse":60,"program":0}}`
	if review != wantReview {
		t.Fatalf("review block:\n got %s\nwant %s", review, wantReview)
	}
	wantCollaborators := `[{"email":"alpha@x.com","name":"Alpha","slack_id":null,"hackatime_id":null,"approved_minutes":91,"approved_hours":1.5,"approved_seconds":5460,"minutes_breakdown":{"hackatime":90,"journals":1,"lapse":0,"program":0},"seconds_breakdown":{"hackatime":5400,"journals":60,"lapse":0,"program":0}},` +
		`{"email":"beta@x.com","name":"Beta","slack_id":null,"hackatime_id":null,"approved_minutes":30,"approved_hours":0.5,"approved_seconds":1800,"minutes_breakdown":{"hackatime":0,"journals":29,"lapse":1,"program":0},"seconds_breakdown":{"hackatime":0,"journals":1740,"lapse":60,"program":0}}]`
	if collaborators != wantCollaborators {
		t.Fatalf("collaborators:\n got %s\nwant %s", collaborators, wantCollaborators)
	}
}

func TestShipAuthorNameOverrides(t *testing.T) {
	overrides := decodeAuthorNameOverrides([]byte(`{
		"MAKER@X.COM": " Corrected Name ",
		"blank@x.com": "  ",
		"number@x.com": 42
	}`))
	if len(overrides) != 1 || overrides["maker@x.com"] != "Corrected Name" {
		t.Fatalf("decoded overrides: %#v", overrides)
	}
	stored := "Stored Name"
	if got := shipAuthorName(overrides, "Maker@X.com", &stored); got != "Corrected Name" {
		t.Fatalf("override name: %q", got)
	}
	if got := shipAuthorName(overrides, "other@x.com", &stored); got != stored {
		t.Fatalf("stored name: %q", got)
	}
	if got := shipAuthorName(overrides, "fallback@x.com", nil); got != "fallback@x.com" {
		t.Fatalf("email fallback: %q", got)
	}
	if got := decodeAuthorNameOverrides([]byte(`[]`)); got != nil {
		t.Fatalf("non-object override: %#v", got)
	}
}
