package sweep

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/autoreject"
	"github.com/hackclub/ari-webhooks/internal/cryptobox"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/integrations/vmplat"
	"github.com/hackclub/ari-webhooks/internal/outbound"
	"github.com/hackclub/ari-webhooks/internal/testdb"
)

func seedProgramAndUser(t *testing.T, pool *pgxpool.Pool) (programId, userId string) {
	t.Helper()
	ctx := context.Background()
	programId = ids.Cuid()
	userId = ids.Cuid()
	if _, err := pool.Exec(ctx, `insert into "Program" (id, name, color) values ($1, 'P', '#000')`, programId); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`insert into "User" (id, email, name, "avatarColor") values ($1, $2, 'R', '#111')`,
		userId, userId+"@x.com"); err != nil {
		t.Fatal(err)
	}
	return
}

func seedShip(t *testing.T, pool *pgxpool.Pool, programId, status string, ingestedAgo string) string {
	t.Helper()
	ctx := context.Background()
	makerId := ids.Cuid()
	subId := ids.ShipId()
	if _, err := pool.Exec(ctx, `insert into "Maker" (id, email) values ($1, $2)`, makerId, makerId+"@x.com"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status, "ingestedAt")
		values ($1, $2, $3, $4, 'T', 'https://github.com/a/b', 0, $5::"SubmissionStatus", now() - $6::interval)`,
		subId, programId, "ext-"+subId, makerId, status, ingestedAgo); err != nil {
		t.Fatal(err)
	}
	return subId
}

func TestReapStaleClaims(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	programId, userId := seedProgramAndUser(t, pool)

	idleOpen := seedShip(t, pool, programId, "pending", "1 hour")
	freshClaim := seedShip(t, pool, programId, "pending", "1 hour")
	decidedStale := seedShip(t, pool, programId, "approved", "1 hour")
	decidedFresh := seedShip(t, pool, programId, "rejected", "1 hour")
	for _, c := range []struct {
		id  string
		ago string
	}{{idleOpen, "45 minutes"}, {freshClaim, "1 minute"}, {decidedStale, "45 minutes"}, {decidedFresh, "1 minute"}} {
		if _, err := pool.Exec(ctx, `
			update "Submission" set "claimedById" = $2, "claimedAt" = now() - $3::interval where id = $1`,
			c.id, userId, c.ago); err != nil {
			t.Fatal(err)
		}
	}
	for _, subId := range []string{idleOpen, decidedStale, decidedFresh} {
		if _, err := pool.Exec(ctx, `
			insert into "SubmissionOpen" (id, "submissionId", "reviewerId", "openedAt")
			values ($1, $2, $3, now() - interval '45 minutes')`,
			ids.Cuid(), subId, userId); err != nil {
			t.Fatal(err)
		}
	}

	if err := ReapStaleClaims(ctx, pool); err != nil {
		t.Fatal(err)
	}

	var stillClaimed string
	if err := pool.QueryRow(ctx, `select id from "Submission" where "claimedById" is not null`).Scan(&stillClaimed); err != nil {
		t.Fatal(err)
	}
	if stillClaimed != freshClaim {
		t.Fatalf("only the fresh claim on an open ship must survive, got %s", stillClaimed)
	}
	assertCloseReason := func(subId, want string) {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx,
			`select "closeReason" from "SubmissionOpen" where "submissionId" = $1 and "closedAt" is not null`, subId).
			Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("session on %s must be closed with reason %s: %q", subId, want, got)
		}
	}
	assertCloseReason(idleOpen, "idle")
	// Claims on ships no longer open are cleared regardless of age, and their
	// sessions must close with them - a cleared claim leaves nothing else able
	// to find the open row.
	assertCloseReason(decidedStale, "left")
	assertCloseReason(decidedFresh, "left")
}

func TestReapStuckSubmissions(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	programId, _ := seedProgramAndUser(t, pool)
	codec, _ := cryptobox.New(make([]byte, 32))
	reject := &autoreject.Service{Pool: pool, Outbound: outbound.NewWorker(pool, codec, "test")}

	wedged := seedShip(t, pool, programId, "processing", "7 hours")
	young := seedShip(t, pool, programId, "processing", "1 hour")
	backdated := seedShip(t, pool, programId, "processing", "1 minute")
	if _, err := pool.Exec(ctx,
		`update "Submission" set "receivedAt" = now() - interval '30 days' where id = $1`, backdated); err != nil {
		t.Fatal(err)
	}

	if err := ReapStuckSubmissions(ctx, pool, reject); err != nil {
		t.Fatal(err)
	}

	assertStatus := func(id, want string) {
		t.Helper()
		var got string
		if err := pool.QueryRow(ctx, `select status::text from "Submission" where id = $1`, id).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("submission %s: status %s, want %s", id, got, want)
		}
	}
	assertStatus(wedged, "rejected")
	assertStatus(young, "processing")
	assertStatus(backdated, "processing") // receivedAt is backdatable; only ingestedAt decides wedged

	var audit string
	if err := pool.QueryRow(ctx,
		`select "auditNote" from "Review" where "submissionId" = $1`, wedged).Scan(&audit); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(audit, "stuck in processing for over 6 hours") || !strings.Contains(audit, "Last error: stuck in processing for 7.0h") {
		t.Fatalf("audit note: %q", audit)
	}
}

func TestReapStuckSubmissionsSkipsArchivedPrograms(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	programId, _ := seedProgramAndUser(t, pool)
	codec, _ := cryptobox.New(make([]byte, 32))
	reject := &autoreject.Service{Pool: pool, Outbound: outbound.NewWorker(pool, codec, "test")}
	wedged := seedShip(t, pool, programId, "processing", "7 hours")
	if _, err := pool.Exec(ctx, `update "Program" set status = 'ARCHIVED' where id = $1`, programId); err != nil {
		t.Fatal(err)
	}

	if err := ReapStuckSubmissions(ctx, pool, reject); err != nil {
		t.Fatal(err)
	}

	var status string
	if err := pool.QueryRow(ctx, `select status::text from "Submission" where id = $1`, wedged).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "processing" {
		t.Fatalf("an archived program's ship must be left alone, got %s", status)
	}
}

func TestVmReaper(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	programId, activeUser := seedProgramAndUser(t, pool)
	_, idleUser := seedProgramAndUser(t, pool)
	if _, err := pool.Exec(ctx,
		`update "User" set "lastSeenAt" = now() - interval '2 hours' where id = $1`, idleUser); err != nil {
		t.Fatal(err)
	}

	var deleted atomic.Int32
	var failNext atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failNext.Load() {
			w.WriteHeader(502)
			return
		}
		deleted.Add(1)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	reaper := &VmReaper{Pool: pool, Vm: &vmplat.Client{BaseUrl: srv.URL, Token: "tok"}}

	shipA := seedShip(t, pool, programId, "pending", "1 hour")
	shipB := seedShip(t, pool, programId, "pending", "1 hour")
	shipC := seedShip(t, pool, programId, "pending", "1 hour")
	seedVm := func(subId, userId string, vmid int, age string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			insert into "ReviewerVm" (id, "submissionId", "reviewerId", "programId", vmid, "vmType", name, "guacUrl", "createdAt")
			values ($1, $2, $3, $4, $5, 'linux', 'vm', 'https://guac/x', now() - $6::interval)`,
			ids.Cuid(), subId, userId, programId, vmid, age); err != nil {
			t.Fatal(err)
		}
	}
	seedVm(shipA, idleUser, 101, "10 minutes")   // idle owner
	seedVm(shipB, activeUser, 102, "4 hours")    // expired
	seedVm(shipC, activeUser, 103, "10 minutes") // healthy

	if err := reaper.ReapStale(ctx); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `select count(*) from "ReviewerVm"`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 || deleted.Load() != 2 {
		t.Fatalf("reap: %d rows remain, %d platform deletes", remaining, deleted.Load())
	}
	var idleEvents, expiredEvents int
	pool.QueryRow(ctx, `select count(*) from "ActivityEvent" where kind = 'VM' and meta->>'reason' = 'idle'`).Scan(&idleEvents)
	pool.QueryRow(ctx, `select count(*) from "ActivityEvent" where kind = 'VM' and meta->>'reason' = 'expired'`).Scan(&expiredEvents)
	if idleEvents != 1 || expiredEvents != 1 {
		t.Fatalf("audit: %d idle, %d expired", idleEvents, expiredEvents)
	}

	failNext.Store(true)
	if _, err := pool.Exec(ctx, `update "User" set "lastSeenAt" = now() - interval '2 hours' where id = $1`, activeUser); err != nil {
		t.Fatal(err)
	}
	if err := reaper.ReapStale(ctx); err != nil {
		t.Fatal(err)
	}
	var tombstones int
	if err := pool.QueryRow(ctx, `select count(*) from "VmTombstone" where vmid = 103`).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 1 {
		t.Fatalf("unconfirmed delete must tombstone: %d", tombstones)
	}

	failNext.Store(false)
	if err := reaper.ReapTombstones(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `select count(*) from "VmTombstone"`).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 0 {
		t.Fatalf("confirmed retry must drop the tombstone: %d remain", tombstones)
	}
}
