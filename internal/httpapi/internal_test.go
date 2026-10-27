package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/integrations/githost"
	"github.com/hackclub/ari-webhooks/internal/jobs"
	"github.com/hackclub/ari-webhooks/internal/testdb"
)

type stubRepoReader struct {
	context githost.RepoContext
}

func (s *stubRepoReader) FetchFile(context.Context, string, string) githost.FileContent {
	return githost.FileContent{}
}

func (s *stubRepoReader) FetchRepoContext(context.Context, string) githost.RepoContext {
	return s.context
}

// shipFixture stands up the internal API against a throwaway database holding
// one pending ship.
func shipFixture(t *testing.T, mutate func(s *Server)) (*pgxpool.Pool, *fiber.App, string) {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()

	programId := ids.Cuid()
	makerId := ids.Cuid()
	subId := ids.ShipId()
	if _, err := pool.Exec(ctx, `insert into "Program" (id, name, color) values ($1, 'P', '#000')`, programId); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `insert into "Maker" (id, email, name) values ($1, 'm@x.com', 'Mia')`, makerId); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status, track)
		values ($1, $2, 'ext', $3, 'T', 'https://github.com/a/b', 0, 'pending', 'hardware')`,
		subId, programId, makerId); err != nil {
		t.Fatal(err)
	}

	server := &Server{Pool: pool, Jobs: jobs.NewQueue(pool, "test"), InternalToken: "sekrit"}
	if mutate != nil {
		mutate(server)
	}
	return pool, server.App(), subId
}

func callInternal(t *testing.T, app *fiber.App, method, path, bearer, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var parsed map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("response body must be JSON: %v", err)
	}
	return resp.StatusCode, parsed
}

func postInternal(t *testing.T, app *fiber.App, path, bearer string) int {
	t.Helper()
	code, _ := callInternal(t, app, "POST", path, bearer, "")
	return code
}

func liveReenrichJobs(t *testing.T, pool *pgxpool.Pool, subId string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `
		select count(*) from ariw.job
		where kind = 'reenrich' and "submissionId" = $1 and status in ('due', 'running')`,
		subId).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestInternalReenrichEnqueuesOncePerLiveJob(t *testing.T) {
	pool, app, subId := shipFixture(t, nil)

	if code := postInternal(t, app, "/internal/reenrich/"+subId, "wrong"); code != 401 {
		t.Fatalf("wrong token: got %d, want 401", code)
	}
	if n := liveReenrichJobs(t, pool, subId); n != 0 {
		t.Fatalf("a rejected trigger must enqueue nothing: %d", n)
	}

	if code := postInternal(t, app, "/internal/reenrich/"+subId, "sekrit"); code != 200 {
		t.Fatalf("pending ship: got %d, want 200", code)
	}
	// Spamming resync while a capture is already queued must not stack jobs.
	if code := postInternal(t, app, "/internal/reenrich/"+subId, "sekrit"); code != 200 {
		t.Fatalf("repeat trigger: got %d, want 200", code)
	}
	if n := liveReenrichJobs(t, pool, subId); n != 1 {
		t.Fatalf("one live reenrich job per ship: %d", n)
	}
}

func TestInternalReenrichAcceptsAHeldSecondPassShip(t *testing.T) {
	pool, app, subId := shipFixture(t, nil)
	if _, err := pool.Exec(context.Background(),
		`update "Submission" set status = 'secondpass' where id = $1`, subId); err != nil {
		t.Fatal(err)
	}

	if code := postInternal(t, app, "/internal/reenrich/"+subId, "sekrit"); code != 200 {
		t.Fatalf("held second-pass ship: got %d, want 200", code)
	}
	if n := liveReenrichJobs(t, pool, subId); n != 1 {
		t.Fatalf("the trigger must enqueue the capture: %d", n)
	}
}

func TestInternalReenrichRefusesShipsNotInTheQueue(t *testing.T) {
	pool, app, subId := shipFixture(t, nil)
	if _, err := pool.Exec(context.Background(),
		`update "Submission" set status = 'approved' where id = $1`, subId); err != nil {
		t.Fatal(err)
	}

	if code := postInternal(t, app, "/internal/reenrich/"+subId, "sekrit"); code != 409 {
		t.Fatalf("decided ship: got %d, want 409", code)
	}
	if code := postInternal(t, app, "/internal/reenrich/"+ids.ShipId(), "sekrit"); code != 404 {
		t.Fatalf("unknown ship: got %d, want 404", code)
	}
	if n := liveReenrichJobs(t, pool, subId); n != 0 {
		t.Fatalf("refused triggers must enqueue nothing: %d", n)
	}
}

func TestInternalReenrichRefusesArchivedPrograms(t *testing.T) {
	pool, app, subId := shipFixture(t, nil)
	var programId string
	if err := pool.QueryRow(context.Background(), `
		update "Program" set status = 'ARCHIVED'
		where id = (select "programId" from "Submission" where id = $1)
		returning id`, subId).Scan(&programId); err != nil {
		t.Fatal(err)
	}

	code, body := callInternal(t, app, "POST", "/internal/reenrich/"+subId, "sekrit", "")
	if code != 409 || body["message"] != "This program is archived. Restore it first." {
		t.Fatalf("archived ship: got %d %v", code, body)
	}
	if code, _ := callInternal(t, app, "POST", "/internal/reenrich-program/"+programId, "sekrit", ""); code != 409 {
		t.Fatalf("archived program: got %d, want 409", code)
	}
	if n := liveReenrichJobs(t, pool, subId); n != 0 {
		t.Fatalf("an archived program must get no captures: %d", n)
	}
}

func TestInternalRejectsWrongOrMissingTokenOnEveryRoute(t *testing.T) {
	_, app, subId := shipFixture(t, nil)
	for _, path := range []string{"/internal/reenrich/" + subId, "/internal/anything/" + subId} {
		if code := postInternal(t, app, path, ""); code != 401 {
			t.Fatalf("%s missing token: got %d, want 401", path, code)
		}
		if code := postInternal(t, app, path, "wrong"); code != 401 {
			t.Fatalf("%s wrong token: got %d, want 401", path, code)
		}
	}
}

func TestInternalFailsClosedWithoutAToken(t *testing.T) {
	_, app, subId := shipFixture(t, func(s *Server) { s.InternalToken = "" })
	if code := postInternal(t, app, "/internal/reenrich/"+subId, ""); code != 401 {
		t.Fatalf("got %d, want 401", code)
	}
}

func TestInternalAnswersNotAvailableForRoutesThisBuildLacks(t *testing.T) {
	pool, app, subId := shipFixture(t, nil)
	for _, call := range [][2]string{{"POST", "/internal/somecheck/" + subId}, {"GET", "/internal/sometool"}} {
		code, body := callInternal(t, app, call[0], call[1], "sekrit", "")
		if code != 501 || body["ok"] != false || body["message"] != "This feature is not available on this deployment" {
			t.Fatalf("%s %s: %d %v", call[0], call[1], code, body)
		}
	}
	var status string
	if err := pool.QueryRow(context.Background(), `select status::text from "Submission" where id = $1`, subId).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("an unanswered call must leave the ship alone: %s", status)
	}
}
