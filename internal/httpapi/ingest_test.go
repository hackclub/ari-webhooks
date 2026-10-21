package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/autoreject"
	"github.com/hackclub/ari-webhooks/internal/cryptobox"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/ingest"
	"github.com/hackclub/ari-webhooks/internal/jobs"
	"github.com/hackclub/ari-webhooks/internal/outbound"
	"github.com/hackclub/ari-webhooks/internal/testdb"
)

type fixture struct {
	pool          *pgxpool.Pool
	app           *fiber.App
	programId     string
	secret        string
	followups     *[]string
	fraudWithdraw *[]string
}

func setup(t *testing.T, collaborative bool) fixture {
	t.Helper()
	pool := testdb.New(t)
	ctx := context.Background()

	codec, err := cryptobox.New(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	programId := ids.Cuid()
	if _, err := pool.Exec(ctx, `
		insert into "Program" (id, name, color, accepts, collaborative)
		values ($1, 'Test Program', '#123456', array['commits','devlog']::"Evidence"[], $2)`,
		programId, collaborative); err != nil {
		t.Fatal(err)
	}
	secret := "whsec_integration-test"
	enc, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		insert into "WebhookSecret" (id, "programId", last4, "secretEnc")
		values ($1, $2, $3, $4)`,
		ids.Cuid(), programId, secret[len(secret)-4:], enc); err != nil {
		t.Fatal(err)
	}

	var followups []string
	var fraudWithdraw []string
	var mu sync.Mutex
	service := &ingest.Service{
		Pool:  pool,
		Codec: codec,
		EnqueueEnrich: func(ctx context.Context, tx pgx.Tx, submissionId string) error {
			return jobs.EnqueueTx(ctx, tx, "enrich", submissionId, time.Now())
		},
		Followups: func(ctx context.Context, submissionId string) {
			mu.Lock()
			defer mu.Unlock()
			followups = append(followups, submissionId)
		},
		OnFraudWithdraw: func(ctx context.Context, submissionId string) {
			mu.Lock()
			defer mu.Unlock()
			fraudWithdraw = append(fraudWithdraw, submissionId)
		},
	}
	service.OnDisallowedProject = func(ctx context.Context, submissionId string) {
		(&autoreject.Service{Pool: pool, Outbound: outbound.NewWorker(pool, codec, "test")}).RequestChanges(ctx, autoreject.Input{
			SubmissionId: submissionId,
			Reason:       autoreject.DisallowedHackatimeProject,
		})
	}
	server := &Server{Pool: pool, Ingest: service}
	return fixture{pool: pool, app: server.App(), programId: programId, secret: secret, followups: &followups, fraudWithdraw: &fraudWithdraw}
}

func sign(secret, body string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	return hex.EncodeToString(mac.Sum(nil))
}

func signAt(secret string, ts int64, body string) string {
	return sign(secret, strconv.FormatInt(ts, 10)+"."+body)
}

func (f fixture) post(t *testing.T, path, body, sig string) (int, map[string]any) {
	t.Helper()
	return f.postAt(t, path, body, sig, "")
}

func (f fixture) postAt(t *testing.T, path, body, sig, timestamp string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("content-type", "application/json")
	if sig != "" {
		req.Header.Set("x-ari-signature", sig)
	}
	if timestamp != "" {
		req.Header.Set("x-ari-timestamp", timestamp)
	}
	res, err := f.app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func validShip(externalId string) string {
	return `{
		"external_id": "` + externalId + `",
		"maker": {"email": "Maker@Example.com ", "name": "Mia Maker", "slack_id": "U123", "hackatime_id": "ht_9"},
		"title": "My Project",
		"description": "A thing I made",
		"repo_url": "https://github.com/mia/proj.git/",
		"demo_url": "https://proj.example.com",
		"thumbnail_url": "https://img.example.com/t.png",
		"evidence": ["commits", "elapsed", "devlog", "bogus"],
		"hackatime_projects": [" proj ", ""],
		"journals": [{"at": "2026-06-20T10:00:00Z", "minutes": 90, "text": "did stuff"}],
		"meta": {"source": "unit-test", "shots": ["https://a.png", ""]}
	}`
}

func TestIngestEndToEnd(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	status, body := f.post(t, "/api/ingest/unknownprog", validShip("p1"), "x")
	if status != 404 || body["error"] != "unknown_program" {
		t.Fatalf("unknown program: %d %v", status, body)
	}

	ship := validShip("p1")
	status, body = f.post(t, "/api/ingest/"+f.programId, ship, "deadbeef")
	if status != 401 || body["error"] != "bad_signature" {
		t.Fatalf("bad signature: %d %v", status, body)
	}
	var badSigLogged int
	if err := f.pool.QueryRow(ctx, `select count(*) from "WebhookDelivery" where status = 'BAD_SIGNATURE'`).Scan(&badSigLogged); err != nil || badSigLogged != 1 {
		t.Fatalf("BAD_SIGNATURE delivery rows: %d err %v", badSigLogged, err)
	}

	ping := `{"external_id":"test-ping","maker":{"email":"test@ping.local"}}`
	status, body = f.post(t, "/api/ingest/"+f.programId, ping, sign(f.secret, ping))
	if status != 200 || body["status"] != "test_ok" {
		t.Fatalf("test ping: %d %v", status, body)
	}

	invalid := `{"external_id":"p1"}`
	status, body = f.post(t, "/api/ingest/"+f.programId, invalid, sign(f.secret, invalid))
	if status != 422 || body["field"] != "maker.email" {
		t.Fatalf("invalid payload: %d %v", status, body)
	}

	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 || body["status"] != "accepted" {
		t.Fatalf("accept: %d %v", status, body)
	}
	subId := body["id"].(string)

	var (
		subStatus, title, repoUrl, makerEmail, track string
		version, devlogMinutes, programMinutes       int
		evidenceVersion, publicIngestVersion         int
		accepted                                     []string
		htProjects                                   []string
	)
	err := f.pool.QueryRow(ctx, `
		select s.status::text, s.title, s."repoUrl", m.email, s.track::text, s.version,
		       h."devlogMinutes", h."programMinutes", s."acceptedEvidence"::text[], s."hackatimeProjects",
		       ev.version, s."ingestVersion"
		from "Submission" s
		join "Maker" m on m.id = s."makerId"
		join "HoursBreakdown" h on h."submissionId" = s.id
		join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where s.id = $1`, subId).
		Scan(&subStatus, &title, &repoUrl, &makerEmail, &track, &version, &devlogMinutes,
			&programMinutes, &accepted, &htProjects, &evidenceVersion, &publicIngestVersion)
	if err != nil {
		t.Fatal(err)
	}
	if subStatus != "processing" || title != "My Project" || version != 1 {
		t.Fatalf("submission row: status=%s title=%s version=%d", subStatus, title, version)
	}
	if evidenceVersion != ingest.CurrentEvidenceVersion || publicIngestVersion != 1 {
		t.Fatalf("version checkpoints: evidence=%d public ingest=%d",
			evidenceVersion, publicIngestVersion)
	}

	// The enrich job that drives the ship out of 'processing' must be committed in
	// the same transaction as the ship: an accepted ship is never left with no driver.
	var enrichJobs int
	if err := f.pool.QueryRow(ctx,
		`select count(*) from ariw.job where kind = 'enrich' and "submissionId" = $1 and status = 'due'`,
		subId).Scan(&enrichJobs); err != nil || enrichJobs != 1 {
		t.Fatalf("enrich job not enqueued atomically with the ship: count=%d err=%v", enrichJobs, err)
	}
	if repoUrl != "https://github.com/mia/proj.git" { // ari strips .git BEFORE the slash, so .git/ keeps its .git
		t.Fatalf("repo url not normalized like ari: %s", repoUrl)
	}
	if makerEmail != "maker@example.com" {
		t.Fatalf("maker email not lowercased/trimmed: %s", makerEmail)
	}
	if devlogMinutes != 90 || programMinutes != 0 {
		t.Fatalf("hours: devlog=%d program=%d", devlogMinutes, programMinutes)
	}
	if len(accepted) != 2 || accepted[0] != "commits" || accepted[1] != "devlog" {
		t.Fatalf("acceptedEvidence should intersect with program accepts: %v", accepted)
	}
	if len(htProjects) != 1 || htProjects[0] != "proj" {
		t.Fatalf("hackatime projects not trimmed/filtered: %v", htProjects)
	}
	if len(*f.followups) != 1 || (*f.followups)[0] != subId {
		t.Fatalf("followups not fired: %v", *f.followups)
	}
	var activityCount int
	if err := f.pool.QueryRow(ctx, `select count(*) from "ActivityEvent" where kind = 'WEBHOOK' and "submissionId" = $1`, subId).Scan(&activityCount); err != nil || activityCount != 1 {
		t.Fatalf("activity events: %d err %v", activityCount, err)
	}

	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 200 || body["status"] != "duplicate" || body["id"] != subId {
		t.Fatalf("retry dedup: %d %v", status, body)
	}

	changed := strings.Replace(ship, "did stuff", "did more stuff", 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, changed, sign(f.secret, changed))
	if status != 409 || body["error"] != "already_queued" || body["id"] != subId {
		t.Fatalf("open-ship conflict: %d %v", status, body)
	}

	collab := strings.Replace(validShip("p2"), `"title"`, `"collaborators": [{"email": "b@x.com", "program_hours": 2}], "title"`, 1)
	collab = strings.Replace(collab, `"text": "did stuff"`, `"text": "did stuff", "email": "b@x.com"`, 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, collab, sign(f.secret, collab))
	if status != 422 || body["error"] != "collaborators_not_enabled" {
		t.Fatalf("collaborators gate: %d %v", status, body)
	}

	withdrawBody := `{"external_id": "p1"}`
	status, body = f.post(t, "/api/ingest/"+f.programId+"/withdraw", withdrawBody, sign(f.secret, withdrawBody))
	if status != 200 || body["status"] != "withdrawn" || body["id"] != subId {
		t.Fatalf("withdraw: %d %v", status, body)
	}
	var afterWithdraw string
	if err := f.pool.QueryRow(ctx, `select status::text from "Submission" where id = $1`, subId).Scan(&afterWithdraw); err != nil || afterWithdraw != "withdrawn" {
		t.Fatalf("withdraw status: %s err %v", afterWithdraw, err)
	}
	status, body = f.post(t, "/api/ingest/"+f.programId+"/withdraw", withdrawBody, sign(f.secret, withdrawBody))
	if status != 404 || body["error"] != "not_queued" {
		t.Fatalf("second withdraw: %d %v", status, body)
	}

	// The retry dedup only matches while the deduped submission is still OPEN. Once
	// it is withdrawn (or decided, below) a byte-identical resend is a legitimate
	// resubmit and must create a FRESH submission; answering "duplicate" with the
	// closed id strands the sender's ship outside every queue.
	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 || body["id"] == subId || body["id"] == "" {
		t.Fatalf("identical resend after withdraw must open a fresh submission: %d %v", status, body)
	}
	freshId := body["id"].(string)

	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 200 || body["status"] != "duplicate" || body["id"] != freshId {
		t.Fatalf("retry dedup still holds while the fresh submission is open: %d %v", status, body)
	}

	if _, err := f.pool.Exec(ctx,
		`update "Submission" set status = 'rejected' where id = $1`, freshId); err != nil {
		t.Fatal(err)
	}
	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 || body["id"] == freshId || body["id"] == "" {
		t.Fatalf("identical resend after a decision must open a fresh submission: %d %v", status, body)
	}

	// A ship parked in second pass or fraud review is still open: a changed resend
	// must conflict like any other open ship, and a byte-identical retry must rebind
	// to it. Neither may create a duplicate submission that puts the same project in
	// two review queues at once.
	parkedId := body["id"].(string)
	for _, parked := range []string{"secondpass", "fraudreview"} {
		if _, err := f.pool.Exec(ctx,
			`update "Submission" set status = $1::"SubmissionStatus" where id = $2`, parked, parkedId); err != nil {
			t.Fatal(err)
		}
		edited := strings.Replace(ship, "did stuff", "did stuff while "+parked, 1)
		status, body = f.post(t, "/api/ingest/"+f.programId, edited, sign(f.secret, edited))
		if status != 409 || body["error"] != "already_queued" || body["id"] != parkedId {
			t.Fatalf("resend while %s must conflict, not duplicate the ship: %d %v", parked, status, body)
		}
		status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
		if status != 200 || body["status"] != "duplicate" || body["id"] != parkedId {
			t.Fatalf("identical retry while %s must rebind to the parked ship: %d %v", parked, status, body)
		}
	}
}

func TestIngestTimestampedSignature(t *testing.T) {
	f := setup(t, false)
	now := time.Now().Unix()

	ship := validShip("ts-1")
	stale := strconv.FormatInt(now-600, 10)
	status, body := f.postAt(t, "/api/ingest/"+f.programId, ship, signAt(f.secret, now-600, ship), stale)
	if status != 401 || body["error"] != "bad_signature" {
		t.Fatalf("a correctly signed request ten minutes old must be refused: %d %v", status, body)
	}
	status, body = f.postAt(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship), strconv.FormatInt(now, 10))
	if status != 401 || body["error"] != "bad_signature" {
		t.Fatalf("a body-only signature with a timestamp header must be refused: %d %v", status, body)
	}
	status, body = f.postAt(t, "/api/ingest/"+f.programId, ship, signAt(f.secret, now, ship), "soon")
	if status != 401 || body["error"] != "bad_signature" {
		t.Fatalf("a non-integer timestamp must be refused: %d %v", status, body)
	}

	status, body = f.postAt(t, "/api/ingest/"+f.programId, ship, signAt(f.secret, now, ship), strconv.FormatInt(now, 10))
	if status != 202 || body["status"] != "accepted" {
		t.Fatalf("timestamped accept: %d %v", status, body)
	}
	subId := body["id"].(string)

	withdrawBody := `{"external_id": "ts-1"}`
	status, body = f.postAt(t, "/api/ingest/"+f.programId+"/withdraw", withdrawBody, signAt(f.secret, now, withdrawBody), strconv.FormatInt(now, 10))
	if status != 200 || body["status"] != "withdrawn" || body["id"] != subId {
		t.Fatalf("timestamped withdraw: %d %v", status, body)
	}
}

func TestIngestBadSignatureAuditCap(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	ship := validShip("flood-1")
	for i := 0; i < 25; i++ {
		if status, _ := f.post(t, "/api/ingest/"+f.programId, ship, "deadbeef"); status != 401 {
			t.Fatalf("unsigned request %d: %d", i, status)
		}
	}
	var badSigRows int
	if err := f.pool.QueryRow(ctx, `select count(*) from "WebhookDelivery" where "programId" = $1 and status = 'BAD_SIGNATURE'`, f.programId).Scan(&badSigRows); err != nil {
		t.Fatal(err)
	}
	if badSigRows != 20 {
		t.Fatalf("a flood of unsigned requests must stop at the audit cap, got %d rows", badSigRows)
	}
	status, body := f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("a signed request during the flood must still be accepted: %d %v", status, body)
	}
}

func TestIngestDisallowedHackatimeProject(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	mixed := strings.Replace(validShip("mix-1"), `[" proj ", ""]`, `["<<LAST_PROJECT>>", "proj"]`, 1)
	status, body := f.post(t, "/api/ingest/"+f.programId, mixed, sign(f.secret, mixed))
	if status != 202 {
		t.Fatalf("mixed list accept: %d %v", status, body)
	}
	var subStatus string
	var htProjects []string
	if err := f.pool.QueryRow(ctx,
		`select status::text, "hackatimeProjects" from "Submission" where id = $1`,
		body["id"].(string)).Scan(&subStatus, &htProjects); err != nil {
		t.Fatal(err)
	}
	if subStatus != "processing" {
		t.Fatalf("a ship with real projects left must stay open: %s", subStatus)
	}
	if len(htProjects) != 1 || htProjects[0] != "proj" {
		t.Fatalf("the placeholder must never be stored: %v", htProjects)
	}

	only := strings.Replace(validShip("only-1"), `[" proj ", ""]`, `["<<LAST_PROJECT>>"]`, 1)
	only = strings.Replace(only, `[{"at": "2026-06-20T10:00:00Z", "minutes": 90, "text": "did stuff"}]`, `[]`, 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, only, sign(f.secret, only))
	if status != 202 {
		t.Fatalf("placeholder-only ship must be accepted so changes can be requested: %d %v", status, body)
	}
	subId := body["id"].(string)
	var decision, note string
	if err := f.pool.QueryRow(ctx,
		`select status::text, "hackatimeProjects" from "Submission" where id = $1`,
		subId).Scan(&subStatus, &htProjects); err != nil {
		t.Fatal(err)
	}
	if subStatus != "changes" {
		t.Fatalf("placeholder-only ship must get a changes request: %s", subStatus)
	}
	if len(htProjects) != 0 {
		t.Fatalf("the placeholder must never be stored: %v", htProjects)
	}
	if err := f.pool.QueryRow(ctx,
		`select decision::text, "noteToMaker" from "Review" where "submissionId" = $1`,
		subId).Scan(&decision, &note); err != nil {
		t.Fatal(err)
	}
	if decision != "changes" || !strings.Contains(note, "<<LAST_PROJECT>>") {
		t.Fatalf("system review: decision=%s note=%q", decision, note)
	}
	var changesEvents int
	if err := f.pool.QueryRow(ctx,
		`select count(*) from "ActivityEvent" where kind = 'CHANGES' and "submissionId" = $1`,
		subId).Scan(&changesEvents); err != nil || changesEvents != 1 {
		t.Fatalf("changes activity events: %d err %v", changesEvents, err)
	}

	fixed := strings.Replace(validShip("only-1"), `[" proj ", ""]`, `["proj"]`, 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, fixed, sign(f.secret, fixed))
	if status != 202 {
		t.Fatalf("reshipping after the changes request must work: %d %v", status, body)
	}
	var version int
	if err := f.pool.QueryRow(ctx,
		`select version from "Submission" where id = $1`, body["id"].(string)).Scan(&version); err != nil || version != 2 {
		t.Fatalf("reship version: %d err %v", version, err)
	}
}

func TestIngestCollaborativeShip(t *testing.T) {
	f := setup(t, true)
	ctx := context.Background()

	ship := `{
		"external_id": "shared-1",
		"maker": {"email": "lead@x.com", "name": "Lead", "slack_id": "U1"},
		"title": "Shared", "description": "d",
		"repo_url": "https://github.com/a/b",
		"demo_url": "https://demo.x.com",
		"thumbnail_url": "https://img.x.com/t.png",
		"collaborators": [
			{"email": "Lead@X.com", "program_minutes": 30},
			{"email": "pal@x.com", "name": "Pal", "program_hours": 1.5}
		],
		"journals": [
			{"at": "2026-06-19", "minutes": 60, "text": "lead work", "email": "lead@x.com"},
			{"at": "2026-06-20", "minutes": 30, "text": "pal work", "email": "PAL@x.com"}
		]
	}`
	status, body := f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("collaborative accept: %d %v", status, body)
	}
	subId := body["id"].(string)

	rows, err := f.pool.Query(ctx, `
		select m.email, c."devlogMinutes", c."programMinutes"
		from "SubmissionCollaborator" c join "Maker" m on m.id = c."makerId"
		where c."submissionId" = $1 order by m.email`, subId)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type collabRow struct {
		email                   string
		devlogMins, programMins int
	}
	var collabs []collabRow
	for rows.Next() {
		var r collabRow
		if err := rows.Scan(&r.email, &r.devlogMins, &r.programMins); err != nil {
			t.Fatal(err)
		}
		collabs = append(collabs, r)
	}
	if len(collabs) != 2 {
		t.Fatalf("collaborator rows: %+v", collabs)
	}
	if collabs[0].email != "lead@x.com" || collabs[0].devlogMins != 60 || collabs[0].programMins != 30 {
		t.Fatalf("lead row: %+v", collabs[0])
	}
	if collabs[1].email != "pal@x.com" || collabs[1].devlogMins != 30 || collabs[1].programMins != 90 {
		t.Fatalf("pal row: %+v", collabs[1])
	}

	var hoursProgram, hoursDevlog int
	if err := f.pool.QueryRow(ctx, `select "programMinutes", "devlogMinutes" from "HoursBreakdown" where "submissionId" = $1`, subId).Scan(&hoursProgram, &hoursDevlog); err != nil {
		t.Fatal(err)
	}
	if hoursProgram != 120 || hoursDevlog != 90 {
		t.Fatalf("aggregate hours: program=%d devlog=%d", hoursProgram, hoursDevlog)
	}

	var collabProgramSeconds, collabDevlogSeconds int
	if err := f.pool.QueryRow(ctx, `
		select sum("programSeconds"), sum("devlogSeconds") from "SubmissionCollaborator" where "submissionId" = $1`, subId).
		Scan(&collabProgramSeconds, &collabDevlogSeconds); err != nil {
		t.Fatal(err)
	}
	if collabProgramSeconds != 7200 || collabDevlogSeconds != 5400 {
		t.Fatalf("minute inputs are stored as minutes * 60: program=%d devlog=%d", collabProgramSeconds, collabDevlogSeconds)
	}
}

func TestIngestStoresSecondsBesideLegacyMinutes(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	ship := `{
		"external_id": "seconds-1",
		"maker": {"email": "lead@x.com", "name": "Lead", "slack_id": "U1", "program_seconds": 5429, "program_minutes": 1},
		"title": "Seconds", "description": "d",
		"repo_url": "https://github.com/a/b",
		"demo_url": "https://demo.x.com",
		"thumbnail_url": "https://img.x.com/t.png",
		"journals": [
			{"at": "2026-06-19", "seconds": 1830, "minutes": 1, "text": "exact"},
			{"at": "2026-06-20", "minutes": 12, "text": "legacy"}
		]
	}`
	status, body := f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("seconds accept: %d %v", status, body)
	}
	subId := body["id"].(string)

	var programSeconds, programMinutes, devlogSeconds, devlogMinutes int
	if err := f.pool.QueryRow(ctx, `
		select "programSeconds", "programMinutes", "devlogSeconds", "devlogMinutes"
		from "HoursBreakdown" where "submissionId" = $1`, subId).
		Scan(&programSeconds, &programMinutes, &devlogSeconds, &devlogMinutes); err != nil {
		t.Fatal(err)
	}
	if programSeconds != 5429 || programMinutes != 90 || devlogSeconds != 2550 || devlogMinutes != 43 {
		t.Fatalf("hours: program=%ds/%dm devlog=%ds/%dm", programSeconds, programMinutes, devlogSeconds, devlogMinutes)
	}

	var exactSeconds, exactMinutes, legacySeconds, legacyMinutes int
	if err := f.pool.QueryRow(ctx, `
		select max(seconds) filter (where text = 'exact'), max(minutes) filter (where text = 'exact'),
		       max(seconds) filter (where text = 'legacy'), max(minutes) filter (where text = 'legacy')
		from "Devlog" where "submissionId" = $1`, subId).
		Scan(&exactSeconds, &exactMinutes, &legacySeconds, &legacyMinutes); err != nil {
		t.Fatal(err)
	}
	if exactSeconds != 1830 || exactMinutes != 31 || legacySeconds != 720 || legacyMinutes != 12 {
		t.Fatalf("journals: exact=%ds/%dm legacy=%ds/%dm", exactSeconds, exactMinutes, legacySeconds, legacyMinutes)
	}
}

func TestIngestBackdatedShippedAt(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	ship := strings.Replace(validShip("old-1"), `"meta"`, `"shipped_at": "2026-01-15T12:00:00Z", "meta"`, 1)
	status, body := f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("backdated accept: %d %v", status, body)
	}
	var receivedAt, ingestedAt, dbNow time.Time
	if err := f.pool.QueryRow(ctx, `select "receivedAt", "ingestedAt", now()::timestamp from "Submission" where id = $1`, body["id"]).Scan(&receivedAt, &ingestedAt, &dbNow); err != nil {
		t.Fatal(err)
	}
	if !receivedAt.Equal(time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("receivedAt not backdated: %v", receivedAt)
	}
	if d := dbNow.Sub(ingestedAt); d < 0 || d > time.Minute { // wall-clock DB default, never the backdate
		t.Fatalf("ingestedAt must stay wall-clock: %v (db now %v)", ingestedAt, dbNow)
	}

	bad := strings.Replace(validShip("old-2"), `"meta"`, `"shipped_at": "not-a-date", "meta"`, 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, bad, sign(f.secret, bad))
	if status != 422 || body["field"] != "shipped_at" {
		t.Fatalf("malformed shipped_at: %d %v", status, body)
	}

	byQuery := validShip("old-3")
	status, body = f.post(t, "/api/ingest/"+f.programId+"?shipped_at=2026-02-01T00:00:00Z", byQuery, sign(f.secret, byQuery))
	if status != 202 {
		t.Fatalf("query shipped_at accept: %d %v", status, body)
	}
	if err := f.pool.QueryRow(ctx, `select "receivedAt" from "Submission" where id = $1`, body["id"]).Scan(&receivedAt); err != nil {
		t.Fatal(err)
	}
	if !receivedAt.Equal(time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("query shipped_at not applied: %v", receivedAt)
	}
}

func TestIngestConcurrentSameProject(t *testing.T) {
	f := setup(t, false)

	bodyA := validShip("race-1")
	bodyB := strings.Replace(validShip("race-1"), "did stuff", "unique variant", 1)
	var wg sync.WaitGroup
	statuses := make([]int, 2)
	for i, b := range []string{bodyA, bodyB} {
		wg.Add(1)
		go func(i int, b string) {
			defer wg.Done()
			statuses[i], _ = f.post(t, "/api/ingest/"+f.programId, b, sign(f.secret, b))
		}(i, b)
	}
	wg.Wait()
	if !(statuses[0] == 202 && statuses[1] == 409) && !(statuses[0] == 409 && statuses[1] == 202) {
		t.Fatalf("concurrent same-project ingests must produce one 202 and one 409, got %v", statuses)
	}
}

func TestWithdrawFraudReviewShipFiresPurge(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	makerId := ids.Cuid()
	if _, err := f.pool.Exec(ctx,
		`insert into "Maker" (id, email, name) values ($1, 'fr@x.com', 'Fran')`, makerId); err != nil {
		t.Fatal(err)
	}
	subId := ids.Cuid()
	if _, err := f.pool.Exec(ctx, `
		insert into "Submission" (id, "programId", "externalId", "makerId", title, "repoUrl", "claimedHours", status, track, "hackatimeProjects")
		values ($1, $2, 'fr-ext', $3, 'T', 'https://github.com/a/b', 0, 'fraudreview'::"SubmissionStatus", 'software'::"Track", array['proj'])`,
		subId, f.programId, makerId); err != nil {
		t.Fatal(err)
	}

	withdrawBody := `{"external_id": "fr-ext"}`
	status, body := f.post(t, "/api/ingest/"+f.programId+"/withdraw", withdrawBody, sign(f.secret, withdrawBody))
	if status != 200 || body["status"] != "withdrawn" {
		t.Fatalf("a fraud-review ship must be withdrawable: %d %v", status, body)
	}
	var subStatus string
	if err := f.pool.QueryRow(ctx, `select status::text from "Submission" where id = $1`, subId).Scan(&subStatus); err != nil || subStatus != "withdrawn" {
		t.Fatalf("withdraw status: %s err %v", subStatus, err)
	}
	if len(*f.fraudWithdraw) != 1 || (*f.fraudWithdraw)[0] != subId {
		t.Fatalf("withdrawing a fraud-review ship must tell the fraud gateway: %v", *f.fraudWithdraw)
	}
}

func (f fixture) getStatus(t *testing.T, query, bearer string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/ingest/"+f.programId+"/status?"+query, nil)
	if bearer != "" {
		req.Header.Set("authorization", "Bearer "+bearer)
	}
	res, err := f.app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func TestShipStatusPhases(t *testing.T) {
	f := setup(t, false)
	ctx := context.Background()

	ship := validShip("ph-1")
	status, body := f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("ingest: %d %v", status, body)
	}
	subId := body["id"].(string)

	status, body = f.getStatus(t, "external_id=ph-1", "wrong-secret")
	if status != 401 || body["error"] != "unauthorized" {
		t.Fatalf("bad bearer: %d %v", status, body)
	}
	status, body = f.getStatus(t, "external_id=ph-1", "")
	if status != 401 {
		t.Fatalf("missing bearer: %d %v", status, body)
	}
	status, body = f.getStatus(t, "", f.secret)
	if status != 422 || body["field"] != "external_id" {
		t.Fatalf("missing ids: %d %v", status, body)
	}
	status, body = f.getStatus(t, "external_id=never-shipped", f.secret)
	if status != 404 || body["error"] != "not_found" {
		t.Fatalf("unknown project: %d %v", status, body)
	}

	req := httptest.NewRequest("GET", "/api/ingest/unknownprog/status?external_id=ph-1", nil)
	req.Header.Set("authorization", "Bearer "+f.secret)
	res, err := f.app.Test(req, fiber.TestConfig{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("unknown program: %d", res.StatusCode)
	}

	expectPhase := func(query, phase string, decision any) {
		t.Helper()
		status, body := f.getStatus(t, query, f.secret)
		if status != 200 || body["phase"] != phase || body["decision"] != decision {
			t.Fatalf("query %q: want phase=%s decision=%v, got %d %v", query, phase, decision, status, body)
		}
		if body["id"] != subId || body["external_id"] != "ph-1" || body["version"] != float64(1) {
			t.Fatalf("query %q: identity fields wrong: %v", query, body)
		}
	}

	setStatus := func(s string) {
		t.Helper()
		if _, err := f.pool.Exec(ctx,
			`update "Submission" set status = $1::"SubmissionStatus" where id = $2`, s, subId); err != nil {
			t.Fatal(err)
		}
	}

	expectPhase("external_id=ph-1", "processing", nil)
	expectPhase("id="+subId, "processing", nil)

	setStatus("pending")
	expectPhase("external_id=ph-1", "review", nil)

	reviewerId := ids.Cuid()
	if _, err := f.pool.Exec(ctx, `
		insert into "User" (id, email, name, "avatarColor")
		values ($1, 'rev@x.com', 'Rev', '#000000')`, reviewerId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx, `
		insert into "Review" (id, "submissionId", "reviewerId", decision, "noteToMaker", "auditNote", "fieldValues", checklist)
		values ($1, $2, $3, 'approved', 'The firmware looks good', 'Hours check out', '{}', '[]')`,
		ids.Cuid(), subId, reviewerId); err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(ctx,
		`update "Submission" set "claimedById" = $1, "claimedAt" = now() where id = $2`, reviewerId, subId); err != nil {
		t.Fatal(err)
	}
	expectPhase("external_id=ph-1", "under_review", nil)

	setStatus("fraudreview")
	expectPhase("external_id=ph-1", "fraud_review", nil)

	setStatus("secondpass")
	expectPhase("external_id=ph-1", "second_pass", nil)

	setStatus("changes") // still claimed: a reviewer re-opened it
	expectPhase("external_id=ph-1", "under_review", "changes")
	if _, err := f.pool.Exec(ctx,
		`update "Submission" set "claimedById" = null, "claimedAt" = null where id = $1`, subId); err != nil {
		t.Fatal(err)
	}
	expectPhase("external_id=ph-1", "reviewed", "changes")

	setStatus("approved")
	expectPhase("external_id=ph-1", "reviewed", "approved")

	_, body = f.getStatus(t, "external_id=ph-1", f.secret)
	review, ok := body["review"].(map[string]any)
	if !ok {
		t.Fatalf("approved ship must carry the review: %v", body)
	}
	if review["note_to_maker"] != "The firmware looks good" || review["audit_note"] != "Hours check out" {
		t.Fatalf("review wording: %v", review)
	}
	if reviewer, _ := review["reviewer"].(map[string]any); reviewer == nil || reviewer["email"] != "rev@x.com" {
		t.Fatalf("review reviewer: %v", review["reviewer"])
	}

	setStatus("rejected")
	expectPhase("external_id=ph-1", "reviewed", "rejected")

	setStatus("withdrawn")
	expectPhase("external_id=ph-1", "withdrawn", nil)

	setStatus("reverted")
	expectPhase("external_id=ph-1", "reverted", nil)

	_, body = f.getStatus(t, "external_id=ph-1", f.secret)
	if _, present := body["review"]; present {
		t.Fatalf("a ship without a decision must not carry a review: %v", body)
	}

	// A re-ship of the same project: external_id lookup must resolve to the
	// latest version, while the old ship stays reachable by id. The body must
	// differ from the first delivery or the 1h byte-identical dedup returns it.
	ship = strings.Replace(ship, "My Project", "My Project v2", 1)
	status, body = f.post(t, "/api/ingest/"+f.programId, ship, sign(f.secret, ship))
	if status != 202 {
		t.Fatalf("re-ingest: %d %v", status, body)
	}
	status, body = f.getStatus(t, "external_id=ph-1", f.secret)
	if status != 200 || body["version"] != float64(2) || body["phase"] != "processing" {
		t.Fatalf("latest ship: %d %v", status, body)
	}
	status, body = f.getStatus(t, "id="+subId, f.secret)
	if status != 200 || body["version"] != float64(1) || body["phase"] != "reverted" {
		t.Fatalf("old ship by id: %d %v", status, body)
	}
}
