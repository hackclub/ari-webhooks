package ingest

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/cryptobox"
	"github.com/hackclub/ari-webhooks/internal/signature"
)

type Result struct {
	Status int
	Body   map[string]any
}

type Service struct {
	Pool  *pgxpool.Pool
	Codec *cryptobox.Codec
	// EnqueueEnrich schedules the enrich job inside the ship-creation transaction,
	// so an accepted ship and the job that promotes it out of 'processing' commit
	// atomically. A crash or enqueue error can never strand a ship with no driver.
	// Optional: nil skips it (tests that don't exercise the pipeline).
	EnqueueEnrich func(ctx context.Context, tx pgx.Tx, submissionId string) error
	// Followups schedules the auxiliary autocheck/screen jobs after commit. A lost
	// enqueue here degrades a check but never wedges the ship, so it stays best-effort.
	Followups           func(ctx context.Context, submissionId string)
	OnFraudWithdraw     func(ctx context.Context, submissionId string)
	OnDisallowedProject func(ctx context.Context, submissionId string)
}

func (s *Service) ProcessIngest(ctx context.Context, programId string, rawBody []byte, sigHeader, timestampHeader, queryShippedAt string) Result {
	program, err := s.loadProgram(ctx, programId)
	if err != nil {
		slog.Error("program load failed", "programId", programId, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}
	if program == nil || program.status == "ARCHIVED" {
		return Result{404, map[string]any{"error": "unknown_program"}}
	}

	shaBytes := sha256.Sum256(rawBody)
	sha := hex.EncodeToString(shaBytes[:])
	record := func(status string, httpStatus int, externalId, submissionId, errorDetail string) {
		s.recordDelivery(ctx, program.id, status, httpStatus, len(rawBody), sha, externalId, submissionId, errorDetail)
	}

	secret := ""
	if program.secretEnc != "" {
		secret, err = s.Codec.Decrypt(program.secretEnc)
		if err != nil {
			slog.Error("webhook secret undecryptable", "programId", programId, "err", err)
			secret = ""
		}
	}
	if secret == "" || !signature.VerifyInboundAt(secret, rawBody, sigHeader, timestampHeader, time.Now()) { // signature before any payload work
		if s.recentBadSignatures(ctx, program.id) < 20 { // past this many unsigned requests in the window the audit table stops growing: a flood must not fill it with rows nobody will read
			record("BAD_SIGNATURE", 401, "", "", "")
		}
		return Result{401, map[string]any{"error": "bad_signature"}}
	}

	if isTestPing(rawBody) { // diagnostic ping from the Webhooks page: logged, never a Submission
		record("ACCEPTED", 200, "test-ping", "", "")
		return Result{200, map[string]any{"status": "test_ok"}}
	}

	v := validate(rawBody)
	if !v.ok {
		record("INVALID", 422, "", "", "invalid:"+v.field)
		return Result{422, map[string]any{"error": "invalid_payload", "field": v.field}}
	}
	ship := v.data

	queryDate, ok := parseShippedAt(queryStringOrNil(queryShippedAt))
	if !ok { // a bad migration date must never silently fall back to now()
		record("INVALID", 422, ship.ExternalId, "", "invalid:shipped_at")
		return Result{422, map[string]any{"error": "invalid_payload", "field": "shipped_at"}}
	}
	shippedAt := ship.ShippedAt
	if shippedAt == nil {
		shippedAt = queryDate
	}

	if retryId, found := s.findRecentAccepted(ctx, program.id, sha); found { // byte-identical resend within 1h = sender retry
		record("DUPLICATE", 200, ship.ExternalId, retryId, "")
		return Result{200, map[string]any{"status": "duplicate", "id": retryId}}
	}

	if ship.Collaborators != nil && !program.collaborative { // hard 422 so the sender never believes people were recorded when they were not
		record("INVALID", 422, "", "", "collaborators_not_enabled")
		return Result{422, map[string]any{"error": "collaborators_not_enabled"}}
	}

	if openId, found := s.findOpenShip(ctx, program.id, ship.ExternalId); found {
		record("CONFLICT", 409, ship.ExternalId, openId, "already_queued")
		return Result{409, map[string]any{"error": "already_queued", "id": openId}}
	}

	acceptedEvidence := intersectEvidence(ship.Evidence, program.accepts)

	submissionId, err := s.createShip(ctx, program.id, ship, shippedAt, acceptedEvidence)
	if err != nil {
		if isOpenProjectConflict(err) { // lost the race against a concurrent delivery of the same project
			record("CONFLICT", 409, ship.ExternalId, "", "already_queued")
			return Result{409, map[string]any{"error": "already_queued"}}
		}
		slog.Error("ingest transaction failed", "programId", program.id, "externalId", ship.ExternalId, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}

	record("ACCEPTED", 202, ship.ExternalId, submissionId, "")

	if len(ship.DisallowedHackatimeProjects) > 0 && len(ship.HackatimeProjects) == 0 && s.OnDisallowedProject != nil {
		s.OnDisallowedProject(ctx, submissionId) // before Followups, so screen/autocheck see the decided status
	}

	if s.Followups != nil {
		s.Followups(ctx, submissionId)
	}
	return Result{202, map[string]any{"status": "accepted", "id": submissionId}}
}

func (s *Service) ProcessWithdraw(ctx context.Context, programId string, rawBody []byte, sigHeader, timestampHeader string) Result {
	program, err := s.loadProgram(ctx, programId)
	if err != nil {
		slog.Error("program load failed", "programId", programId, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}
	if program == nil || program.status == "ARCHIVED" {
		return Result{404, map[string]any{"error": "unknown_program"}}
	}
	secret := ""
	if program.secretEnc != "" {
		secret, _ = s.Codec.Decrypt(program.secretEnc)
	}
	if secret == "" || !signature.VerifyInboundAt(secret, rawBody, sigHeader, timestampHeader, time.Now()) { // same HMAC contract as ingest
		return Result{401, map[string]any{"error": "bad_signature"}}
	}

	externalId := ""
	var parsed map[string]any
	if err := json.Unmarshal(rawBody, &parsed); err == nil {
		if s, ok := trimmedNonEmpty(parsed["external_id"]); ok {
			externalId = strings.TrimSpace(s)
		}
	}
	if externalId == "" {
		return Result{422, map[string]any{"error": "invalid_payload", "field": "external_id"}}
	}

	openId, title, makerEmail, found := s.findOpenShipWithMaker(ctx, program.id, externalId)
	if !found {
		return Result{404, map[string]any{"error": "not_queued"}}
	}

	withdrawn, wasFraudReview, err := s.withdrawShip(ctx, program.id, openId, title, makerEmail, externalId)
	if err != nil {
		slog.Error("withdraw failed", "programId", program.id, "submissionId", openId, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}
	if !withdrawn { // a decision landed concurrently and wins
		return Result{404, map[string]any{"error": "not_queued"}}
	}
	if wasFraudReview && s.OnFraudWithdraw != nil {
		s.OnFraudWithdraw(ctx, openId)
	}
	return Result{200, map[string]any{"status": "withdrawn", "id": openId}}
}

// ProcessStatus answers "where is this ship in the review flow" for the sender.
// Authenticated with the program's webhook secret as a bearer token (a GET has
// no body for the usual HMAC to cover). Lookup is by ship `id` (from the ingest
// 202) or by `external_id`, which resolves to the project's latest ship.
func (s *Service) ProcessStatus(ctx context.Context, programId, authHeader, shipId, externalId string) Result {
	program, err := s.loadProgram(ctx, programId)
	if err != nil {
		slog.Error("program load failed", "programId", programId, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}
	if program == nil || program.status == "ARCHIVED" {
		return Result{404, map[string]any{"error": "unknown_program"}}
	}

	secret := ""
	if program.secretEnc != "" {
		secret, _ = s.Codec.Decrypt(program.secretEnc)
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	// Fail closed: a program with no usable secret has no way in.
	if secret == "" || subtle.ConstantTimeCompare([]byte(token), []byte(secret)) != 1 {
		return Result{401, map[string]any{"error": "unauthorized"}}
	}

	shipId, externalId = strings.TrimSpace(shipId), strings.TrimSpace(externalId)
	if shipId == "" && externalId == "" {
		return Result{422, map[string]any{"error": "invalid_payload", "field": "external_id"}}
	}

	ship, err := s.findShipForStatus(ctx, program.id, shipId, externalId)
	if err != nil {
		slog.Error("ship status lookup failed", "programId", program.id, "err", err)
		return Result{500, map[string]any{"error": "internal_error"}}
	}
	if ship == nil {
		return Result{404, map[string]any{"error": "not_found"}}
	}

	var decision any
	if ship.status == "approved" || ship.status == "changes" || ship.status == "rejected" {
		decision = ship.status
	}
	body := map[string]any{
		"id":          ship.id,
		"external_id": ship.externalId,
		"version":     ship.version,
		"phase":       shipPhase(ship.status, ship.claimed),
		"decision":    decision,
	}
	if decision != nil && ship.review != nil {
		body["review"] = ship.review.payload()
	}
	return Result{200, body}
}

// shipPhase maps a submission's DB state to the sender-facing review phase.
// `changes` counts as reviewed (the outbound webhook already delivered that
// decision) unless a reviewer has re-claimed the ship.
func shipPhase(status string, claimed bool) string {
	switch status {
	case "fraudreview":
		return "fraud_review"
	case "pending", "changes":
		if claimed {
			return "under_review"
		}
		if status == "changes" {
			return "reviewed"
		}
		return "review"
	case "secondpass":
		return "second_pass"
	case "approved", "rejected":
		return "reviewed"
	default: // processing, withdrawn, reverted
		return status
	}
}

func isTestPing(rawBody []byte) bool {
	var p map[string]any
	if err := json.Unmarshal(rawBody, &p); err != nil {
		return false // not JSON: let validate() report it
	}
	maker, _ := p["maker"].(map[string]any)
	return p["external_id"] == "test-ping" && maker != nil && maker["email"] == "test@ping.local"
}

func intersectEvidence(requested, accepts []string) []string {
	acceptSet := map[string]bool{}
	for _, a := range accepts {
		acceptSet[a] = true
	}
	out := []string{}
	for _, e := range requested {
		if (e == "commits" || e == "elapsed" || e == "devlog") && acceptSet[e] {
			out = append(out, e)
		}
	}
	return out
}

func queryStringOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
