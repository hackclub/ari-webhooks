package outbound

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/hackclub/ari-webhooks/internal/db"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/secs"
	"github.com/hackclub/ari-webhooks/internal/ssrf"
)

type CollaboratorMinutes struct {
	Total     int
	Hackatime int
	Journals  int
	Lapse     int
	Program   int
}

type SourceSeconds struct {
	Hackatime int
	Journals  int
	Lapse     int
	Program   int
}

type CollaboratorSeconds struct {
	Total   int
	Sources SourceSeconds
}

type FraudCheckResult struct {
	Email         string  `json:"email"`
	SlackId       *string `json:"slack_id"`
	TrustScore    *int    `json:"trust_score"`
	Justification *string `json:"justification"`
	// absent on a review recorded before the fraud team gave deflated hours
	DeflateSeconds *int `json:"deflate_seconds,omitempty"`
}

type FraudBlock struct {
	Verdict string             `json:"verdict"`
	Checks  []FraudCheckResult `json:"checks"`
}

type DispatchInput struct {
	DeliveryId          string
	Event               string
	Decision            *string
	ProgramId           string
	SubmissionId        string
	ReviewerId          string
	Note                string
	AuditNote           *string
	ApprovedMinutes     *int
	MinutesBreakdown    map[string]int
	CollaboratorMinutes map[string]CollaboratorMinutes
	// set for a settlementVersion 3 review: the minute fields above are then ignored
	// and the legacy wire fields derive from these settled seconds
	ApprovedSeconds     *int
	SecondsBreakdown    *SourceSeconds
	CollaboratorSeconds map[string]CollaboratorSeconds
	Fields              map[string]any
	Fraud               *FraudBlock
}

type reviewerRef struct {
	Email   string  `json:"email"`
	SlackId *string `json:"slack_id"`
}

type makerRef struct {
	Email   string  `json:"email"`
	Name    string  `json:"name"`
	SlackId *string `json:"slack_id"`
}

type shipAuthorRef struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

type shipBlock struct {
	Title             string          `json:"title"`
	Description       *string         `json:"description"`
	Track             string          `json:"track"`
	ThumbnailUrl      *string         `json:"thumbnail_url"`
	Authors           []shipAuthorRef `json:"authors"`
	RepoUrl           string          `json:"repo_url"`
	DemoUrl           *string         `json:"demo_url"`
	HackatimeProjects []string        `json:"hackatime_projects"`
}

type minutesBlock struct {
	Hackatime int `json:"hackatime"`
	Journals  int `json:"journals"`
	Lapse     int `json:"lapse"`
	Program   int `json:"program"`
}

type collaboratorRef struct {
	Email            string        `json:"email"`
	Name             string        `json:"name"`
	SlackId          *string       `json:"slack_id"`
	HackatimeId      *string       `json:"hackatime_id"`
	ApprovedMinutes  *int          `json:"approved_minutes,omitempty"`
	ApprovedHours    *float64      `json:"approved_hours,omitempty"`
	ApprovedSeconds  *int          `json:"approved_seconds,omitempty"`
	MinutesBreakdown *minutesBlock `json:"minutes_breakdown,omitempty"`
	SecondsBreakdown *minutesBlock `json:"seconds_breakdown,omitempty"`
}

type fieldAnswer struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

type reviewBlock struct {
	NoteToMaker      string              `json:"note_to_maker"`
	Reviewer         *reviewerRef        `json:"reviewer"`
	AuditNote        *string             `json:"audit_note,omitempty"`
	ApprovedMinutes  *int                `json:"approved_minutes,omitempty"`
	ApprovedHours    *float64            `json:"approved_hours,omitempty"`
	ApprovedSeconds  *int                `json:"approved_seconds,omitempty"`
	MinutesBreakdown map[string]int      `json:"minutes_breakdown,omitempty"`
	SecondsBreakdown map[string]int      `json:"seconds_breakdown,omitempty"`
	Fields           []fieldAnswer       `json:"fields,omitempty"`
	Justification    *justificationBlock `json:"justification,omitempty"`
}

type webhookPayload struct {
	Event         string            `json:"event"`
	Decision      *string           `json:"decision"`
	Id            string            `json:"id"`
	ExternalId    string            `json:"external_id"`
	Maker         makerRef          `json:"maker"`
	Collaborators []collaboratorRef `json:"collaborators,omitempty"`
	Ship          shipBlock         `json:"ship"`
	Fraud         *FraudBlock       `json:"fraud,omitempty"`
	Review        reviewBlock       `json:"review"`
}

func (b minutesBlock) asMap() map[string]int {
	return map[string]int{"hackatime": b.Hackatime, "journals": b.Journals, "lapse": b.Lapse, "program": b.Program}
}

func legacyBreakdown(totalSeconds int, sources SourceSeconds) minutesBlock {
	parts := secs.SplitProportional(secs.LegacyMinutes(totalSeconds),
		[]int{sources.Hackatime, sources.Journals, sources.Lapse, sources.Program})
	return minutesBlock{Hackatime: parts[0], Journals: parts[1], Lapse: parts[2], Program: parts[3]}
}

func decodeAuthorNameOverrides(raw []byte) map[string]string {
	var parsed any
	if json.Unmarshal(raw, &parsed) != nil {
		return nil
	}
	entries, ok := parsed.(map[string]any)
	if !ok {
		return nil
	}
	overrides := map[string]string{}
	for email, value := range entries {
		name, ok := value.(string)
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if name != "" {
			overrides[strings.ToLower(email)] = name
		}
	}
	return overrides
}

func shipAuthorName(overrides map[string]string, email string, storedName *string) string {
	if name, present := overrides[strings.ToLower(email)]; present {
		return name
	}
	if storedName != nil {
		return *storedName
	}
	return email
}

// DispatchReview swallows every error so an outbound failure can never break the
// caller's already-committed review decision.
func (w *Worker) DispatchReview(ctx context.Context, input DispatchInput) {
	if err := w.DispatchReviewResult(ctx, input); err != nil {
		slog.Warn("outbound dispatch skipped", "submissionId", input.SubmissionId, "event", input.Event, "err", err)
	}
}

// DispatchReviewResult is the error-returning form used by durable callers
// which keep their own retry marker until the outbox row has committed.
func (w *Worker) DispatchReviewResult(ctx context.Context, input DispatchInput) error {
	return w.dispatch(ctx, input)
}

func (w *Worker) dispatch(ctx context.Context, input DispatchInput) error {
	if input.DeliveryId != "" {
		var exists bool
		if err := w.Pool.QueryRow(ctx,
			`select exists(select 1 from "OutboundDelivery" where id = $1)`, input.DeliveryId).
			Scan(&exists); err != nil {
			return err
		}
		if exists {
			return nil
		}
	}
	var enabled bool
	var url, secretEnc *string
	err := w.Pool.QueryRow(ctx,
		`select enabled, url, "secretEnc" from "OutboundEndpoint" where "programId" = $1`,
		input.ProgramId).Scan(&enabled, &url, &secretEnc)
	if err == pgx.ErrNoRows {
		return nil // no endpoint configured: silently a no-op, like ari
	}
	if err != nil {
		return err
	}
	if !enabled || url == nil || *url == "" || secretEnc == nil || *secretEnc == "" {
		return nil
	}
	if !ssrf.IsSafeOutboundUrl(*url) { // never persist a delivery aimed inside the network
		return nil
	}
	if _, err := w.Codec.Decrypt(*secretEnc); err != nil {
		return nil // bad/rotated key must not orphan a PENDING row that never reconciles
	}

	var subExternalId, subMakerEmail string
	var subMakerName *string
	var subMakerSlackId *string
	var ship shipBlock
	var authorNameOverridesRaw []byte
	// Row-to-JSON keeps this query compatible while Ari's optional override column rolls out.
	err = w.Pool.QueryRow(ctx, `
		select s."externalId", s.title, s.description, s.track::text, s."thumbnailUrl",
		       s."repoUrl", s."demoUrl", s."hackatimeProjects",
		       coalesce(to_jsonb(s)->'authorNameOverrides', '{}'::jsonb),
		       m.email, m.name, m."slackId"
		from "Submission" s join "Maker" m on m.id = s."makerId"
		where s.id = $1`,
		input.SubmissionId).Scan(
		&subExternalId, &ship.Title, &ship.Description, &ship.Track, &ship.ThumbnailUrl,
		&ship.RepoUrl, &ship.DemoUrl, &ship.HackatimeProjects, &authorNameOverridesRaw,
		&subMakerEmail, &subMakerName, &subMakerSlackId)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if ship.HackatimeProjects == nil {
		ship.HackatimeProjects = []string{}
	}
	authorNameOverrides := decodeAuthorNameOverrides(authorNameOverridesRaw)
	maker := makerRef{
		Email:   subMakerEmail,
		Name:    shipAuthorName(authorNameOverrides, subMakerEmail, subMakerName),
		SlackId: subMakerSlackId,
	}

	var reviewer *reviewerRef
	var reviewerEmail string
	var reviewerSlackId *string
	err = w.Pool.QueryRow(ctx, `select email, "slackId" from "User" where id = $1`, input.ReviewerId).
		Scan(&reviewerEmail, &reviewerSlackId)
	if err == nil {
		reviewer = &reviewerRef{Email: reviewerEmail, SlackId: reviewerSlackId}
	} else if err != pgx.ErrNoRows {
		return err
	}

	review := reviewBlock{NoteToMaker: input.Note, Reviewer: reviewer, AuditNote: input.AuditNote}
	if input.ApprovedSeconds != nil {
		minutes := secs.LegacyMinutes(*input.ApprovedSeconds)
		hours := secs.LegacyHours(*input.ApprovedSeconds)
		review.ApprovedMinutes = &minutes
		review.ApprovedHours = &hours
		review.ApprovedSeconds = input.ApprovedSeconds
		if input.SecondsBreakdown != nil {
			review.MinutesBreakdown = legacyBreakdown(*input.ApprovedSeconds, *input.SecondsBreakdown).asMap()
			review.SecondsBreakdown = minutesBlock(*input.SecondsBreakdown).asMap()
		}
	} else {
		if input.ApprovedMinutes != nil {
			review.ApprovedMinutes = input.ApprovedMinutes
			hours := math.Floor(float64(*input.ApprovedMinutes)/60*10+0.5) / 10
			review.ApprovedHours = &hours
			seconds := *input.ApprovedMinutes * 60
			review.ApprovedSeconds = &seconds
		}
		review.MinutesBreakdown = input.MinutesBreakdown
		if len(input.MinutesBreakdown) > 0 {
			review.SecondsBreakdown = map[string]int{}
			for source, minutes := range input.MinutesBreakdown {
				review.SecondsBreakdown[source] = minutes * 60
			}
		}
	}
	if input.Fields != nil {
		defRows, err := w.Pool.Query(ctx,
			`select key, label, type::text from "ReviewField" where "programId" = $1`,
			input.ProgramId)
		if err != nil {
			return err
		}
		defer defRows.Close()
		fields := []fieldAnswer{}
		for defRows.Next() {
			var f fieldAnswer
			if err := defRows.Scan(&f.Key, &f.Label, &f.Type); err != nil {
				return err
			}
			if v, present := input.Fields[f.Key]; present {
				f.Value = v
			}
			fields = append(fields, f)
		}
		if err := defRows.Err(); err != nil {
			return err
		}
		review.Fields = fields
	}

	justification, err := w.loadJustification(ctx, input.SubmissionId)
	if err != nil {
		return err
	}
	review.Justification = justification

	collabRows, err := w.Pool.Query(ctx, `
		select c."makerId", m.email, m.name, m."slackId", m."hackatimeUserId"
		from "SubmissionCollaborator" c join "Maker" m on m.id = c."makerId"
		where c."submissionId" = $1
		order by c.id`,
		input.SubmissionId)
	if err != nil {
		return err
	}
	defer collabRows.Close()
	var collaborators []collaboratorRef
	for collabRows.Next() {
		var makerId string
		var storedName *string
		var ref collaboratorRef
		if err := collabRows.Scan(&makerId, &ref.Email, &storedName, &ref.SlackId, &ref.HackatimeId); err != nil {
			return err
		}
		ref.Name = shipAuthorName(authorNameOverrides, ref.Email, storedName)
		if settled, present := input.CollaboratorSeconds[makerId]; present && input.ApprovedSeconds != nil {
			total := settled.Total
			minutes := secs.LegacyMinutes(total)
			hours := secs.LegacyHours(total)
			breakdown := legacyBreakdown(total, settled.Sources)
			sources := minutesBlock(settled.Sources)
			ref.ApprovedMinutes = &minutes
			ref.ApprovedHours = &hours
			ref.ApprovedSeconds = &total
			ref.MinutesBreakdown = &breakdown
			ref.SecondsBreakdown = &sources
		} else if mins, present := input.CollaboratorMinutes[makerId]; present && input.ApprovedSeconds == nil {
			total := mins.Total
			hours := math.Floor(float64(total)/60*10+0.5) / 10
			seconds := total * 60
			ref.ApprovedMinutes = &total
			ref.ApprovedHours = &hours
			ref.ApprovedSeconds = &seconds
			ref.MinutesBreakdown = &minutesBlock{
				Hackatime: mins.Hackatime, Journals: mins.Journals, Lapse: mins.Lapse, Program: mins.Program,
			}
			ref.SecondsBreakdown = &minutesBlock{
				Hackatime: mins.Hackatime * 60, Journals: mins.Journals * 60, Lapse: mins.Lapse * 60, Program: mins.Program * 60,
			}
		}
		collaborators = append(collaborators, ref)
	}
	if err := collabRows.Err(); err != nil {
		return err
	}
	ship.Authors = []shipAuthorRef{{Email: maker.Email, Name: maker.Name}}
	if len(collaborators) > 0 {
		ship.Authors = make([]shipAuthorRef, len(collaborators))
		for i, collaborator := range collaborators {
			ship.Authors[i] = shipAuthorRef{Email: collaborator.Email, Name: collaborator.Name}
		}
	}

	rawBody, err := json.Marshal(webhookPayload{
		Event:         input.Event,
		Decision:      input.Decision,
		Id:            input.SubmissionId,
		ExternalId:    subExternalId,
		Maker:         maker,
		Collaborators: collaborators,
		Ship:          ship,
		Fraud:         input.Fraud,
		Review:        review,
	})
	if err != nil {
		return err
	}

	deliveryId := input.DeliveryId
	if deliveryId == "" {
		deliveryId = ids.Cuid()
	}
	err = db.InTx(ctx, w.Pool, func(tx pgx.Tx) error {
		inserted, err := tx.Exec(ctx, `
			insert into "OutboundDelivery" (id, "programId", event, "submissionId", url, status, payload)
			values ($1, $2, $3, $4, $5, 'PENDING', $6)
			on conflict (id) do nothing`,
			deliveryId, input.ProgramId, input.Event, input.SubmissionId, *url, string(rawBody))
		if err != nil {
			return err
		}
		if inserted.RowsAffected() == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, `
			insert into ariw."outboundSchedule" ("deliveryId", "nextAttemptAt") values ($1, now())`,
			deliveryId); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `select pg_notify($1, $2)`, NotifyChannel, deliveryId)
		return err
	})
	if err != nil {
		return err
	}
	w.Wake()
	return nil
}
