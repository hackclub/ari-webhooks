package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hackclub/ari-webhooks/internal/db"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/secs"
)

type programRow struct {
	id            string
	status        string
	collaborative bool
	accepts       []string
	secretEnc     string
}

func (s *Service) loadProgram(ctx context.Context, programId string) (*programRow, error) {
	var p programRow
	err := s.Pool.QueryRow(ctx, `
		select p.id, p.status::text, p.collaborative, p.accepts::text[],
		       coalesce((select ws."secretEnc" from "WebhookSecret" ws
		                 where ws."programId" = p.id and ws."revokedAt" is null
		                 order by ws."createdAt" desc limit 1), '')
		from "Program" p where p.id = $1`,
		programId).Scan(&p.id, &p.status, &p.collaborative, &p.accepts, &p.secretEnc)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// recordDelivery is best-effort, matching ari: a full audit table must never fail a webhook.
func (s *Service) recordDelivery(ctx context.Context, programId, status string, httpStatus, payloadBytes int, sha, externalId, submissionId, errorDetail string) {
	_, err := s.Pool.Exec(ctx, `
		insert into "WebhookDelivery" (id, "programId", status, "httpStatus", "payloadBytes", "rawBodySha256", "externalId", "submissionId", "errorDetail")
		values ($1, $2, $3::"WebhookDeliveryStatus", $4, $5, $6, nullif($7, ''), nullif($8, ''), nullif($9, ''))`,
		ids.Cuid(), programId, status, httpStatus, payloadBytes, sha, externalId, submissionId, errorDetail)
	if err != nil {
		slog.Warn("delivery record failed", "programId", programId, "err", err)
	}
}

func (s *Service) recentBadSignatures(ctx context.Context, programId string) int {
	var count int
	err := s.Pool.QueryRow(ctx, `
		select count(*) from "WebhookDelivery"
		where "programId" = $1 and status = 'BAD_SIGNATURE'
		  and "receivedAt" >= now() - interval '60 seconds'`, // the window the bad-signature audit cap applies to
		programId).Scan(&count)
	if err != nil {
		slog.Warn("bad signature count failed", "programId", programId, "err", err)
	}
	return count
}

func (s *Service) findRecentAccepted(ctx context.Context, programId, sha string) (string, bool) {
	var id string
	// The join gates the retry dedup on the submission still being OPEN. Without it a
	// byte-identical legitimate resubmit within the hour (student clicks Resubmit after
	// an auto-reject without changing anything) is answered "duplicate" with the id of
	// the already-DECIDED submission, the sender rebinds to it believing it is queued,
	// and the ship strands outside every review queue.
	// Parked ships (second pass, fraud review) count as open: they are the same
	// undelivered ship, so a retry rebinding to them is correct.
	err := s.Pool.QueryRow(ctx, `
		select d."submissionId" from "WebhookDelivery" d
		join "Submission" sub on sub.id = d."submissionId"
		where d."programId" = $1 and d."rawBodySha256" = $2 and d.status = 'ACCEPTED'
		  and d."receivedAt" >= now() - interval '1 hour'
		  and sub.status in ('processing', 'pending', 'secondpass', 'fraudreview')
		order by d."receivedAt" desc limit 1`,
		programId, sha).Scan(&id)
	if err != nil {
		return "", false
	}
	return id, true
}

func (s *Service) findOpenShip(ctx context.Context, programId, externalId string) (string, bool) {
	var id string
	err := s.Pool.QueryRow(ctx, `
		select id from "Submission"
		where "programId" = $1 and "externalId" = $2
		  and status in ('processing', 'pending', 'secondpass', 'fraudreview')
		limit 1`,
		programId, externalId).Scan(&id)
	if err != nil {
		return "", false
	}
	return id, true
}

func (s *Service) findOpenShipWithMaker(ctx context.Context, programId, externalId string) (id, title, makerEmail string, found bool) {
	err := s.Pool.QueryRow(ctx, `
		select s.id, s.title, m.email from "Submission" s
		join "Maker" m on m.id = s."makerId"
		where s."programId" = $1 and s."externalId" = $2 and s.status in ('processing', 'pending', 'fraudreview')
		limit 1`,
		programId, externalId).Scan(&id, &title, &makerEmail)
	if err != nil {
		return "", "", "", false
	}
	return id, title, makerEmail, true
}

type shipStatusRow struct {
	id, externalId, status string
	version                int
	claimed                bool
	review                 *statusReviewRow
}

type statusReviewRow struct {
	noteToMaker     string
	auditNote       *string
	reviewerEmail   *string
	reviewerSlackId *string
}

func (r statusReviewRow) payload() map[string]any {
	out := map[string]any{"note_to_maker": r.noteToMaker, "reviewer": nil}
	if r.reviewerEmail != nil {
		out["reviewer"] = map[string]any{"email": *r.reviewerEmail, "slack_id": r.reviewerSlackId}
	}
	if r.auditNote != nil {
		out["audit_note"] = *r.auditNote
	}
	return out
}

// findShipForStatus resolves the status query's target: a specific ship by id,
// or a project's latest ship by externalId (highest version; ingestedAt breaks
// the tie for legacy rows sharing one). nil means no match, not an error.
func (s *Service) findShipForStatus(ctx context.Context, programId, shipId, externalId string) (*shipStatusRow, error) {
	where, arg := `s.id = $2`, shipId
	if shipId == "" {
		where, arg = `s."externalId" = $2 order by s.version desc, s."ingestedAt" desc`, externalId
	}
	var row shipStatusRow
	var noteToMaker, auditNote, reviewerEmail, reviewerSlackId *string
	err := s.Pool.QueryRow(ctx, `
		select s.id, s."externalId", s.status::text, s.version, s."claimedById" is not null,
		       latest."noteToMaker", latest."auditNote", u.email, u."slackId"
		from "Submission" s
		left join lateral (
			select "noteToMaker", "auditNote", "reviewerId"
			from "Review" where "submissionId" = s.id
			order by "createdAt" desc limit 1
		) latest on true
		left join "User" u on u.id = latest."reviewerId"
		where s."programId" = $1 and `+where+` limit 1`,
		programId, arg).Scan(&row.id, &row.externalId, &row.status, &row.version, &row.claimed,
		&noteToMaker, &auditNote, &reviewerEmail, &reviewerSlackId)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if noteToMaker != nil {
		row.review = &statusReviewRow{
			noteToMaker:     *noteToMaker,
			auditNote:       auditNote,
			reviewerEmail:   reviewerEmail,
			reviewerSlackId: reviewerSlackId,
		}
	}
	return &row, nil
}

func isOpenProjectConflict(err error) bool {
	return db.IsUniqueViolation(err, "Submission_open_project_key")
}

func (s *Service) createShip(ctx context.Context, programId string, ship ShipPayload, shippedAt *time.Time, acceptedEvidence []string) (string, error) {
	var submissionId string
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var prior int
		if err := tx.QueryRow(ctx,
			`select count(*) from "Submission" where "programId" = $1 and "externalId" = $2`,
			programId, ship.ExternalId).Scan(&prior); err != nil {
			return err
		}

		var inheritedQueuedAt *time.Time
		if prior > 0 {
			var prevStatus string
			var prevQueuedAt time.Time
			if err := tx.QueryRow(ctx, `
				select status::text, "queuedAt" from "Submission"
				where "programId" = $1 and "externalId" = $2
				order by version desc, "ingestedAt" desc limit 1`,
				programId, ship.ExternalId).Scan(&prevStatus, &prevQueuedAt); err != nil {
				return err
			}
			if prevStatus == "changes" { // a re-ship fixing requested changes keeps the project's place in the review queue; any other prior outcome resets it
				inheritedQueuedAt = &prevQueuedAt
			}
		}

		upsertPerson := func(p ShipPerson) (string, error) {
			var makerId string
			// Create with nulls for absent fields; update only overwrites with present
			// values (COALESCE keeps the old value when EXCLUDED is null), matching ari.
			err := tx.QueryRow(ctx, `
				insert into "Maker" (id, email, name, "slackId", "hackatimeUserId")
				values ($1, $2, nullif($3, ''), nullif($4, ''), nullif($5, ''))
				on conflict (email) do update set
					name = coalesce(excluded.name, "Maker".name),
					"slackId" = coalesce(excluded."slackId", "Maker"."slackId"),
					"hackatimeUserId" = coalesce(excluded."hackatimeUserId", "Maker"."hackatimeUserId")
				returning id`,
				ids.Cuid(), p.Email, p.Name, p.SlackId, p.HackatimeUserId).Scan(&makerId)
			return makerId, err
		}

		makerId, err := upsertPerson(ship.Maker)
		if err != nil {
			return err
		}
		collabIdByEmail := map[string]string{}
		for _, c := range ship.Collaborators {
			id, err := upsertPerson(c)
			if err != nil {
				return err
			}
			collabIdByEmail[c.Email] = id
		}

		submissionId = ids.ShipId()
		for {
			var exists bool
			if err := tx.QueryRow(ctx, `select exists(select 1 from "Submission" where id = $1)`, submissionId).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				break
			}
			submissionId = ids.ShipId()
		}

		journalSeconds := 0
		for _, j := range ship.Journals {
			journalSeconds += j.Seconds
		}
		programSeconds := ship.Maker.ProgramSeconds
		if ship.Collaborators != nil {
			programSeconds = 0
			for _, c := range ship.Collaborators {
				programSeconds += c.ProgramSeconds
			}
		}

		// receivedAt / queuedAt: only written when overridden so an absent override
		// keeps the exact DB-default semantics Prisma relies on (both default to
		// the same transaction-time now(), so they stay equal for a fresh ship).
		extraCols, extraVals := "", ""
		args := []any{submissionId, programId, ship.ExternalId, prior + 1, makerId, ship.Title, ship.Description,
			ship.RepoUrl, ship.DemoUrl, ship.ThumbnailUrl, jsonOrNil(ship.Meta), ship.IsUpdate, ship.UpdateMessage,
			ship.Track, acceptedEvidence, textArray(ship.HackatimeProjects)}
		addCol := func(col string, v any) {
			args = append(args, v)
			extraCols += `, "` + col + `"`
			extraVals += fmt.Sprintf(", $%d", len(args))
		}
		if shippedAt != nil {
			addCol("receivedAt", shippedAt.UTC())
		}
		switch {
		case inheritedQueuedAt != nil:
			addCol("queuedAt", inheritedQueuedAt.UTC())
		case shippedAt != nil:
			addCol("queuedAt", shippedAt.UTC())
		}
		_, err = tx.Exec(ctx, `
			insert into "Submission"
				(id, "programId", "externalId", version, "makerId", title, description,
				 "repoUrl", "demoUrl", "thumbnailUrl", "programMeta", "isUpdate", "updateMessage",
				 track, "acceptedEvidence", "hackatimeProjects", "claimedHours", status`+extraCols+`)
			values ($1, $2, $3, $4, $5, $6, $7,
			        $8, $9, $10, $11, $12, $13,
			        $14::"Track", $15::"Evidence"[], $16, 0, 'processing'`+extraVals+`)`,
			args...)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into ariw."submissionEvidenceVersion" ("submissionId", version)
			values ($1, $2)`, submissionId, CurrentEvidenceVersion); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx, `
			insert into "HoursBreakdown" ("submissionId", "hackatimeMinutes", "devlogSeconds", "programSeconds",
			                              "devlogMinutes", "programMinutes")
			values ($1, 0, $2, $3, $4, $5)`,
			submissionId, journalSeconds, programSeconds,
			secs.LegacyMinutes(journalSeconds), secs.LegacyMinutes(programSeconds)); err != nil {
			return err
		}

		for _, j := range ship.Journals {
			var journalMakerId any
			if j.Email != "" {
				if id, found := collabIdByEmail[j.Email]; found {
					journalMakerId = id
				}
			}
			if _, err := tx.Exec(ctx, `
				insert into "Devlog" (id, "submissionId", at, seconds, minutes, text, markdown, "makerId")
				values ($1, $2, $3, $4, $5, $6, $7, $8)`,
				ids.Cuid(), submissionId, j.At.UTC(), j.Seconds, secs.LegacyMinutes(j.Seconds),
				j.Text, j.Markdown, journalMakerId); err != nil {
				return err
			}
		}

		for _, c := range ship.Collaborators {
			devlogSeconds := 0
			for _, j := range ship.Journals {
				if j.Email == c.Email {
					devlogSeconds += j.Seconds
				}
			}
			if _, err := tx.Exec(ctx, `
				insert into "SubmissionCollaborator" (id, "submissionId", "makerId", "devlogSeconds", "programSeconds",
				                                      "devlogMinutes", "programMinutes")
				values ($1, $2, $3, $4, $5, $6, $7)`,
				ids.Cuid(), submissionId, collabIdByEmail[c.Email], devlogSeconds, c.ProgramSeconds,
				secs.LegacyMinutes(devlogSeconds), secs.LegacyMinutes(c.ProgramSeconds)); err != nil {
				return err
			}
		}

		activityMeta := map[string]any{
			"title":      ship.Title,
			"externalId": ship.ExternalId,
			"makerEmail": ship.Maker.Email,
			"journals":   len(ship.Journals),
			"track":      ship.Track,
			"evidence":   acceptedEvidence,
		}
		if ship.Collaborators != nil {
			emails := make([]string, len(ship.Collaborators))
			for i, c := range ship.Collaborators {
				emails[i] = c.Email
			}
			activityMeta["collaborators"] = emails
		}
		if programSeconds > 0 {
			activityMeta["programSeconds"] = programSeconds
			activityMeta["programMinutes"] = secs.LegacyMinutes(programSeconds)
		}
		if ship.IsUpdate {
			activityMeta["update"] = true
		}
		if _, err = tx.Exec(ctx, `
			insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
			values ($1, $2, 'WEBHOOK'::"ActivityKind", $3, $4, $5)`,
			ids.Cuid(), programId, submissionId, "New ship received - "+ship.Title, activityMeta); err != nil {
			return err
		}

		// Enqueue enrich in this same transaction: the ship and the job that drives
		// it out of 'processing' become durable together or not at all.
		if s.EnqueueEnrich != nil {
			return s.EnqueueEnrich(ctx, tx, submissionId)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return submissionId, nil
}

func (s *Service) withdrawShip(ctx context.Context, programId, submissionId, title, makerEmail, externalId string) (withdrawn, wasFraudReview bool, err error) {
	err = db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var prior string
		if err := tx.QueryRow(ctx,
			`select status::text from "Submission" where id = $1 for update`, // lock the row so a concurrent decision either wins or waits
			submissionId).Scan(&prior); err != nil {
			return err
		}
		if prior != "processing" && prior != "pending" && prior != "fraudreview" {
			return nil // a concurrent decision claimed it first and wins
		}
		if _, err := tx.Exec(ctx,
			`update "Submission" set status = 'withdrawn' where id = $1`, submissionId); err != nil {
			return err
		}
		withdrawn = true
		wasFraudReview = prior == "fraudreview"
		_, err := tx.Exec(ctx, `
			insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
			values ($1, $2, 'WEBHOOK'::"ActivityKind", $3, $4, $5)`,
			ids.Cuid(), programId, submissionId, "Withdrawn "+title, map[string]any{
				"op":         "withdrawn",
				"title":      title,
				"externalId": externalId,
				"makerEmail": makerEmail,
			})
		return err
	})
	return withdrawn, wasFraudReview, err
}

func jsonOrNil(m map[string]any) any {
	if m == nil {
		return nil
	}
	return m
}

func textArray(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
