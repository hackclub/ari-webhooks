package reingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/db"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/ingest"
	"github.com/hackclub/ari-webhooks/internal/jobs"
)

type Queue interface {
	Enqueue(ctx context.Context, kind, submissionId string, runAt time.Time) error
	Wake()
}

type DecisionInvalidation struct {
	ActivityEventId string
	ProgramId       string
	SubmissionId    string
	Event           string
	PriorStatus     string
	TargetStatus    string
}

type Service struct {
	Pool                  *pgxpool.Pool
	Queue                 Queue
	Capture               func(ctx context.Context, submissionId string) (complete bool, err error)
	OnFraudReset          func(ctx context.Context, submissionId, activityEventId string)
	OnDecisionInvalidated func(ctx context.Context, invalidation DecisionInvalidation) error
	AfterReingest         func(ctx context.Context, submissionId string)
	TargetEvidenceVersion int
}

var retryDelays = []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

func (s *Service) targetEvidenceVersion() int {
	if s.TargetEvidenceVersion > 0 {
		return s.TargetEvidenceVersion
	}
	return ingest.CurrentEvidenceVersion
}

// Sweep turns stale submissions into durable per-submission work. Revisions of
// one project are strictly oldest-first: a later ship's evidence window depends
// on which earlier ships are still approved after reprocessing.
func (s *Service) Sweep(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, `
		select s.id from "Submission" s
		join "Program" p on p.id = s."programId"
		left join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where coalesce(ev.version, 1) < $1 and s.status != 'processing'
		  and p.status != 'ARCHIVED'
		  and not exists (
		    select 1 from "Submission" earlier
		    left join ariw."submissionEvidenceVersion" earlier_ev
		      on earlier_ev."submissionId" = earlier.id
		    where earlier."programId" = s."programId"
		      and earlier."externalId" = s."externalId"
		      and coalesce(earlier_ev.version, 1) < $1
		      and (earlier."receivedAt", earlier.version, earlier."ingestedAt", earlier.id)
		          < (s."receivedAt", s.version, s."ingestedAt", s.id)
		  )
		order by coalesce(ev.version, 1), s."receivedAt", s.version, s."ingestedAt", s.id
		limit 500`, s.targetEvidenceVersion())
	if err != nil {
		return err
	}
	var submissionIds []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		submissionIds = append(submissionIds, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	for _, id := range submissionIds {
		if err := s.Queue.Enqueue(ctx, "reingest", id, time.Now()); err != nil {
			return err
		}
	}
	if len(submissionIds) > 0 {
		s.Queue.Wake()
	}
	return s.retryDecisionNotifications(ctx)
}

func (s *Service) Handle(ctx context.Context, job jobs.Job) jobs.Outcome {
	var currentEvidenceVersion int
	err := s.Pool.QueryRow(ctx, `
		select coalesce(ev.version, 1)
		from "Submission" s
		left join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
		where s.id = $1`, job.SubmissionId).Scan(&currentEvidenceVersion)
	if errors.Is(err, pgx.ErrNoRows) || currentEvidenceVersion >= s.targetEvidenceVersion() {
		return jobs.Done()
	}
	if err != nil {
		return s.retry(job, err)
	}

	before, err := hoursFingerprint(ctx, s.Pool, job.SubmissionId, currentEvidenceVersion)
	if err != nil {
		return s.retry(job, err)
	}
	complete, err := s.Capture(ctx, job.SubmissionId)
	if err != nil {
		return s.retry(job, err)
	}
	if !complete {
		return s.retry(job, errors.New("evidence capture was incomplete"))
	}

	var result finishResult
	// A new revision can enter the queue while this long-running capture is in
	// flight. If the partial unique index wins that race, retry only the atomic
	// finish with the original before-fingerprint; the second pass will see the
	// new open revision and terminally invalidate this older decision instead.
	for attempt := 0; attempt < 2; attempt++ {
		result, err = s.finish(ctx, job.SubmissionId, currentEvidenceVersion, before)
		if !db.IsUniqueViolation(err, "Submission_open_project_key") {
			break
		}
	}
	if err != nil {
		return s.retry(job, err)
	}
	if !result.updated {
		return jobs.Done()
	}
	if result.needsFraudReset && s.OnFraudReset != nil {
		s.OnFraudReset(ctx, job.SubmissionId, result.activityEventId)
	}
	if result.invalidation != nil {
		_ = s.deliverDecisionInvalidation(ctx, *result.invalidation)
	}
	if s.AfterReingest != nil {
		s.AfterReingest(ctx, job.SubmissionId)
	}
	return jobs.Done()
}

func (s *Service) retry(job jobs.Job, err error) jobs.Outcome {
	if job.Attempt >= len(retryDelays) {
		return jobs.Dead(err.Error())
	}
	return jobs.RetryAt(time.Now().Add(retryDelays[job.Attempt]), job.Attempt+1, job.TimeoutRetries, err.Error())
}

type finishResult struct {
	updated         bool
	needsFraudReset bool
	activityEventId string
	programId       string
	invalidation    *DecisionInvalidation
}

func (s *Service) finish(ctx context.Context, submissionId string, expectedEvidenceVersion int, before string) (finishResult, error) {
	var result finishResult
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var title, status, externalId string
		var currentEvidenceVersion int
		if err := tx.QueryRow(ctx, `
			select s.title, s.status::text, s."programId", s."externalId",
			       coalesce(ev.version, 1)
			from "Submission" s
			left join ariw."submissionEvidenceVersion" ev on ev."submissionId" = s.id
			where s.id = $1 for update of s`, submissionId).
			Scan(&title, &status, &result.programId, &externalId, &currentEvidenceVersion); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}
		if currentEvidenceVersion >= s.targetEvidenceVersion() || currentEvidenceVersion != expectedEvidenceVersion {
			return nil
		}

		after, err := hoursFingerprint(ctx, tx, submissionId, expectedEvidenceVersion)
		if err != nil {
			return err
		}
		hoursChanged := before != after
		targetStatus := status
		if hoursChanged {
			switch status {
			case "approved", "changes", "rejected", "secondpass", "fraudreview":
				var anotherRevisionOpen bool
				if err := tx.QueryRow(ctx, `
					select exists (
					  select 1 from "Submission"
					  where id != $1 and "programId" = $2 and "externalId" = $3
					    and status in ('processing', 'pending')
					)`, submissionId, result.programId, externalId).Scan(&anotherRevisionOpen); err != nil {
					return err
				}
				if anotherRevisionOpen {
					targetStatus = "reverted"
				} else {
					targetStatus = "pending"
				}
			}
		}
		invalidated := targetStatus != status
		deliveredDecisionInvalidated := invalidated &&
			(status == "approved" || status == "changes" || status == "rejected")

		if _, err := tx.Exec(ctx, `
			update "Submission"
			set status = $2::"SubmissionStatus",
			    "claimedById" = null,
			    "claimedAt" = null,
			    "aiCheckResult" = null,
			    "aiCheckRanAt" = null
			where id = $1`, submissionId, targetStatus); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into ariw."submissionEvidenceVersion" ("submissionId", version)
			values ($1, $2)
			on conflict ("submissionId") do update set version = excluded.version`,
			submissionId, s.targetEvidenceVersion()); err != nil {
			return err
		}

		if _, err := tx.Exec(ctx,
			`delete from "Draft" where "submissionId" = $1`, submissionId); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			update "SubmissionOpen" set "closedAt" = now(), "closeReason" = 'left'
			where "submissionId" = $1 and "closedAt" is null`, submissionId); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			with removed as (
				delete from "ReviewerVm" where "submissionId" = $1 returning vmid
			)
			insert into "VmTombstone" (vmid)
			select vmid from removed
			on conflict (vmid) do update
			set attempts = "VmTombstone".attempts + 1, "lastTriedAt" = now()`, submissionId); err != nil {
			return err
		}

		var hasFraudChecks bool
		if err := tx.QueryRow(ctx,
			`select exists(select 1 from "FraudCheck" where "submissionId" = $1)`, submissionId).
			Scan(&hasFraudChecks); err != nil {
			return err
		}
		result.needsFraudReset = hoursChanged && hasFraudChecks
		fraudResetState := "complete"
		if result.needsFraudReset {
			fraudResetState = "pending"
		}
		result.activityEventId = ids.Cuid()
		kind := "EVIDENCE"
		text := fmt.Sprintf("Reprocessed %s with evidence algorithm version %d", title, s.targetEvidenceVersion())
		if invalidated {
			kind = "REVERT"
			if targetStatus == "pending" {
				text = fmt.Sprintf("Returned %s to review after reprocessing changed its hours", title)
			} else {
				text = fmt.Sprintf("Invalidated the decision for %s after reprocessing changed its hours", title)
			}
		}
		outboundEvent := ""
		if deliveredDecisionInvalidated {
			outboundEvent = "review.requeued"
			if targetStatus == "reverted" {
				outboundEvent = "review.reverted"
			}
		}
		if _, err := tx.Exec(ctx, `
			insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
			values ($1, $2, $3::"ActivityKind", $4, $5, $6)`,
			result.activityEventId, result.programId, kind, submissionId, text, map[string]any{
				"op":                  "evidence_algorithm_updated",
				"fromEvidenceVersion": expectedEvidenceVersion,
				"toEvidenceVersion":   s.targetEvidenceVersion(),
				"hoursChanged":        hoursChanged,
				"fromStatus":          status,
				"toStatus":            targetStatus,
				"fraudResetComplete":  !result.needsFraudReset,
				"fraudResetState":     fraudResetState,
				"outboundEvent":       outboundEvent,
				"outboundComplete":    !deliveredDecisionInvalidated,
			}); err != nil {
			return err
		}
		if deliveredDecisionInvalidated {
			result.invalidation = &DecisionInvalidation{
				ActivityEventId: result.activityEventId,
				ProgramId:       result.programId,
				SubmissionId:    submissionId,
				Event:           outboundEvent,
				PriorStatus:     status,
				TargetStatus:    targetStatus,
			}
		}
		result.updated = true
		return nil
	})
	return result, err
}

func (s *Service) retryDecisionNotifications(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, `
		select ae.id, ae."programId", ae."submissionId", ae.meta->>'outboundEvent',
		       ae.meta->>'fromStatus', ae.meta->>'toStatus'
		from "ActivityEvent" ae join "Program" p on p.id = ae."programId"
		where ae.meta->>'op' = 'evidence_algorithm_updated'
		  and coalesce((ae.meta->>'outboundComplete')::boolean, true) = false
		  and p.status != 'ARCHIVED'
		order by ae."createdAt" limit 100`) // an archived program is paused: the notice stays owed and goes out once it is restored
	if err != nil {
		return err
	}
	var pending []DecisionInvalidation
	for rows.Next() {
		var in DecisionInvalidation
		if err := rows.Scan(&in.ActivityEventId, &in.ProgramId, &in.SubmissionId,
			&in.Event, &in.PriorStatus, &in.TargetStatus); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, in)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	var firstErr error
	for _, in := range pending {
		if err := s.deliverDecisionInvalidation(ctx, in); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *Service) deliverDecisionInvalidation(ctx context.Context, in DecisionInvalidation) error {
	if s.OnDecisionInvalidated != nil {
		if err := s.OnDecisionInvalidated(ctx, in); err != nil {
			return err
		}
	}
	_, err := s.Pool.Exec(ctx, `
		update "ActivityEvent"
		set meta = jsonb_set(meta, '{outboundComplete}', 'true'::jsonb, true)
		where id = $1`, in.ActivityEventId)
	return err
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type hoursState struct {
	Totals        [8]int             `json:"totals"`
	Commits       []evidenceMinutes  `json:"commits"`
	Devlogs       []evidenceMinutes  `json:"devlogs"`
	Clips         []evidenceMinutes  `json:"clips"`
	Collaborators []collaboratorTime `json:"collaborators"`
}

type evidenceMinutes struct {
	Id      string `json:"id"`
	Minutes int    `json:"minutes"`
	MakerId string `json:"makerId"`
}

type collaboratorTime struct {
	MakerId   string `json:"makerId"`
	Hackatime int    `json:"hackatime"`
	Devlog    int    `json:"devlog"`
	After     int    `json:"after"`
	Lapse     int    `json:"lapse"`
	Program   int    `json:"program"`
}

// evidence captured at version 1 only knew whole minutes, so a ship leaving it is
// compared in minutes: its backfilled seconds would differ from any real capture
func hoursFingerprint(ctx context.Context, q queryer, submissionId string, fromEvidenceVersion int) (string, error) {
	wholeMinutes := fromEvidenceVersion < 2
	unit, devlogUnit := "Seconds", "seconds"
	if wholeMinutes {
		unit, devlogUnit = "Minutes", "minutes"
	}
	var state hoursState
	err := q.QueryRow(ctx, `
		select "hackatime`+unit+`", "devlog`+unit+`", "afterLastCommit`+unit+`", "lapse`+unit+`",
		       "aiDiscounted`+unit+`", "program`+unit+`",
		       coalesce((select sum("codingSeconds") from "Commit" where "submissionId" = $1), 0),
		       coalesce((select sum("lengthSeconds") from "ElapsedClip" where "submissionId" = $1), 0)
		from "HoursBreakdown" where "submissionId" = $1`, submissionId).
		Scan(&state.Totals[0], &state.Totals[1], &state.Totals[2], &state.Totals[3],
			&state.Totals[4], &state.Totals[5], &state.Totals[6], &state.Totals[7])
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	loadEvidence := func(sql string, scaleSeconds bool) ([]evidenceMinutes, error) {
		rows, err := q.Query(ctx, sql, submissionId)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []evidenceMinutes
		for rows.Next() {
			var row evidenceMinutes
			if err := rows.Scan(&row.Id, &row.Minutes, &row.MakerId); err != nil {
				return nil, err
			}
			if scaleSeconds && wholeMinutes {
				row.Minutes = int(float64(row.Minutes)/60 + 0.5)
			}
			out = append(out, row)
		}
		return out, rows.Err()
	}
	state.Commits, err = loadEvidence(`
		select id, "codingSeconds", coalesce("makerId", '') from "Commit"
		where "submissionId" = $1 order by id`, true)
	if err != nil {
		return "", err
	}
	state.Devlogs, err = loadEvidence(`
		select id, `+devlogUnit+`, coalesce("makerId", '') from "Devlog"
		where "submissionId" = $1 order by id`, false)
	if err != nil {
		return "", err
	}
	state.Clips, err = loadEvidence(`
		select id, "lengthSeconds", coalesce("makerId", '') from "ElapsedClip"
		where "submissionId" = $1 order by id`, true)
	if err != nil {
		return "", err
	}

	rows, err := q.Query(ctx, `
		select "makerId", "hackatime`+unit+`", "devlog`+unit+`", "afterLastCommit`+unit+`",
		       "lapse`+unit+`", "program`+unit+`"
		from "SubmissionCollaborator" where "submissionId" = $1 order by "makerId"`, submissionId)
	if err != nil {
		return "", err
	}
	for rows.Next() {
		var row collaboratorTime
		if err := rows.Scan(&row.MakerId, &row.Hackatime, &row.Devlog, &row.After, &row.Lapse, &row.Program); err != nil {
			rows.Close()
			return "", err
		}
		state.Collaborators = append(state.Collaborators, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", err
	}

	raw, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:]), nil
}
