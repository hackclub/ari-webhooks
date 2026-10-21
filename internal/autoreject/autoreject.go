package autoreject

import (
	"context"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/db"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/outbound"
)

// SystemUserId is the well-known DB id for automated system decisions, seeded by an ari migration.
const SystemUserId = "system"

type Reason string

const (
	InaccessibleRepo           Reason = "inaccessible_repo"
	InaccessibleDemo           Reason = "inaccessible_demo"
	CollaboratorNoHours        Reason = "collaborator_no_hours"
	ProcessingTimeout          Reason = "processing_timeout"
	DisallowedHackatimeProject Reason = "disallowed_hackatime_project"
	HackatimeAccountNotFound   Reason = "hackatime_account_not_found"
)

type Wording struct {
	NoteToMaker func(in Input) string
	AuditNote   func(in Input) string
	Activity    func(in Input) string
}

var wordings = map[Reason]Wording{}

// wording for a reason this package does not know. call from an init function
// only: the table is read without a lock
func Describe(reason Reason, wording Wording) {
	wordings[reason] = wording
}

type Input struct {
	SubmissionId  string
	Reason        Reason
	Who           []string
	Detail        string
	Collaborative bool
	ProgramName   string
	// ClaimChanges also claims a ship already decided as 'changes', for a
	// verdict that must override a delivered changes request. Every other
	// automated decision only ever decides open ships.
	ClaimChanges bool
}

type Service struct {
	Pool     *pgxpool.Pool
	Outbound *outbound.Worker
	// OnFraudDecided runs after a decision claims a ship, so a ship claimed out
	// of fraud review can be cleaned up. Wired to the fraud gateway in main.
	OnFraudDecided func(ctx context.Context, submissionId string)
}

// The maker/audit/activity text tables are program-visible contract, not
// prose to restyle.
func noteToMaker(in Input) string {
	if wording, known := wordings[in.Reason]; known {
		return wording.NoteToMaker(in)
	}
	names := strings.Join(in.Who, ", ")
	switch in.Reason {
	case InaccessibleRepo:
		return "We couldn't access your repository, so this submission was auto-rejected. Make sure the repo is public and resubmit."
	case InaccessibleDemo:
		return "We couldn't reach your demo URL, so this submission was auto-rejected. Make sure the demo is live and publicly accessible, then resubmit."
	case ProcessingTimeout:
		return "We couldn't finish processing your submission in time, so it was auto-rejected. That's usually a hiccup on our side or a slow repository host - just resubmit."
	case DisallowedHackatimeProject:
		return "The <<LAST_PROJECT>> Hackatime project is not allowed to be used for shipping in YSWS programs, and it was the only Hackatime project on this submission. Track your time under a real project name on Hackatime, then ship again with that project attached."
	case HackatimeAccountNotFound:
		return "We couldn't find a Hackatime account for " + names + ". Everyone on a collaborative ship needs their own time on Hackatime, journals, or timelapses. If they tracked time on Hackatime, make sure their Hackatime account uses the email on the submission or is linked to their Slack account, then ship again."
	}
	return names + " hasn't logged any verified time on this project, so the submission was auto-rejected. Everyone on a collaborative ship needs their own time on Hackatime, journals, or timelapses. If they did work on it, make sure their Hackatime account uses the email on the submission, then resubmit."
}

func auditNote(in Input) string {
	names := strings.Join(in.Who, ", ")
	base := ""
	wording, known := wordings[in.Reason]
	switch {
	case known:
		base = wording.AuditNote(in)
	case in.Reason == InaccessibleRepo:
		base = "Auto-rejected: git repository was not cloneable after all retries."
	case in.Reason == InaccessibleDemo:
		base = "Auto-rejected: demo URL was unreachable after all retries."
	case in.Reason == ProcessingTimeout:
		base = "Auto-rejected: stuck in processing for over 6 hours (evidence capture never completed)."
	case in.Reason == DisallowedHackatimeProject:
		base = "Changes requested: <<LAST_PROJECT>> was the only Hackatime project attached; it is not allowed for shipping in YSWS programs."
	case in.Reason == HackatimeAccountNotFound:
		base = "Changes requested: no verified time and no Hackatime account found by email or Slack id for - " + names + "."
	default:
		base = "Auto-rejected: collaborator(s) with zero verified time - " + names + "."
	}
	if in.Detail != "" {
		return base + " Last error: " + sliceRunes(in.Detail, 300)
	}
	return base
}

func activityText(in Input) string {
	if wording, known := wordings[in.Reason]; known {
		return wording.Activity(in)
	}
	switch in.Reason {
	case InaccessibleRepo:
		return "Auto-rejected: repository not accessible"
	case InaccessibleDemo:
		return "Auto-rejected: demo not accessible"
	case ProcessingTimeout:
		return "Auto-rejected: processing timed out"
	case DisallowedHackatimeProject:
		return "Changes requested: only Hackatime project was <<LAST_PROJECT>>"
	case HackatimeAccountNotFound:
		return "Changes requested: couldn't find a Hackatime account for " + strings.Join(in.Who, ", ")
	}
	return "Auto-rejected: no hours logged by " + strings.Join(in.Who, ", ")
}

// AutoReject is idempotent: a no-op when the submission is already decided
// (the atomic claim loses).
func (s *Service) AutoReject(ctx context.Context, in Input) {
	s.settle(ctx, in, "rejected")
}

// RequestChanges is AutoReject's idempotent counterpart for the 'changes' decision.
func (s *Service) RequestChanges(ctx context.Context, in Input) {
	s.settle(ctx, in, "changes")
}

func (s *Service) settle(ctx context.Context, in Input, decision string) { // status, review decision, activity kind, and outbound event all derive from decision; the enums share values
	note := noteToMaker(in)
	audit := auditNote(in)

	var programId string
	claimed := false
	err := db.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		claimable := []string{"processing", "pending", "secondpass", "fraudreview"}
		if in.ClaimChanges {
			claimable = append(claimable, "changes")
		}
		// Atomic claim so only one concurrent decider proceeds; racing deciders
		// (ari's in-process jobs, another replica, a human decision) lose the update.
		tag, err := tx.Exec(ctx, `
			update "Submission" set status = $3::"SubmissionStatus", "claimedById" = null, "claimedAt" = null
			where id = $1 and status = any($2::"SubmissionStatus"[])`,
			in.SubmissionId, claimable, decision)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil // already decided
		}
		if _, err := tx.Exec(ctx, `
			update "SubmissionOpen" set "closedAt" = now(), "closeReason" = 'left'
			where "submissionId" = $1 and "closedAt" is null`, in.SubmissionId); err != nil { // a held ship decided out from under its reviewer would otherwise strand an open session row forever
			return err
		}
		if err := tx.QueryRow(ctx, `select "programId" from "Submission" where id = $1`, in.SubmissionId).Scan(&programId); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			insert into "Review" (id, "submissionId", "reviewerId", decision, "noteToMaker", "auditNote", "fieldValues", checklist, "approvedSeconds", "approvedMinutes", adjustments)
			values ($1, $2, $3, $6::"ReviewDecision", $4, $5, '{}'::jsonb, '[]'::jsonb, 0, 0, '{}'::jsonb)`,
			ids.Cuid(), in.SubmissionId, SystemUserId, note, audit, decision); err != nil {
			return err
		}
		meta := map[string]any{"reason": string(in.Reason), "auto": true}
		if len(in.Who) > 0 {
			meta["who"] = in.Who
		}
		if _, err := tx.Exec(ctx, `
			insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
			values ($1, $2, upper($6)::"ActivityKind", $3, $4, $5)`,
			ids.Cuid(), programId, in.SubmissionId, activityText(in), meta, decision); err != nil {
			return err
		}
		claimed = true
		return nil
	})
	if err != nil {
		slog.Error("automated decision failed", "submissionId", in.SubmissionId, "decision", decision, "reason", in.Reason, "err", err)
		return
	}
	if !claimed {
		return
	}

	if s.OnFraudDecided != nil {
		s.OnFraudDecided(ctx, in.SubmissionId) // a ship claimed out of fraudreview may leave fraud review state behind
	}

	// Name every collaborator with explicit zeros so the payload shape matches
	// human decisions; a system decision approves nobody's time.
	collabMinutes := map[string]outbound.CollaboratorMinutes{}
	rows, err := s.Pool.Query(ctx, `select "makerId" from "SubmissionCollaborator" where "submissionId" = $1`, in.SubmissionId)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var makerId string
			if rows.Scan(&makerId) == nil {
				collabMinutes[makerId] = outbound.CollaboratorMinutes{}
			}
		}
	}
	if len(collabMinutes) == 0 {
		collabMinutes = nil
	}

	zero := 0
	s.Outbound.DispatchReview(ctx, outbound.DispatchInput{
		Event:               "review." + decision,
		Decision:            &decision,
		ProgramId:           programId,
		SubmissionId:        in.SubmissionId,
		ReviewerId:          SystemUserId,
		Note:                note,
		AuditNote:           &audit,
		ApprovedMinutes:     &zero,
		CollaboratorMinutes: collabMinutes,
	})
}

// LogEvidenceIssue mirrors ari's audit trail for capture/demo failures so
// organizers can see why a capture failed without server access. Best-effort.
func (s *Service) LogEvidenceIssue(ctx context.Context, submissionId, source string, attempt int, errText string, notes []string, exhausted bool) {
	var programId, title string
	err := s.Pool.QueryRow(ctx,
		`select "programId", title from "Submission" where id = $1`, submissionId).Scan(&programId, &title)
	if err != nil {
		return
	}
	text := "Evidence capture failed - " + title
	if source == "demo" {
		text = "Demo probe failed - " + title
	}
	meta := map[string]any{"source": source, "attempt": attempt, "error": sliceRunes(errText, 300)}
	if len(notes) > 0 {
		if len(notes) > 10 {
			notes = notes[:10]
		}
		meta["notes"] = notes
	}
	if exhausted {
		meta["exhausted"] = true
	}
	_, err = s.Pool.Exec(ctx, `
		insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
		values ($1, $2, 'EVIDENCE'::"ActivityKind", $3, $4, $5)`,
		ids.Cuid(), programId, submissionId, text, meta)
	if err != nil {
		slog.Warn("evidence audit write failed", "submissionId", submissionId, "err", err)
	}
}

func sliceRunes(s string, max int) string {
	r := []rune(s)
	if len(r) > max {
		return string(r[:max])
	}
	return s
}
