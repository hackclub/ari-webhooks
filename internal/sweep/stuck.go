package sweep

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/autoreject"
)

// ReapStuckSubmissions rejects anything still processing 6h after ingest so the
// maker is told to resubmit.
func ReapStuckSubmissions(ctx context.Context, pool *pgxpool.Pool, reject *autoreject.Service) error {
	// Age from ingestedAt (wall-clock row creation), NOT receivedAt: receivedAt is
	// backdatable via shipped_at, which would make every migration ship "stuck".
	rows, err := pool.Query(ctx, `
		select s.id, extract(epoch from now() - s."ingestedAt") / 3600
		from "Submission" s join "Program" p on p.id = s."programId"
		where s.status = 'processing' and s."ingestedAt" < now() - interval '6 hours'
		  and p.status != 'ARCHIVED'`) // an archived program is paused: its held captures resume on restore instead of being rejected as stuck
	if err != nil {
		return err
	}
	defer rows.Close()
	type stuck struct {
		id       string
		ageHours float64
	}
	var wedged []stuck
	for rows.Next() {
		var s stuck
		if err := rows.Scan(&s.id, &s.ageHours); err != nil {
			return err
		}
		wedged = append(wedged, s)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(wedged) == 0 {
		return nil
	}

	// Sequential, not parallel: a burst of wedged ships must not fire N webhooks
	// at once. AutoReject atomically claims, so racing a late capture is a no-op.
	for _, s := range wedged {
		reject.AutoReject(ctx, autoreject.Input{
			SubmissionId: s.id,
			Reason:       autoreject.ProcessingTimeout,
			Detail:       fmt.Sprintf("stuck in processing for %.1fh", s.ageHours),
		})
	}
	slog.Info("auto-rejected wedged submissions", "count", len(wedged))
	return nil
}
