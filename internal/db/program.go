package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// An unknown ship is not archived, so callers keep their own not-found handling.
func SubmissionArchived(ctx context.Context, pool *pgxpool.Pool, submissionId string) (bool, error) {
	var archived bool
	err := pool.QueryRow(ctx, `
		select exists (
			select 1 from "Submission" s join "Program" p on p.id = s."programId"
			where s.id = $1 and p.status = 'ARCHIVED'
		)`, submissionId).Scan(&archived)
	return archived, err
}
