package outbound

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/cryptobox"
	"github.com/hackclub/ari-webhooks/internal/httpx"
	"github.com/hackclub/ari-webhooks/internal/ids"
	"github.com/hackclub/ari-webhooks/internal/ssrf"
)

type Worker struct {
	Pool                     *pgxpool.Pool
	Codec                    *cryptobox.Codec
	WorkerId                 string
	AllowPrivateDestinations bool // TEST ONLY: bypasses the SSRF guard; production wiring must never set it or deliveries can be aimed at the internal network
	wake                     chan struct{}
}

func NewWorker(pool *pgxpool.Pool, codec *cryptobox.Codec, workerId string) *Worker {
	return &Worker{Pool: pool, Codec: codec, WorkerId: workerId, wake: make(chan struct{}, 1)}
}

func (w *Worker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run polls for due deliveries until ctx ends. The 30s tick is the fallback for
// when a NOTIFY wake is lost.
func (w *Worker) Run(ctx context.Context) {
	for {
		claimed, err := w.claimDue(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("outbound claim failed", "err", err)
		}
		if len(claimed) > 0 {
			httpx.MapLimit(ctx, claimed, 8, func(ctx context.Context, id string, _ int) struct{} {
				w.processDelivery(ctx, id)
				return struct{}{}
			})
			continue // a full backlog may remain; poll again immediately
		}
		select {
		case <-ctx.Done():
			return
		case <-w.wake:
		case <-time.After(30 * time.Second):
		}
	}
}

// claimDue leases due PENDING rows. Rows with no schedule entry become claimable
// only 5 minutes after createdAt so ari's still-live in-process retry chains are
// never raced during the transition; NOTIFY-scheduled rows are claimed at once.
func (w *Worker) claimDue(ctx context.Context) ([]string, error) {
	rows, err := w.Pool.Query(ctx, `
		with due as (
			select d.id
			from "OutboundDelivery" d
			left join ariw."outboundSchedule" s on s."deliveryId" = d.id
			where d.status = 'PENDING'
			  and coalesce(s."nextAttemptAt", d."createdAt" + interval '5 minutes') <= now()
			  and (s."deliveryId" is null or s."leaseUntil" <= now())
			order by d."createdAt"
			limit 50
			for update of d skip locked
		)
		insert into ariw."outboundSchedule" ("deliveryId", "nextAttemptAt", "leaseUntil", "workerId")
		select id, now(), now() + interval '2 minutes', $1 from due
		on conflict ("deliveryId") do update
			set "leaseUntil" = excluded."leaseUntil", "workerId" = excluded."workerId"
		returning "deliveryId"`,
		w.WorkerId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (w *Worker) processDelivery(ctx context.Context, deliveryId string) {
	var (
		programId, event  string
		submissionId      *string
		payload           *string
		attempts          int
		lastStatus        *int
		ageSeconds        float64
		endpointEnabled   *bool
		endpointUrl       *string
		endpointSecretEnc *string
		programArchived   bool
	)
	err := w.Pool.QueryRow(ctx, `
		select d."programId", d.event, d."submissionId", d.payload, d.attempts, d."httpStatus",
		       extract(epoch from now() - d."createdAt"),
		       e.enabled, e.url, e."secretEnc",
		       coalesce(p.status = 'ARCHIVED', false)
		from "OutboundDelivery" d
		left join "OutboundEndpoint" e on e."programId" = d."programId"
		left join "Program" p on p.id = d."programId"
		where d.id = $1 and d.status = 'PENDING'`,
		deliveryId).Scan(&programId, &event, &submissionId, &payload, &attempts, &lastStatus,
		&ageSeconds, &endpointEnabled, &endpointUrl, &endpointSecretEnc, &programArchived)
	if err != nil {
		if ctx.Err() == nil && err != pgx.ErrNoRows {
			slog.Warn("outbound load failed", "deliveryId", deliveryId, "err", err)
		}
		return
	}

	if payload == nil {
		w.markFailed(ctx, deliveryId, lastStatus, "cannot resume (no stored payload)", attempts)
		return
	}
	if programArchived { // an archived program sends nothing; restoring it within the hold window lets the delivery go out
		w.holdOrFail(ctx, deliveryId, lastStatus, "program archived", attempts, ageSeconds)
		return
	}
	if endpointEnabled == nil || !*endpointEnabled || endpointUrl == nil || *endpointUrl == "" ||
		endpointSecretEnc == nil || *endpointSecretEnc == "" ||
		!(w.AllowPrivateDestinations || ssrf.IsSafeOutboundUrl(*endpointUrl)) { // re-checked every send: endpoint config and SSRF safety may have changed since dispatch
		w.holdOrFail(ctx, deliveryId, lastStatus, "endpoint unavailable on resume", attempts, ageSeconds)
		return
	}
	secret, err := w.Codec.Decrypt(*endpointSecretEnc)
	if err != nil {
		w.holdOrFail(ctx, deliveryId, lastStatus, "secret undecryptable on resume", attempts, ageSeconds)
		return
	}
	if ageSeconds > 12*3600 && attempts > 0 { // a destination dead for half a day is FAILED, not endlessly retried; a never-attempted row still gets its shot
		w.markFailed(ctx, deliveryId, lastStatus, "abandoned (orphaned past retry horizon)", attempts)
		return
	}

	client := httpClient
	if w.AllowPrivateDestinations {
		client = privateDestinationClient // the httptest receiver is loopback, which the vetted dialer refuses
	}
	result := sendOnce(ctx, client, deliveryId, *endpointUrl, secret, []byte(*payload))
	slog.Info("outbound attempt",
		"deliveryId", deliveryId, "event", event, "urlOrigin", ssrf.OriginOf(*endpointUrl),
		"attempt", attempts+1, "delivered", result.delivered, "httpStatus", intOrNil(result.httpStatus), "errorDetail", result.errorDetail)

	if result.delivered {
		_, err := w.Pool.Exec(ctx, `
			update "OutboundDelivery"
			set status = 'DELIVERED', "httpStatus" = $2, attempts = $3, "deliveredAt" = now(), payload = null
			where id = $1`,
			deliveryId, result.httpStatus, attempts+1)
		if err != nil {
			slog.Warn("delivered update failed", "deliveryId", deliveryId, "err", err)
		}
		w.dropSchedule(ctx, deliveryId)
		w.logDelivery(ctx, deliveryId, "DELIVERED", result.httpStatus, "", attempts+1)
		return
	}

	if s := result.httpStatus; s != nil && *s >= 400 && *s < 500 && *s != 408 && *s != 429 { // the destination understood the request and rejected it; retrying identical bytes cannot succeed
		w.markFailed(ctx, deliveryId, result.httpStatus, result.errorDetail, attempts+1)
		return
	}

	backoff := []time.Duration{2 * time.Second, 10 * time.Second, 30 * time.Second,
		2 * time.Minute, 10 * time.Minute, 30 * time.Minute, time.Hour, 2 * time.Hour, 4 * time.Hour}
	if attempts >= len(backoff) {
		w.markFailed(ctx, deliveryId, result.httpStatus, result.errorDetail, attempts+1)
		return
	}
	_, err = w.Pool.Exec(ctx, `
		update "OutboundDelivery" set status = 'PENDING', "httpStatus" = $2, "errorDetail" = $3, attempts = $4
		where id = $1`,
		deliveryId, result.httpStatus, result.errorDetail, attempts+1)
	if err != nil {
		slog.Warn("retry update failed", "deliveryId", deliveryId, "err", err)
	}
	_, err = w.Pool.Exec(ctx, `
		update ariw."outboundSchedule"
		set "nextAttemptAt" = now() + $2 * interval '1 second', "leaseUntil" = '-infinity'
		where "deliveryId" = $1`,
		deliveryId, backoff[attempts].Seconds())
	if err != nil {
		slog.Warn("retry schedule failed", "deliveryId", deliveryId, "err", err)
	}
	w.logDelivery(ctx, deliveryId, "RETRYING", result.httpStatus, result.errorDetail, attempts+1)
}

// holdOrFail parks a delivery whose endpoint is currently disabled or
// misconfigured: the row stays PENDING and is re-checked later without
// consuming an attempt, so fixing the endpoint settings resumes queued
// deliveries instead of finding them terminally failed. Past the 12-hour
// horizon the hold gives up and the delivery fails.
func (w *Worker) holdOrFail(ctx context.Context, deliveryId string, httpStatus *int, errorDetail string, attempts int, ageSeconds float64) {
	if ageSeconds > 12*3600 {
		w.markFailed(ctx, deliveryId, httpStatus, errorDetail, attempts)
		return
	}
	_, err := w.Pool.Exec(ctx, `
		update ariw."outboundSchedule"
		set "nextAttemptAt" = now() + interval '10 minutes', "leaseUntil" = '-infinity'
		where "deliveryId" = $1`, deliveryId)
	if err != nil {
		slog.Warn("hold schedule failed", "deliveryId", deliveryId, "err", err)
	}
}

func (w *Worker) markFailed(ctx context.Context, deliveryId string, httpStatus *int, errorDetail string, attempts int) {
	_, err := w.Pool.Exec(ctx, `
		update "OutboundDelivery"
		set status = 'FAILED', "httpStatus" = $2, "errorDetail" = $3, attempts = $4, payload = null
		where id = $1`,
		deliveryId, httpStatus, errorDetail, attempts)
	if err != nil {
		slog.Warn("failed update failed", "deliveryId", deliveryId, "err", err)
	}
	w.dropSchedule(ctx, deliveryId)
	w.logDelivery(ctx, deliveryId, "FAILED", httpStatus, errorDetail, attempts)
}

func (w *Worker) dropSchedule(ctx context.Context, deliveryId string) {
	if _, err := w.Pool.Exec(ctx, `delete from ariw."outboundSchedule" where "deliveryId" = $1`, deliveryId); err != nil {
		slog.Warn("schedule cleanup failed", "deliveryId", deliveryId, "err", err)
	}
}

// logDelivery is best-effort: a failed audit write must not break the retry loop.
func (w *Worker) logDelivery(ctx context.Context, deliveryId, status string, httpStatus *int, errorDetail string, attempts int) {
	var programId, event, url string
	var submissionId *string
	err := w.Pool.QueryRow(ctx,
		`select "programId", "submissionId", event, url from "OutboundDelivery" where id = $1`,
		deliveryId).Scan(&programId, &submissionId, &event, &url)
	if err != nil {
		return
	}
	isTest := submissionId != nil && *submissionId == "TEST-0000"
	verb := "failed"
	switch status {
	case "DELIVERED":
		verb = "delivered"
	case "RETRYING":
		verb = "attempt " + itoa(attempts) + " failed, retrying"
	}
	testTag := ""
	activitySubmission := submissionId
	if isTest {
		testTag = "test ping "
		activitySubmission = nil // pings read as pings in the feed, not real traffic
	}
	meta := map[string]any{
		"event":       event,
		"status":      status,
		"httpStatus":  intOrNil(httpStatus),
		"errorDetail": nilIfEmpty(errorDetail),
		"attempts":    attempts,
		"url":         ssrf.OriginOf(url),
		"deliveryId":  deliveryId,
	}
	if isTest {
		meta["test"] = true
	}
	_, err = w.Pool.Exec(ctx, `
		insert into "ActivityEvent" (id, "programId", kind, "submissionId", text, meta)
		values ($1, $2, 'DELIVERY'::"ActivityKind", $3, $4, $5)`,
		ids.Cuid(), programId, activitySubmission, "Webhook "+testTag+verb+" · "+event, meta)
	if err != nil {
		slog.Warn("delivery audit write failed", "deliveryId", deliveryId, "err", err)
	}
}

func intOrNil(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
