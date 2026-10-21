package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackclub/ari-webhooks/internal/autoreject"
	"github.com/hackclub/ari-webhooks/internal/ext"
	"github.com/hackclub/ari-webhooks/internal/filehours"
	"github.com/hackclub/ari-webhooks/internal/httpx"
	"github.com/hackclub/ari-webhooks/internal/integrations/githost"
	"github.com/hackclub/ari-webhooks/internal/integrations/hackatime"
	"github.com/hackclub/ari-webhooks/internal/integrations/lapse"
	"github.com/hackclub/ari-webhooks/internal/secs"
)

type Pipeline struct {
	Pool       *pgxpool.Pool
	Reject     *autoreject.Service
	Fraud      ext.FraudGateway
	FlagChecks ext.FlagChecks
	Evidence   ext.Evidence
	Git        *githost.Fetcher
	Hackatime  *hackatime.Client
	Lapse      *lapse.Client
}

type Result struct {
	OK                     bool
	Commits                int
	Clips                  int
	HackatimeMinutes       int
	AfterLastCommitMinutes int
	Notes                  []string
}

type person struct {
	makerId         string
	email           string
	slackId         string
	hackatimeUserId string
	collaboratorRow string // SubmissionCollaborator.id; "" on solo ships
}

type shipRow struct {
	programId, externalId, makerId, repoUrl string
	receivedAt                              time.Time
	hackatimeProjects                       []string
	programMeta                             map[string]any
	trackingStartsAt                        *time.Time
	maker                                   person
	collaborators                           []person
}

func jsRound(f float64) int {
	return int(math.Floor(f + 0.5))
}

// Enrich ports ari's enrich(): the one-shot evidence capture at ingest. Returns
// OK=false with diagnostic notes when the capture is degraded, so Handle keeps
// the retry window open instead of persisting a false zero. May settle the ship
// (auto-reject, request changes, fraud review): no reviewer has seen it yet.
func (p *Pipeline) Enrich(ctx context.Context, submissionId string) (Result, error) {
	return p.capture(ctx, submissionId, true)
}

// Recapture refreshes the evidence snapshot of a ship already sitting in the
// review queue (the reviewer-triggered resync). Same capture as Enrich, but
// deciding is the reviewer's territory now: data and the advisory flag checks
// refresh, while the settling actions (auto-reject, request changes, fraud
// review) never fire from here.
func (p *Pipeline) Recapture(ctx context.Context, submissionId string) (Result, error) {
	return p.capture(ctx, submissionId, false)
}

func (p *Pipeline) capture(ctx context.Context, submissionId string, settle bool) (Result, error) {
	var notes []string
	empty := func() Result {
		return Result{Notes: notes}
	}

	sub, found, err := p.loadShip(ctx, submissionId)
	if err != nil {
		return Result{}, err
	}
	if !found {
		notes = append(notes, "submission_not_found")
		return empty(), nil
	}

	collab := len(sub.collaborators) > 0
	people := sub.collaborators
	if !collab {
		people = []person{sub.maker}
	}

	// Tracking window: the LATER of the program start, the most recent approved
	// prior ship of this project, and the source app's _prior_credited_at meta,
	// up to the moment this ship entered the queue.
	priorApprovedMs, err := p.priorApprovedShipMs(ctx, sub.programId, sub.externalId, sub.receivedAt, submissionId)
	if err != nil {
		return Result{}, err
	}
	priorCreditedMs := int64(0)
	if raw, isString := sub.programMeta["_prior_credited_at"].(string); isString {
		if t, ok := parseJsDate(raw); ok {
			priorCreditedMs = t.UnixMilli()
		}
	}
	trackStartMs := int64(0)
	if sub.trackingStartsAt != nil {
		trackStartMs = sub.trackingStartsAt.UnixMilli()
	}
	trackFromMs := max(trackStartMs, priorApprovedMs, priorCreditedMs)
	trackUntilMs := sub.receivedAt.UnixMilli()
	if priorApprovedMs > trackStartMs {
		notes = append(notes, "window_from_prior_approved_ship")
	}
	if priorCreditedMs > max(trackStartMs, priorApprovedMs) {
		notes = append(notes, "window_from_prior_credited_meta")
	}

	// The window is applied where the commits are read. The filter below stays as
	// the one definition of which commits a ship owns, whatever the source returned.
	window := githost.Window{Until: sub.receivedAt}
	if trackFromMs > 0 {
		window.Since = time.UnixMilli(trackFromMs)
	}
	gh := p.Git.FetchCommits(ctx, sub.repoUrl, window)
	if gh.Error == "truncated" {
		notes = append(notes, "commit_history_truncated_at_1000")
	}
	if gh.Transient { // a transient failure must never write an empty snapshot
		note := gh.Error
		if note == "" {
			note = "git_unavailable"
		}
		notes = append(append(notes, note), gh.Notes...)
		return empty(), nil
	}
	if !gh.OK {
		note := gh.Error
		if note == "" {
			note = "git_empty"
		}
		notes = append(notes, note)
	}
	notes = append(notes, gh.Notes...)
	headPaths := make([]string, 0, len(gh.Files))
	for _, file := range gh.Files {
		headPaths = append(headPaths, file.Path)
	}

	allCommits := append([]githost.Commit{}, gh.Commits...)
	sort.SliceStable(allCommits, func(a, b int) bool {
		return allCommits[a].CommittedAt.Before(allCommits[b].CommittedAt)
	})
	var commits []githost.Commit
	for _, c := range allCommits {
		ms := c.CommittedAt.UnixMilli()
		if ms >= trackFromMs && ms <= trackUntilMs {
			commits = append(commits, c)
		}
	}
	if outside := gh.OutsideWindow + len(allCommits) - len(commits); outside > 0 {
		notes = append(notes, fmt.Sprintf("commits_outside_tracking_window_%d", outside))
	}

	projects := sub.hackatimeProjects
	hasHtId := false
	for _, pers := range people {
		if pers.hackatimeUserId != "" {
			hasHtId = true
		}
	}
	// Hackatime is consulted only when the ship declares it: a project list or a
	// known Hackatime id. program_hours-only ships skip it wholesale.
	htDeclared := len(projects) > 0 || hasHtId

	htIdByMakerId := map[string]string{}
	htResolveFailed := false
	if htDeclared {
		for _, pers := range people {
			uid := pers.hackatimeUserId
			if uid == "" {
				ok, resolved := p.Hackatime.ResolveUserId(ctx, pers.email, pers.slackId)
				if !ok {
					htResolveFailed = true // degraded: someone's time may be missing
				}
				uid = resolved
				if uid != "" {
					_, _ = p.Pool.Exec(ctx, `update "Maker" set "hackatimeUserId" = $2 where id = $1`, pers.makerId, uid)
				}
			}
			if uid != "" {
				htIdByMakerId[pers.makerId] = uid
			} else if collab {
				notes = append(notes, "hackatime_user_unresolved:"+pers.email)
			} else {
				notes = append(notes, "hackatime_user_unresolved")
			}
		}
		if len(htIdByMakerId) > 0 && len(projects) == 0 {
			notes = append(notes, "no_hackatime_projects_declared")
		}
	}

	makerIdByEmail := map[string]string{}
	for _, pers := range people {
		makerIdByEmail[pers.email] = pers.makerId
	}
	ownerOf := func(c githost.Commit) string {
		if !collab {
			return sub.makerId
		}
		if c.AuthorEmail == "" {
			return ""
		}
		return makerIdByEmail[c.AuthorEmail] // strict: an outside contributor attributes to nobody
	}
	ownIdx := map[string][]int{}
	for i, c := range commits {
		if owner := ownerOf(c); owner != "" {
			ownIdx[owner] = append(ownIdx[owner], i)
		}
	}

	// Heartbeats over the full tracking window [trackFrom, submission]. Anchoring
	// the window to the first commit would drop time logged before it, and makers
	// often code for weeks before their first push. Worth fetching even with zero
	// commits when projects are declared (hardware ships with a CAD repo_url
	// still have real Hackatime time).
	spansByMaker := map[string][]hackatime.Span{}
	// Per-file coding time, summed over everyone on the ship. Left nil unless at least
	// one person's capture came from the heartbeats walk, which is the only source that
	// carries file identity: nil means unknown, never "no files".
	var entitySeconds map[string]float64
	// Credit the one-third "ai coding" rate removed, summed over everyone on the
	// ship, persisted so the review screen can show what was discounted.
	aiDiscountedSeconds := 0.0
	// Spans of heartbeat-derived captures only, kept for the flag checks: their span
	// ends are real beat times, so end minus start is a genuine unbroken run.
	heartbeatSpansByMaker := map[string][]hackatime.Span{}
	var spanOrder []string
	markerUrls := map[string]bool{}
	htAttempted := len(htIdByMakerId) > 0 && (len(commits) > 0 || len(projects) > 0)
	htOk := true
	if htAttempted {
		endMs := trackUntilMs
		startMs := trackFromMs
		for _, pers := range people {
			makerId := pers.makerId
			uid, tracked := htIdByMakerId[makerId]
			if !tracked {
				continue
			}
			ht := p.Hackatime.FetchProjectSpansWindow(ctx, uid, projects, float64(startMs)/1000, float64(endMs)/1000)
			if !ht.OK {
				htOk = false
			}
			for _, note := range ht.Notes {
				if collab {
					notes = append(notes, note+":"+makerId)
				} else {
					notes = append(notes, note)
				}
			}
			if ht.AiDiscountedSeconds > 0 {
				aiDiscountedSeconds += ht.AiDiscountedSeconds
				aiNote := fmt.Sprintf("ai_coding_discounted_%dm", jsRound(ht.AiDiscountedSeconds/60))
				if collab {
					aiNote += ":" + makerId
				}
				notes = append(notes, aiNote)
			}
			spansByMaker[makerId] = ht.Spans
			spanOrder = append(spanOrder, makerId)
			if ht.EntitySeconds != nil {
				if entitySeconds == nil {
					entitySeconds = map[string]float64{}
				}
				for entity, seconds := range ht.EntitySeconds {
					entitySeconds[entity] += seconds
				}
				if len(ht.Spans) > 0 {
					heartbeatSpansByMaker[makerId] = ht.Spans
				}
			}

			var ownDays []string
			seenDays := map[string]bool{}
			for _, i := range ownIdx[makerId] {
				day := commits[i].CommittedAt.UTC().Format("2006-01-02")
				if !seenDays[day] {
					seenDays[day] = true
					ownDays = append(ownDays, day)
				}
			}
			dayResults := httpx.MapLimit(ctx, ownDays, 6, func(ctx context.Context, day string, _ int) hackatime.Day {
				return p.Hackatime.FetchTimeline(ctx, uid, day)
			})
			for _, r := range dayResults {
				if !r.OK {
					htOk = false
				}
				for _, m := range r.Markers {
					if m.GithubUrl != "" {
						markerUrls[strings.ToLower(m.GithubUrl)] = true
					}
				}
			}
		}
	} else if len(htIdByMakerId) > 0 && len(commits) == 0 {
		notes = append(notes, "no_commits_no_timeline_range")
	}

	// Lapse clips per person x declared project, fetched BEFORE the accounting so
	// each clip's wall-clock interval is subtracted out of the person's spans.
	// Lapse serves a project's whole recording history with no window of its own,
	// so the tracking window has to gate the clips here: [trackFrom, submission),
	// the same half-open shape the heartbeat walk uses. Without it an update ship
	// re-credits every clip a prior approval already paid for.
	type ownedClip struct {
		clip    lapse.Clip
		makerId string
		project string
	}
	var clips []ownedClip
	clipsOutOfWindow := 0
	lapseAttempted := len(htIdByMakerId) > 0 && len(projects) > 0
	lapseOk := true
	if lapseAttempted {
		for _, pers := range people {
			uid, tracked := htIdByMakerId[pers.makerId]
			if !tracked {
				continue
			}
			for _, key := range projects {
				r := p.Lapse.FetchClips(ctx, uid, key)
				if !r.OK {
					lapseOk = false
				}
				for _, clip := range r.Clips {
					at := clip.CreatedAt.UnixMilli()
					if at < trackFromMs || at >= trackUntilMs {
						clipsOutOfWindow++
						continue
					}
					clips = append(clips, ownedClip{clip: clip, makerId: pers.makerId, project: key})
				}
			}
		}
	}
	if clipsOutOfWindow > 0 {
		notes = append(notes, fmt.Sprintf("clips_outside_tracking_window_%d", clipsOutOfWindow))
	}

	lapseByMakerProject := map[string]map[string][]lapse.Interval{}
	for _, oc := range clips {
		start := float64(oc.clip.CreatedAt.UnixMilli()) / 1000
		if !(start > 0) || oc.clip.DurationSeconds <= 0 {
			continue // malformed clips keep their lapse credit but are not subtracted from spans
		}
		byProject := lapseByMakerProject[oc.makerId]
		if byProject == nil {
			byProject = map[string][]lapse.Interval{}
			lapseByMakerProject[oc.makerId] = byProject
		}
		byProject[oc.project] = append(byProject[oc.project], lapse.Interval{Start: start, End: start + oc.clip.DurationSeconds})
	}

	// Per-person verified time + display-only commit pinning. A person's verified
	// time is the sum of THEIR OWN in-project in-window spans; git-author matching
	// only drives which commit row displays it. With ZERO in-window commits,
	// lastCommitMs stays 0 and every span lands on the deflatable after-last-commit
	// row rather than the invisible unpinned bucket: a stale default branch (work
	// pushed or merged after submitting) must surface
	// as "time with no commits to anchor it", never as 0h to the reviewer.
	perCommitSeconds := make([]float64, len(commits))
	afterLastSeconds := 0.0
	lastCommitMs := int64(0)
	if len(commits) > 0 {
		lastCommitMs = commits[len(commits)-1].CommittedAt.UnixMilli()
	}
	verifiedSecondsByMaker := map[string]float64{}
	afterLastSecondsByMaker := map[string]float64{}
	// Per-person, per-project verified seconds (post lapse-dedup, matching
	// verifiedSecondsByMaker) so the review screen can show which tracked project
	// each collaborator's time came from.
	projectSecondsByMaker := map[string]map[string]float64{}
	unpinnedSeconds := 0.0
	unpinnedSecondsByMaker := map[string]float64{}
	lapseDedupedSeconds := 0.0
	// Per-day, per-project coding time for the review-screen heatmap. Built from the
	// raw verified Hackatime spans (before lapse dedup) so it reflects pure tracked
	// coding time; UTC-day buckets, aggregated across all makers on collab ships.
	heatmapSec := map[string]map[string]float64{}
	// Per-day, per-5min (UTC) coding time for the day-detail heatmap. Slot index is
	// 0-287 (hour*12 + minute/5). The UI aggregates these up to 30-min blocks for the
	// day overview and drills back down to 5-min when a block is clicked. Each span's
	// seconds are spread across the 5-min slots it covers, proportional to wall-clock
	// overlap, so a long session lights up every slot it touched.
	slotSec := map[string]map[int]float64{}
	addSlot := func(day string, slot int, sec float64) {
		if slotSec[day] == nil {
			slotSec[day] = map[int]float64{}
		}
		slotSec[day][slot] += sec
	}
	slotOf := func(t time.Time) int { return t.Hour()*12 + t.Minute()/5 }
	for _, makerId := range spanOrder {
		rawSpans := spansByMaker[makerId]
		for _, s := range rawSpans {
			if s.Project == "" {
				continue
			}
			day := time.Unix(int64(s.StartTime), 0).UTC().Format("2006-01-02")
			if heatmapSec[day] == nil {
				heatmapSec[day] = map[string]float64{}
			}
			heatmapSec[day][s.Project] += s.Duration

			start, end := int64(s.StartTime), int64(s.EndTime)
			if end <= start {
				t := time.Unix(start, 0).UTC()
				addSlot(t.Format("2006-01-02"), slotOf(t), s.Duration)
			} else {
				wall := float64(end - start)
				for cur := start; cur < end; {
					t := time.Unix(cur, 0).UTC()
					// seconds remaining until the next 5-min boundary
					secIntoSlot := int64(t.Minute()%5)*60 + int64(t.Second())
					next := cur + (300 - secIntoSlot)
					if next > end {
						next = end
					}
					addSlot(t.Format("2006-01-02"), slotOf(t), s.Duration*float64(next-cur)/wall)
					cur = next
				}
			}
		}
		personSpans := rawSpans
		if lp := lapseByMakerProject[makerId]; lp != nil {
			personSpans = lapse.SubtractLapseFromSpans(rawSpans, lp)
			before, after := 0.0, 0.0
			for _, s := range rawSpans {
				before += s.Duration
			}
			for _, s := range personSpans {
				after += s.Duration
			}
			lapseDedupedSeconds += before - after
		}
		own := ownIdx[makerId]
		for _, s := range personSpans {
			endMs := s.EndTime * 1000
			verifiedSecondsByMaker[makerId] += s.Duration
			if s.Project != "" {
				byProject := projectSecondsByMaker[makerId]
				if byProject == nil {
					byProject = map[string]float64{}
					projectSecondsByMaker[makerId] = byProject
				}
				byProject[s.Project] += s.Duration
			}
			if endMs > float64(lastCommitMs) {
				afterLastSeconds += s.Duration // logged after the final commit (or with no commits at all): the suspicious signal
				afterLastSecondsByMaker[makerId] += s.Duration
				continue
			}
			pinned := false
			for _, i := range own {
				if float64(commits[i].CommittedAt.UnixMilli()) >= endMs {
					perCommitSeconds[i] += s.Duration
					pinned = true
					break
				}
			}
			if !pinned && len(own) > 0 {
				perCommitSeconds[own[len(own)-1]] += s.Duration
				pinned = true
			}
			if !pinned {
				unpinnedSeconds += s.Duration
				unpinnedSecondsByMaker[makerId] += s.Duration
			}
		}
	}
	if unpinnedSeconds > 0 {
		notes = append(notes, fmt.Sprintf("unpinned_spans_%dm", jsRound(unpinnedSeconds/60)))
	}
	if lapseDedupedSeconds > 0 {
		notes = append(notes, fmt.Sprintf("lapse_overlap_deduped_%dm", jsRound(lapseDedupedSeconds/60)))
	}
	// one split over every cell (commit, after last commit, unpinned) so each view sums to the ship total
	cellWeights := append([]float64{}, perCommitSeconds...)
	for _, makerId := range spanOrder {
		cellWeights = append(cellWeights, afterLastSecondsByMaker[makerId], unpinnedSecondsByMaker[makerId])
	}
	hackatimeSeconds, cells := secs.Parts(cellWeights)
	commitSeconds := cells[:len(commits)]
	hackatimeSecondsByMaker := map[string]int{}
	afterLastWholeSecondsByMaker := map[string]int{}
	afterLastCommitSeconds := 0
	for i, c := range commits {
		hackatimeSecondsByMaker[ownerOf(c)] += commitSeconds[i]
	}
	for i, makerId := range spanOrder {
		afterLast := cells[len(commits)+2*i]
		afterLastWholeSecondsByMaker[makerId] = afterLast
		afterLastCommitSeconds += afterLast
		hackatimeSecondsByMaker[makerId] += afterLast + cells[len(commits)+2*i+1]
	}

	// Round the heatmap to whole seconds per project per day and marshal it for
	// the HoursBreakdown.hackatimeHeatmap JSONB column. Empty map ("{}") means the
	// ship is tracked but logged no time in the window.
	heatmap := map[string]map[string]int{}
	for day, byProject := range heatmapSec {
		rounded := map[string]int{}
		for project, seconds := range byProject {
			rounded[project] = jsRound(seconds)
		}
		heatmap[day] = rounded
	}
	heatmapJSON, err := json.Marshal(heatmap)
	if err != nil {
		heatmapJSON = []byte("{}")
	}

	// Per-person project attribution, split from that person's whole seconds and
	// marshaled per maker for the SubmissionCollaborator.hackatimeProjectSeconds and
	// legacy hackatimeProjectMinutes JSONB columns. Zero projects are dropped; a
	// tracked person with no time gets "{}".
	projectSecondsJSONByMaker := map[string]string{}
	projectMinutesJSONByMaker := map[string]string{}
	for makerId, byProject := range projectSecondsByMaker {
		names := make([]string, 0, len(byProject))
		for project := range byProject {
			names = append(names, project)
		}
		sort.Strings(names)
		weights := make([]float64, 0, len(names)+1)
		named := 0.0
		for _, project := range names {
			weights = append(weights, byProject[project])
			named += byProject[project]
		}
		weights = append(weights, verifiedSecondsByMaker[makerId]-named) // time on spans with no project name
		parts := secs.SplitFloat(hackatimeSecondsByMaker[makerId], weights)
		wholeSeconds := map[string]int{}
		wholeMinutes := map[string]int{}
		for i, project := range names {
			if parts[i] > 0 {
				wholeSeconds[project] = parts[i]
			}
			if m := secs.LegacyMinutes(parts[i]); m > 0 {
				wholeMinutes[project] = m
			}
		}
		projectSecondsJSONByMaker[makerId] = marshalOrEmpty(wholeSeconds)
		projectMinutesJSONByMaker[makerId] = marshalOrEmpty(wholeMinutes)
	}

	// 5-min buckets keyed by day → slot-string ("0".."287") → whole seconds. Sparse:
	// zero slots and empty days are dropped so the column only carries real activity.
	hourly := map[string]map[string]int{}
	for day, bySlot := range slotSec {
		rounded := map[string]int{}
		for slot, seconds := range bySlot {
			if r := jsRound(seconds); r > 0 {
				rounded[strconv.Itoa(slot)] = r
			}
		}
		if len(rounded) > 0 {
			hourly[day] = rounded
		}
	}
	hourlyJSON, err := json.Marshal(hourly)
	if err != nil {
		hourlyJSON = []byte("{}")
	}

	markerHashes := hexWindows(markerUrls, 40) // every 40-hex window once, instead of a substring scan of every marker per commit
	htSeen := func(c githost.Commit) bool {
		return markerUrls[strings.ToLower(c.Url)] || markerHashes[c.Hash]
	}

	// Health gates: only overwrite a source's cache when its data is
	// authoritative. A failed identity lookup degrades everything hour-related.
	htHealthy := (!htAttempted || htOk) && !htResolveFailed
	lapseHealthy := (!lapseAttempted || lapseOk) && !htResolveFailed
	fullySynced := htHealthy && lapseHealthy
	if htResolveFailed {
		notes = append(notes, "hackatime_resolve_failed")
	}
	if htAttempted && !htOk {
		notes = append(notes, "hackatime_partial_preserved")
	}
	if lapseAttempted && !lapseOk {
		notes = append(notes, "lapse_partial_preserved")
	}

	lapseWriteable := lapseAttempted && lapseOk && !htResolveFailed
	persistClips := make([]persistClip, len(clips))
	for i, oc := range clips {
		persistClips[i] = persistClip{clip: oc.clip, makerId: oc.makerId}
	}

	// Per-file rows for the review screen's file browser: entitySeconds resolved
	// against the repository while both sides of the match are in hand. nil
	// entitySeconds or an unread tree means unknown (never "no files"), and
	// persist leaves the previous snapshot alone.
	var fileHours []filehours.FileHours
	fileHoursKnown := gh.TreeRead && entitySeconds != nil
	if fileHoursKnown {
		headFiles := make(map[string]int64, len(gh.Files))
		for _, file := range gh.Files {
			headFiles[file.Path] = file.Bytes
		}
		fileHours = filehours.ResolveFileHours(entitySeconds, headFiles, gh.HistoryPaths)
	}
	emails := []string{sub.maker.email}
	for _, c := range sub.collaborators {
		emails = append(emails, c.email)
	}
	gathered, gatherNotes, err := p.Evidence.Gather(ctx, emails)
	if err != nil {
		return Result{}, err
	}
	notes = append(notes, gatherNotes...)

	persistNotes, err := p.persist(ctx, persistInput{
		submissionId:            submissionId,
		collab:                  collab,
		collaborators:           sub.collaborators,
		commits:                 commits,
		commitSeconds:           commitSeconds,
		htSeen:                  htSeen,
		ownerOf:                 ownerOf,
		trackFromMs:             trackFromMs,
		htHealthy:               htHealthy,
		lapseWriteable:          lapseWriteable,
		hackatimeSeconds:        hackatimeSeconds,
		afterLastCommitSeconds:  afterLastCommitSeconds,
		aiDiscountedSeconds:     secs.Round(aiDiscountedSeconds),
		hackatimeHeatmap:        string(heatmapJSON),
		hackatimeHourly:         string(hourlyJSON),
		hackatimeSecondsByMaker: hackatimeSecondsByMaker,
		afterLastSecondsByMaker: afterLastWholeSecondsByMaker,

		projectSecondsJSONByMaker: projectSecondsJSONByMaker,
		projectMinutesJSONByMaker: projectMinutesJSONByMaker,
		clips:                     persistClips,
		fullySynced:               fullySynced,
		treeRead:                  gh.TreeRead,
		files:                     gh.Files,
		fileHoursKnown:            fileHoursKnown,
		fileHours:                 fileHours,
		gathered:                  gathered,
	})
	if err != nil {
		return Result{}, err
	}
	notes = append(notes, persistNotes...)

	settled := false // an automated decision (reject or changes) already claimed this ship

	// Collaborative invariant: everyone on the ship must have logged time,
	// checked only on a complete healthy capture - and only at ingest; on a
	// recapture the reviewer sees the fresh zeros and decides themselves.
	if settle && fullySynced && collab {
		idle, err := p.collaboratorsWithoutHours(ctx, submissionId)
		if err == nil && len(idle) > 0 {
			notes = append(notes, fmt.Sprintf("collaborators_without_hours_%d", len(idle)))
			var idleNames, unfound []string
			for _, who := range idle {
				idleNames = append(idleNames, who.name)
				if _, tracked := htIdByMakerId[who.makerId]; htDeclared && !tracked {
					unfound = append(unfound, who.name)
				}
			}
			if len(unfound) == len(idle) {
				p.Reject.RequestChanges(ctx, autoreject.Input{
					SubmissionId:  submissionId,
					Reason:        autoreject.HackatimeAccountNotFound,
					Who:           unfound,
					Collaborative: true,
				})
			} else {
				p.Reject.AutoReject(ctx, autoreject.Input{
					SubmissionId: submissionId,
					Reason:       autoreject.CollaboratorNoHours,
					Who:          idleNames,
				})
			}
			settled = true
		}
	}

	if !settled {
		daySeconds := map[string]float64{}
		for day, byProject := range heatmapSec {
			for _, seconds := range byProject {
				daySeconds[day] += seconds
			}
		}
		checkNotes, checkSettled := p.FlagChecks.AfterCapture(ctx, ext.Capture{
			SubmissionId:       submissionId,
			Settle:             settle,
			HackatimeAttempted: htAttempted,
			HackatimeHealthy:   htHealthy,
			HackatimeProjects:  projects,
			SpansByMaker:       heartbeatSpansByMaker,
			EntitySeconds:      entitySeconds,
			DaySeconds:         daySeconds,
			TreeRead:           gh.TreeRead,
			HeadPaths:          headPaths,
			HistoryPaths:       gh.HistoryPaths,
			Readme:             gh.Readme,
		})
		notes = append(notes, checkNotes...)
		settled = checkSettled
	}

	// Never on a recapture: fraud review may hold a ship, which would pull a
	// queued one out from under the reviewer holding its claim.
	if settle && !settled {
		p.Fraud.OnEnteredQueue(ctx, submissionId)
	}

	return Result{
		OK:                     fullySynced,
		Commits:                len(commits),
		Clips:                  len(clips),
		HackatimeMinutes:       secs.LegacyMinutes(hackatimeSeconds),
		AfterLastCommitMinutes: secs.LegacyMinutes(afterLastCommitSeconds),
		Notes:                  notes,
	}, nil
}

func marshalOrEmpty(v map[string]int) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func hexWindows(urls map[string]bool, size int) map[string]bool {
	out := map[string]bool{}
	isHexLower := func(b byte) bool {
		return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')
	}
	for u := range urls {
		runStart := -1
		for i := 0; i <= len(u); i++ {
			inRun := i < len(u) && isHexLower(u[i])
			if inRun && runStart < 0 {
				runStart = i
			}
			if !inRun && runStart >= 0 {
				for w := runStart; w+size <= i; w++ {
					out[u[w:w+size]] = true
				}
				runStart = -1
			}
		}
	}
	return out
}

func parseJsDate(s string) (time.Time, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if d, err := time.Parse(layout, t); err == nil {
			return d, true
		}
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if d, err := time.ParseInLocation(layout, t, time.Local); err == nil {
			return d, true
		}
	}
	if d, err := time.Parse("2006-01-02", t); err == nil {
		return d, true
	}
	return time.Time{}, false
}

func (p *Pipeline) loadShip(ctx context.Context, submissionId string) (shipRow, bool, error) {
	var s shipRow
	var programMeta map[string]any
	err := p.Pool.QueryRow(ctx, `
		select sub."programId", sub."externalId", sub."makerId", sub."repoUrl", sub."receivedAt",
		       sub."hackatimeProjects", sub."programMeta", pr."trackingStartsAt",
		       m.id, m.email, coalesce(m."slackId", ''), coalesce(m."hackatimeUserId", '')
		from "Submission" sub
		join "Program" pr on pr.id = sub."programId"
		join "Maker" m on m.id = sub."makerId"
		where sub.id = $1`,
		submissionId).Scan(&s.programId, &s.externalId, &s.makerId, &s.repoUrl, &s.receivedAt,
		&s.hackatimeProjects, &programMeta, &s.trackingStartsAt,
		&s.maker.makerId, &s.maker.email, &s.maker.slackId, &s.maker.hackatimeUserId)
	if err == pgx.ErrNoRows {
		return shipRow{}, false, nil
	}
	if err != nil {
		return shipRow{}, false, err
	}
	s.programMeta = programMeta
	if s.programMeta == nil {
		s.programMeta = map[string]any{}
	}

	rows, err := p.Pool.Query(ctx, `
		select c.id, m.id, m.email, coalesce(m."slackId", ''), coalesce(m."hackatimeUserId", '')
		from "SubmissionCollaborator" c join "Maker" m on m.id = c."makerId"
		where c."submissionId" = $1 order by c.id`,
		submissionId)
	if err != nil {
		return shipRow{}, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var pers person
		if err := rows.Scan(&pers.collaboratorRow, &pers.makerId, &pers.email, &pers.slackId, &pers.hackatimeUserId); err != nil {
			return shipRow{}, false, err
		}
		s.collaborators = append(s.collaborators, pers)
	}
	return s, true, rows.Err()
}

func (p *Pipeline) priorApprovedShipMs(ctx context.Context, programId, externalId string, before time.Time, excludeId string) (int64, error) {
	var receivedAt time.Time
	err := p.Pool.QueryRow(ctx, `
		select "receivedAt" from "Submission"
		where "programId" = $1 and "externalId" = $2 and status = 'approved'
		  and id != $3 and "receivedAt" < $4
		order by "receivedAt" desc limit 1`,
		programId, externalId, excludeId, before).Scan(&receivedAt)
	if err == pgx.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return receivedAt.UnixMilli(), nil
}

type idlePerson struct {
	makerId, name string
}

func (p *Pipeline) collaboratorsWithoutHours(ctx context.Context, submissionId string) ([]idlePerson, error) {
	rows, err := p.Pool.Query(ctx, `
		select m.id, coalesce(nullif(m.name, ''), m.email)
		from "SubmissionCollaborator" c join "Maker" m on m.id = c."makerId"
		where c."submissionId" = $1
		  and c."hackatimeSeconds" + c."devlogSeconds" + c."lapseSeconds" + c."programSeconds" = 0
		  and c."hackatimeMinutes" + c."devlogMinutes" + c."lapseMinutes" + c."programMinutes" = 0 -- the old app writes no seconds
		order by c.id`,
		submissionId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []idlePerson
	for rows.Next() {
		var who idlePerson
		if err := rows.Scan(&who.makerId, &who.name); err != nil {
			return nil, err
		}
		out = append(out, who)
	}
	return out, rows.Err()
}
