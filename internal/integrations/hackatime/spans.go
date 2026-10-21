package hackatime

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hackclub/ari-webhooks/internal/httpx"
)

type Client struct {
	BaseUrl  string
	AdminKey string
}

func (c *Client) base() string {
	return strings.TrimSuffix(strings.TrimSpace(c.BaseUrl), "/")
}

// a deployment that doesn't verify time: no admin key or no base url
func (c *Client) off() bool {
	return c.AdminKey == "" || c.base() == ""
}

// HeartbeatCapS is Hackatime's gap cap: a heartbeat extends the previous one by at most 2 minutes.
const HeartbeatCapS = 120

type Span struct {
	StartTime float64 // epoch seconds
	EndTime   float64
	Duration  float64
	Project   string // "" = unattributed
	// AiSeconds is the portion of Duration that came from "ai coding" heartbeats,
	// before any discount. DiscountAiCoding consumes it.
	AiSeconds float64
}

type CommitMarker struct {
	Timestamp float64
	Additions float64
	Deletions float64
	GithubUrl string
}

type Day struct {
	OK      bool // false = no key / network / non-2xx, so enrich can preserve the cache
	Spans   []Span
	Markers []CommitMarker
}

type Heartbeat struct {
	Time    float64
	Project string
	// Entity is the file the editor had open, as the maker's machine saw it: an
	// absolute local path, so only its tail is comparable to a repository path.
	Entity string
	// Kind is Hackatime's entity type. Anything other than "file" is not a path.
	Kind string
	// Category is the wakatime activity category. "ai coding" is credited at a
	// reduced rate everywhere time is counted.
	Category string
}

func (c *Client) authHeaders() map[string]string {
	return map[string]string{"authorization": "Bearer " + c.AdminKey, "accept": "application/json"}
}

// ResolveUserId resolves the Hackatime user id by email, then by slack id when
// no account has that email. OK false means lookup failed; the caller must
// not treat that as "no Hackatime account".
func (c *Client) ResolveUserId(ctx context.Context, email, slackId string) (ok bool, id string) {
	if c.off() {
		return true, "" // the deployment doesn't verify time: authoritative empty
	}
	body, _ := json.Marshal(map[string]string{"email": email})
	headers := c.authHeaders()
	headers["content-type"] = "application/json"
	res := httpx.JsonRetry(ctx, c.base()+"/api/admin/v1/user/get_user_by_email",
		httpx.Opts{Method: "POST", Headers: headers, Body: string(body), QuietStatuses: []int{404}}, 2)
	if res.OK {
		if id := userIdOf(res.Data); id != "" {
			return true, id
		}
	} else if res.Status != 404 {
		return false, ""
	}
	if slackId == "" {
		return true, ""
	}
	res = httpx.JsonRetry(ctx, c.base()+"/api/v1/users/lookup_slack_uid/"+url.PathEscape(slackId),
		httpx.Opts{Headers: c.authHeaders(), QuietStatuses: []int{404}}, 2)
	if res.OK {
		return true, userIdOf(res.Data)
	}
	return res.Status == 404, ""
}

func userIdOf(data any) string {
	d := httpx.Obj(data)
	user := httpx.Obj(d["user"])
	for _, candidate := range []any{d["id"], d["user_id"], d["userId"], user["id"]} {
		if candidate != nil {
			if s := jsAnyString(candidate); s != "" {
				return s
			}
		}
	}
	return ""
}

// FetchTimeline fetches one UTC day of timeline (spans + commit markers).
func (c *Client) FetchTimeline(ctx context.Context, userId, dateStr string) Day {
	if c.off() {
		return Day{OK: true}
	}
	res := httpx.JsonRetry(ctx,
		c.base()+"/api/admin/v1/timeline?date="+url.QueryEscape(dateStr)+"&user_ids="+url.QueryEscape(userId),
		httpx.Opts{Headers: c.authHeaders()}, 2)
	if !res.OK {
		return Day{}
	}
	spans, markers := normalizeDay(res.Data, userId)
	return Day{OK: true, Spans: spans, Markers: markers}
}

func (c *Client) fetchHeartbeatPage(ctx context.Context, userId, windowParams string, offset, limit int) (ok bool, beats []Heartbeat, hasMore bool) {
	res := httpx.JsonRetry(ctx,
		c.base()+"/api/admin/v1/user/heartbeats?user_id="+url.QueryEscape(userId)+windowParams+"&limit="+strconv.Itoa(limit)+"&offset="+strconv.Itoa(offset),
		httpx.Opts{Headers: c.authHeaders()}, 2)
	if !res.OK {
		return false, nil, false
	}
	d := httpx.Obj(res.Data)
	for _, raw := range httpx.Arr(d["heartbeats"]) {
		h := httpx.Obj(raw)
		beat := Heartbeat{
			Time:     httpx.Num(h["time"]),
			Project:  httpx.Str(h["project"]),
			Entity:   firstStr(h, "entity", "file", "path"),
			Kind:     firstStr(h, "type", "entity_type"),
			Category: firstStr(h, "category"),
		}
		if beat.Time > 0 {
			beats = append(beats, beat)
		}
	}
	return true, beats, d["has_more"] == true
}

// FetchHeartbeatsWindow returns all heartbeats with startTs <= time < endTs,
// ascending. The upstream start_date/end_date filter only takes whole epoch
// seconds (inclusive on both ends), so the window is widened to the enclosing
// integers server-side and trimmed back to the exact half-open bounds here.
func (c *Client) FetchHeartbeatsWindow(ctx context.Context, userId string, startTs, endTs float64) (ok bool, beats []Heartbeat) {
	if c.off() {
		return true, nil
	}
	if endTs <= startTs {
		return true, nil
	}
	windowParams := "&start_date=" + strconv.FormatInt(int64(math.Max(0, math.Floor(startTs))), 10) + // negative epochs 400 upstream
		"&end_date=" + strconv.FormatInt(int64(math.Ceil(endTs)), 10)
	offset := 0
	seen := map[Heartbeat]bool{}
	for {
		pageOk, page, hasMore := c.fetchHeartbeatPage(ctx, userId, windowParams, offset, 5000) // upstream max page size
		if !pageOk {
			return false, nil
		}
		if len(page) == 0 {
			break
		}
		for _, b := range page {
			if b.Time >= startTs && b.Time < endTs && !seen[b] {
				seen[b] = true
				beats = append(beats, b)
			}
		}
		if page[len(page)-1].Time >= endTs || !hasMore {
			break
		}
		// Upstream orders by time with no tiebreak, and heartbeats arriving mid-walk
		// shift every offset: either can slide a row across a page boundary and drop
		// it. Rereading the boundary absorbs that; the dedup above makes the repeats
		// free, since a collapsed identical beat only ever contributed a zero gap.
		step := len(page) - 100
		if step < 1 { // a page too small to afford the overlap still has to advance
			step = len(page)
		}
		offset += step
	}
	return true, beats
}

// aiCreditShare is the fraction of "ai coding" heartbeat time that counts toward a
// ship's verified hours: AI-assisted time is credited at one third.
const aiCreditShare = 1.0 / 3

func isAiCoding(category string) bool {
	c := strings.ToLower(strings.TrimSpace(category))
	c = strings.ReplaceAll(c, "_", " ")
	c = strings.ReplaceAll(c, "-", " ")
	return c == "ai coding"
}

// SpansFromHeartbeats reshapes Hackatime's exact counting rule into per-project
// spans: sessions break on gaps > 120s; each session after the first carries
// the capped 120s re-entry gap. Sum of durations == Hackatime's official total.
// Each gap's seconds are attributed to the category of the beat that opened it,
// so a span also knows how much of its duration was "ai coding".
func SpansFromHeartbeats(beats []Heartbeat) []Span {
	byProject := map[string][]Heartbeat{}
	var order []string
	for _, b := range beats {
		if b.Project == "" {
			continue
		}
		if _, seen := byProject[b.Project]; !seen {
			order = append(order, b.Project)
		}
		byProject[b.Project] = append(byProject[b.Project], b)
	}
	var out []Span
	for _, project := range order {
		bs := byProject[project]
		sort.SliceStable(bs, func(a, b int) bool { return bs[a].Time < bs[b].Time })
		sessionIdx := 0
		startIdx := 0
		aiSeconds := 0.0
		push := func(endIdx int) {
			start, end := bs[startIdx].Time, bs[endIdx].Time
			duration := end - start
			ai := aiSeconds
			if sessionIdx > 0 {
				duration += HeartbeatCapS
				if isAiCoding(bs[startIdx].Category) { // the re-entry credit follows the beat that opens the session
					ai += HeartbeatCapS
				}
			}
			if duration > 0 {
				out = append(out, Span{StartTime: start, EndTime: end, Duration: duration, Project: project, AiSeconds: ai})
			}
			sessionIdx++
			aiSeconds = 0
		}
		for i := 1; i < len(bs); i++ {
			if bs[i].Time-bs[i-1].Time > HeartbeatCapS {
				push(i - 1)
				startIdx = i
			} else if isAiCoding(bs[i-1].Category) {
				aiSeconds += bs[i].Time - bs[i-1].Time
			}
		}
		push(len(bs) - 1)
	}
	return out
}

// DiscountAiCoding reduces each span's credited duration so its "ai coding" seconds
// count at one third, returning the seconds removed. Wall-clock bounds are left
// untouched: an 8-hour AI session is still 8 hours of sitting there, it just is not
// credited like hand-written code.
func DiscountAiCoding(spans []Span) ([]Span, float64) {
	removed := 0.0
	var out []Span
	for _, s := range spans {
		cut := s.AiSeconds * (1 - aiCreditShare)
		if cut > 0 {
			s.Duration -= cut
			removed += cut
		}
		if s.Duration > 0 {
			out = append(out, s)
		}
	}
	return out, removed
}

func isoOf(ts float64) string {
	return time.Unix(int64(ts), 0).UTC().Format("2006-01-02T15:04:05.000Z")
}

// statsDenied reports whether a public-stats request was rejected outright rather
// than failing transiently: these endpoints 403 when the maker turned off
// allow_public_stats_lookup, and no amount of retrying changes that.
func statsDenied(res httpx.Result) bool {
	return res.Status == 401 || res.Status == 403
}

func (c *Client) fetchProjectTotal(ctx context.Context, userId, project string, startTs, endTs float64) (ok, denied bool, seconds float64) {
	res := httpx.JsonRetry(ctx,
		c.base()+"/api/v1/users/"+url.PathEscape(userId)+"/projects/details?projects="+url.QueryEscape(project)+
			"&start_date="+url.QueryEscape(isoOf(startTs))+"&end_date="+url.QueryEscape(isoOf(endTs)),
		httpx.Opts{Headers: c.authHeaders(), QuietStatuses: []int{403}}, 2) // an expected privacy setting, not a failure
	if !res.OK {
		return false, statsDenied(res), 0
	}
	for _, raw := range httpx.Arr(httpx.Obj(res.Data)["projects"]) {
		p := httpx.Obj(raw)
		if httpx.Str(p["name"]) == project {
			return true, false, httpx.Num(p["total_seconds"])
		}
	}
	return true, false, 0 // authoritative: no time for this project
}

func (c *Client) fetchSpansApi(ctx context.Context, userId, project string, startTs, endTs float64) (ok, denied bool, spans []Span) {
	res := httpx.JsonRetry(ctx,
		c.base()+"/api/v1/users/"+url.PathEscape(userId)+"/heartbeats/spans?project="+url.QueryEscape(project)+
			"&start_date="+url.QueryEscape(isoOf(startTs))+"&end_date="+url.QueryEscape(isoOf(endTs)),
		httpx.Opts{Headers: c.authHeaders(), QuietStatuses: []int{403}}, 2) // an expected privacy setting, not a failure
	if !res.OK {
		return false, statsDenied(res), nil
	}
	for _, raw := range httpx.Arr(httpx.Obj(res.Data)["spans"]) {
		s := httpx.Obj(raw)
		span := Span{
			StartTime: httpx.Num(s["start_time"]),
			EndTime:   httpx.Num(s["end_time"]),
			Duration:  httpx.Num(s["duration"]),
			Project:   project,
		}
		// Defensive window clamp: a span outside the asked window must never count.
		if span.Duration > 0 && span.EndTime >= startTs && span.StartTime < endTs {
			spans = append(spans, span)
		}
	}
	return true, false, spans
}

type ProjectSpansResult struct {
	OK     bool // false = capture suspect/degraded, caller must NOT persist it
	Spans  []Span
	Source string   // heartbeats | spans-api | none
	Notes  []string // diagnostic tokens surfaced on the submission's notes
	// EntitySeconds is per-file coding time, keyed by the raw entity path. Only the
	// heartbeats walk carries file identity, so this is nil on every other source and
	// callers must read nil as "unknown", never as "no files".
	EntitySeconds map[string]float64
	// AiDiscountedSeconds is how much credit the "ai coding" one-third rate removed
	// from Spans. Always 0 on non-heartbeat sources: the spans API carries no
	// categories, so recovered captures cannot be discounted.
	AiDiscountedSeconds float64
}

// EntitySeconds mirrors Hackatime's counting so file totals sum to the same
// number the maker sees logged: every gap is worth min(gap, 120s). A gap inside
// a session goes to the file that was open when it started; a session break's
// capped 120s follows the beat that opens the next session, the same beat the
// span re-entry credit follows, so files and spans stay consistent. Beats
// without a file path (apps, domains) keep their span time but have nowhere to
// carry it here. "ai coding" gaps carry the same one third credit as
// everywhere else, so per-file totals stay comparable to the ship's credited
// hours.
func EntitySeconds(beats []Heartbeat) map[string]float64 {
	ordered := append([]Heartbeat{}, beats...)
	sort.SliceStable(ordered, func(a, b int) bool { return ordered[a].Time < ordered[b].Time })
	out := map[string]float64{}
	credit := func(b Heartbeat, seconds float64) {
		if b.Entity == "" || (b.Kind != "" && b.Kind != "file") {
			return // an app or a domain heartbeat is not a path
		}
		if isAiCoding(b.Category) {
			seconds *= aiCreditShare
		}
		out[b.Entity] += seconds
	}
	for i := 0; i+1 < len(ordered); i++ {
		gap := ordered[i+1].Time - ordered[i].Time
		if gap <= 0 {
			continue
		}
		if gap > HeartbeatCapS {
			credit(ordered[i+1], HeartbeatCapS)
			continue
		}
		credit(ordered[i], gap)
	}
	return out
}

func totalOf(spans []Span) float64 {
	total := 0.0
	for _, s := range spans {
		total += s.Duration
	}
	return total
}

// FetchProjectSpansWindow ports ari's hardened capture: heartbeats walk as the
// primary source, cross-checked against the projects/details oracle, recovered
// through the public spans endpoint when they disagree. OK false keeps the
// retry window open instead of persisting a possible false zero.
func (c *Client) FetchProjectSpansWindow(ctx context.Context, userId string, projects []string, startTs, endTs float64) ProjectSpansResult {
	if c.off() {
		return ProjectSpansResult{OK: true, Source: "none"}
	}
	if len(projects) == 0 {
		return ProjectSpansResult{OK: true, Source: "none"}
	}

	declaredExact := map[string]bool{}
	declaredFold := map[string]bool{}
	for _, p := range projects {
		declaredExact[p] = true
		declaredFold[strings.ToLower(p)] = true
	}
	// Heartbeat projects match declared names case-insensitively: Hackatime stores
	// names verbatim and compares them byte-exact, so a renamed checkout ("game" to
	// "Game") splits one project into spellings the maker cannot see. Variant
	// spellings are credited here and surfaced as a note; the oracle cross-check
	// below still queries the declared spellings only, which can only read low and
	// the tolerance rule already forgives that direction.
	declared := func(project string) bool {
		return project != "" && declaredFold[strings.ToLower(project)]
	}
	hbOk, beats := c.FetchHeartbeatsWindow(ctx, userId, startTs, endTs)
	var derived []Span
	var entitySeconds map[string]float64
	variantSet := map[string]bool{}
	if hbOk {
		for _, s := range SpansFromHeartbeats(beats) {
			if declared(s.Project) {
				derived = append(derived, s)
				if !declaredExact[s.Project] {
					variantSet[s.Project] = true
				}
			}
		}
		var declaredBeats []Heartbeat
		for _, b := range beats {
			if declared(b.Project) {
				declaredBeats = append(declaredBeats, b)
			}
		}
		entitySeconds = EntitySeconds(declaredBeats)
	}
	var baseNotes []string
	if len(variantSet) > 0 {
		variants := slices.Sorted(maps.Keys(variantSet))
		baseNotes = append(baseNotes, "hackatime_case_variants_credited:"+strings.Join(variants, ","))
	}
	withNotes := func(r ProjectSpansResult, notes ...string) ProjectSpansResult {
		r.Notes = append(append([]string{}, baseNotes...), notes...)
		return r
	}
	// The oracle cross-check compares like with like: derivedS keeps Hackatime's
	// official counting, and the one-third AI discount lands on the spans only
	// after the source is chosen. Discounting first would read as a broken capture
	// and trigger the spans-api recovery, which cannot see categories and would
	// silently undo it.
	derivedS := totalOf(derived)
	aiRemoved := 0.0
	derived, aiRemoved = DiscountAiCoding(derived)

	recover := func() (ok bool, spans []Span) {
		results := httpx.MapLimit(ctx, projects, 4, func(ctx context.Context, p string, _ int) struct {
			ok    bool
			spans []Span
		} {
			ok, _, spans := c.fetchSpansApi(ctx, userId, p, startTs, endTs)
			return struct {
				ok    bool
				spans []Span
			}{ok, spans}
		})
		var all []Span
		for _, r := range results {
			if !r.ok {
				return false, nil
			}
			all = append(all, r.spans...)
		}
		return true, all
	}

	if !hbOk {
		if recOk, recSpans := recover(); recOk {
			return withNotes(ProjectSpansResult{OK: true, Spans: recSpans, Source: "spans-api"}, "hackatime_recovered_after_heartbeats_failure")
		}
		return withNotes(ProjectSpansResult{Source: "none"})
	}

	oracleResults := httpx.MapLimit(ctx, projects, 4, func(ctx context.Context, p string, _ int) struct {
		ok      bool
		denied  bool
		seconds float64
	} {
		ok, denied, seconds := c.fetchProjectTotal(ctx, userId, p, startTs, endTs)
		return struct {
			ok      bool
			denied  bool
			seconds float64
		}{ok, denied, seconds}
	})
	oracleOk := true
	oracleDenied := false
	oracleS := 0.0
	for _, r := range oracleResults {
		oracleOk = oracleOk && r.ok
		oracleDenied = oracleDenied || r.denied
		oracleS += r.seconds
	}

	if oracleDenied {
		// The maker turned off public stats lookup, which 403s the oracle and the
		// spans recovery alike; retrying can never verify anything. The admin
		// heartbeats walk already succeeded, so its answer is authoritative even
		// at zero, and persisting it beats wedging the capture open forever.
		return withNotes(ProjectSpansResult{OK: true, Spans: derived, Source: "heartbeats", EntitySeconds: entitySeconds, AiDiscountedSeconds: aiRemoved},
			"hackatime_oracle_denied")
	}

	if !oracleOk {
		if derivedS > 0 {
			return withNotes(ProjectSpansResult{OK: true, Spans: derived, Source: "heartbeats", EntitySeconds: entitySeconds, AiDiscountedSeconds: aiRemoved})
		}
		if recOk, recSpans := recover(); recOk {
			if totalOf(recSpans) > 0 {
				return withNotes(ProjectSpansResult{OK: true, Spans: recSpans, Source: "spans-api"}, "hackatime_zero_recovered")
			}
			return withNotes(ProjectSpansResult{OK: true, Source: "heartbeats", EntitySeconds: entitySeconds}) // two sources agree: genuine 0, nothing to discount
		}
		return withNotes(ProjectSpansResult{Source: "none"}, "hackatime_zero_unverifiable")
	}

	tolerance := math.Max(1800, oracleS*0.25) // disagreement allowance before a capture is distrusted
	if derivedS >= oracleS-tolerance {
		return withNotes(ProjectSpansResult{OK: true, Spans: derived, Source: "heartbeats", EntitySeconds: entitySeconds, AiDiscountedSeconds: aiRemoved})
	}

	if recOk, recSpans := recover(); recOk && totalOf(recSpans) > derivedS {
		return withNotes(ProjectSpansResult{OK: true, Spans: recSpans, Source: "spans-api"},
			fmt.Sprintf("hackatime_recovered_via_spans_%dm_vs_%dm", jsRoundMinutes(totalOf(recSpans)), jsRoundMinutes(derivedS)))
	}
	if derivedS == 0 {
		// The oracle says there IS time but neither source produced it.
		return withNotes(ProjectSpansResult{Source: "none"}, "hackatime_zero_contradicts_oracle")
	}
	return withNotes(ProjectSpansResult{OK: true, Spans: derived, Source: "heartbeats", EntitySeconds: entitySeconds, AiDiscountedSeconds: aiRemoved},
		fmt.Sprintf("hackatime_short_vs_oracle_%dm_vs_%dm", jsRoundMinutes(derivedS), jsRoundMinutes(oracleS)))
}

func jsRoundMinutes(seconds float64) int {
	return int(math.Floor(seconds/60 + 0.5))
}

func collectContainers(data any, userId string) []map[string]any {
	d := httpx.Obj(data)
	if _, isArray := d["spans"].([]any); isArray {
		return []map[string]any{d} // flat single-user shape
	}
	var out []map[string]any
	if users, isArray := d["users"].([]any); isArray {
		// List shape (the live one): the endpoint can return OTHER users too, so
		// match on user.id or someone else's time gets credited to this maker.
		for _, raw := range users {
			c := httpx.Obj(raw)
			uid := httpx.Obj(c["user"])["id"]
			if uid == nil {
				uid = c["id"]
			}
			if uid == nil {
				uid = c["user_id"]
			}
			if uid != nil && jsAnyString(uid) == userId {
				out = append(out, c)
			}
		}
	} else {
		for k, v := range httpx.Obj(d["users"]) {
			c := httpx.Obj(v)
			uid := httpx.Obj(c["user"])["id"]
			if uid == nil {
				uid = c["id"]
			}
			uidStr := k
			if uid != nil {
				uidStr = jsAnyString(uid)
			}
			if uidStr == userId {
				out = append(out, c)
			}
		}
	}
	return out
}

func normalizeSpans(raw []any) []Span {
	var out []Span
	for _, r := range raw {
		s := httpx.Obj(r)
		start := httpx.Num(s["start_time"])
		end := httpx.Num(s["end_time"])
		details := httpx.Arr(s["projects_edited_details"])
		if len(details) > 0 {
			spanSeconds := firstNum(s, "duration", "total_seconds")
			for _, dRaw := range details {
				d := httpx.Obj(dRaw)
				explicit := firstNum(d, "duration", "total_seconds", "seconds", "total")
				duration := explicit
				if explicit <= 0 {
					duration = spanSeconds / float64(len(details)) // even split: conservative, never double-counts
				}
				out = append(out, Span{
					StartTime: start, EndTime: end, Duration: duration,
					Project: firstStr(d, "name", "project", "key", "project_name"),
				})
			}
		} else {
			out = append(out, Span{
				StartTime: start, EndTime: end,
				Duration: firstNum(s, "duration", "total_seconds"),
				Project:  firstStr(s, "project", "project_name", "project_key"),
			})
		}
	}
	var filtered []Span
	for _, s := range out {
		if s.Duration > 0 {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func normalizeMarkers(raw []any) []CommitMarker {
	var out []CommitMarker
	for _, r := range raw {
		m := httpx.Obj(r)
		out = append(out, CommitMarker{
			Timestamp: firstNum(m, "timestamp", "time", "committed_at"),
			Additions: firstNum(m, "additions", "lines_added"),
			Deletions: firstNum(m, "deletions", "lines_removed"),
			GithubUrl: firstStr(m, "github_url", "url", "html_url"),
		})
	}
	return out
}

func normalizeDay(data any, userId string) ([]Span, []CommitMarker) {
	var spans []Span
	var markers []CommitMarker
	for _, c := range collectContainers(data, userId) {
		spans = append(spans, normalizeSpans(httpx.Arr(c["spans"]))...)
		markers = append(markers, normalizeMarkers(httpx.Arr(c["commit_markers"]))...)
	}
	// Top-level markers are shared across the response and safe: htSeen matches
	// them against this ship's own commit URLs/hashes.
	markers = append(markers, normalizeMarkers(httpx.Arr(httpx.Obj(data)["commit_markers"]))...)
	return spans, markers
}

// firstNum mirrors TS `num(a ?? b ?? ...)`: the first PRESENT key wins even if
// it coerces to 0, matching nullish (not truthy) coalescing.
func firstNum(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		if v, present := m[k]; present && v != nil {
			return httpx.Num(v)
		}
	}
	return 0
}

func firstStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, present := m[k]; present && v != nil {
			if s := httpx.Str(v); s != "" {
				return s
			}
			return "" // present but empty/non-string: str() returns null, coalescing stops only on non-null
		}
	}
	return ""
}

func jsAnyString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	}
	return ""
}
