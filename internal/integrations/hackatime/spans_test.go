package hackatime

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"testing"
)

// The 120s-cap session math must stay byte-exact with Hackatime's official
// totals (and ari's TS port): sessions break on gaps > 120s, each session
// after the first carries the capped 120s re-entry gap.
func TestSpansFromHeartbeatsSessionMath(t *testing.T) {
	beats := []Heartbeat{
		{Time: 1000, Project: "app"},
		{Time: 1060, Project: "app"},
		{Time: 1120, Project: "app"}, // gap 60: same session
		{Time: 1500, Project: "app"}, // gap 380 > 120: new session
		{Time: 1550, Project: "app"},
		{Time: 900, Project: "other"},
		{Time: 950, Project: ""}, // unattributed: dropped
	}
	spans := SpansFromHeartbeats(beats)
	if len(spans) != 2 { // the single-beat "other" first session has duration 0 and is dropped
		t.Fatalf("spans: %+v", spans)
	}
	app1, app2 := spans[0], spans[1]
	if app1.StartTime != 1000 || app1.EndTime != 1120 || app1.Duration != 120 {
		t.Fatalf("first session (no re-entry gap): %+v", app1)
	}
	if app2.StartTime != 1500 || app2.EndTime != 1550 || app2.Duration != 50+120 {
		t.Fatalf("second session must carry the 120s re-entry credit: %+v", app2)
	}
}

func TestSpansSingleBeatSessionsDropZeroDurations(t *testing.T) {
	spans := SpansFromHeartbeats([]Heartbeat{{Time: 500, Project: "solo"}})
	if len(spans) != 0 {
		t.Fatalf("a lone first-session heartbeat has duration 0 and is dropped: %+v", spans)
	}
	spans = SpansFromHeartbeats([]Heartbeat{
		{Time: 500, Project: "solo"},
		{Time: 1000, Project: "solo"}, // second session, single beat: 0 + 120 re-entry
	})
	if len(spans) != 1 || spans[0].Duration != 120 {
		t.Fatalf("lone re-entry beat keeps the 120s credit: %+v", spans)
	}
}

func TestSpansUnsortedInputIsSorted(t *testing.T) {
	spans := SpansFromHeartbeats([]Heartbeat{
		{Time: 1120, Project: "app"},
		{Time: 1000, Project: "app"},
		{Time: 1060, Project: "app"},
	})
	if len(spans) != 1 || spans[0].Duration != 120 {
		t.Fatalf("timestamps must be sorted before the session walk: %+v", spans)
	}
}

func TestOracleToleranceRule(t *testing.T) {
	if got := math.Max(1800, 1000*0.25); got != 1800 {
		t.Fatalf("small totals use the 1800s floor: %v", got)
	}
	if got := math.Max(1800, 20000*0.25); got != 5000 {
		t.Fatalf("large totals use 25 percent: %v", got)
	}
}

func TestEntitySecondsCreditsTheFileThatWasOpen(t *testing.T) {
	// Two 60s gaps on game.ts, then a switch to audio.ts for one 60s gap, then a
	// 10-minute break (capped at 120s, credited to the file that reopens work)
	// before a final 60s gap on audio.ts.
	beats := []Heartbeat{
		{Time: 1000, Project: "snake", Entity: "/home/mia/snake/game.ts", Kind: "file"},
		{Time: 1060, Project: "snake", Entity: "/home/mia/snake/game.ts", Kind: "file"},
		{Time: 1120, Project: "snake", Entity: "/home/mia/snake/audio.ts", Kind: "file"},
		{Time: 1180, Project: "snake", Entity: "/home/mia/snake/audio.ts", Kind: "file"},
		{Time: 1800, Project: "snake", Entity: "/home/mia/snake/audio.ts", Kind: "file"},
		{Time: 1860, Project: "snake", Entity: "/home/mia/snake/audio.ts", Kind: "file"},
	}
	got := EntitySeconds(beats)
	if got["/home/mia/snake/game.ts"] != 120 {
		t.Fatalf("game.ts = %v, want 120", got["/home/mia/snake/game.ts"])
	}
	if got["/home/mia/snake/audio.ts"] != 240 {
		t.Fatalf("audio.ts = %v, want 240 (60s + capped 120s re-entry + 60s)",
			got["/home/mia/snake/audio.ts"])
	}
}

func TestEntitySecondsMatchesSpanTotalsOnSparseBeats(t *testing.T) {
	// Beats spaced over the 120s cap: Hackatime credits each gap at the cap, so
	// sparse trackers still accrue real time. File totals must keep pace with the
	// span totals or the review screen shows less file time than hours logged.
	beats := []Heartbeat{
		{Time: 1000, Project: "p", Entity: "a.go", Kind: "file"},
		{Time: 1300, Project: "p", Entity: "a.go", Kind: "file"},
		{Time: 1600, Project: "p", Entity: "b.go", Kind: "file"},
	}
	got := EntitySeconds(beats)
	if got["a.go"] != 120 || got["b.go"] != 120 {
		t.Fatalf("each session re-entry must carry the capped 120s: %+v", got)
	}
	spanTotal := totalOf(SpansFromHeartbeats(beats))
	if got["a.go"]+got["b.go"] != spanTotal {
		t.Fatalf("file seconds %v must sum to the span total %v", got["a.go"]+got["b.go"], spanTotal)
	}
}

// closeTo absorbs the one-ulp gap between the runtime's per-operation rounding
// and the test's exact constant arithmetic: 1/3 is not representable, so the
// same formula can differ in the last bit depending on where it is evaluated.
func closeTo(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}

func TestEntitySecondsReentryCreditFollowsTheOpenersCategory(t *testing.T) {
	got := EntitySeconds([]Heartbeat{
		{Time: 1000, Project: "p", Entity: "a.go", Kind: "file", Category: "coding"},
		{Time: 2000, Project: "p", Entity: "b.go", Kind: "file", Category: "ai coding"}, // opens a session: capped 120s at one third
	})
	if !closeTo(got["b.go"], 120*(1.0/3)) {
		t.Fatalf("b.go = %v, want the ai re-entry credit at one third", got["b.go"])
	}
	if got["a.go"] != 0 {
		t.Fatalf("a.go = %v, the break belongs to the beat that ends it", got["a.go"])
	}
}

func TestEntitySecondsIgnoresNonFileHeartbeats(t *testing.T) {
	beats := []Heartbeat{
		{Time: 1000, Project: "p", Entity: "Google Chrome", Kind: "app"},
		{Time: 1060, Project: "p", Entity: "", Kind: "file"},
		{Time: 1120, Project: "p", Entity: "/home/mia/p/main.go", Kind: "file"},
		{Time: 1180, Project: "p", Entity: "/home/mia/p/main.go", Kind: "file"},
	}
	got := EntitySeconds(beats)
	if len(got) != 1 || got["/home/mia/p/main.go"] != 60 {
		t.Fatalf("only file heartbeats with a path may be credited: %+v", got)
	}
}

func TestEntitySecondsSortsUnorderedInput(t *testing.T) {
	got := EntitySeconds([]Heartbeat{
		{Time: 1120, Project: "p", Entity: "a.go", Kind: "file"},
		{Time: 1000, Project: "p", Entity: "a.go", Kind: "file"},
		{Time: 1060, Project: "p", Entity: "a.go", Kind: "file"},
	})
	if got["a.go"] != 120 {
		t.Fatalf("a.go = %v, want 120 from two 60s gaps after sorting", got["a.go"])
	}
}

// Each gap belongs to the category of the beat that opened it, and the re-entry
// credit follows the beat that opens the session.
func TestSpansAttributeAiSecondsPerGap(t *testing.T) {
	spans := SpansFromHeartbeats([]Heartbeat{
		{Time: 1000, Project: "app", Category: "coding"},
		{Time: 1060, Project: "app", Category: "ai coding"}, // 60s gap opened by coding
		{Time: 1120, Project: "app", Category: "ai coding"}, // 60s gap opened by ai
		{Time: 1180, Project: "app", Category: "coding"},    // 60s gap opened by ai
		{Time: 2000, Project: "app", Category: "AI_Coding"}, // new session, ai opens it: cap is ai
		{Time: 2060, Project: "app", Category: "coding"},    // 60s gap opened by ai
	})
	if len(spans) != 2 {
		t.Fatalf("spans: %+v", spans)
	}
	if spans[0].Duration != 180 || spans[0].AiSeconds != 120 {
		t.Fatalf("first session: 60s coding + 120s ai, got %+v", spans[0])
	}
	if spans[1].Duration != 60+120 || spans[1].AiSeconds != 60+120 {
		t.Fatalf("second session: the 120s re-entry credit follows its ai opener, got %+v", spans[1])
	}
}

func TestDiscountAiCodingCreditsAtOneThird(t *testing.T) {
	spans, removed := DiscountAiCoding([]Span{
		{StartTime: 0, EndTime: 800, Duration: 800, AiSeconds: 800, Project: "app"}, // all ai
		{StartTime: 900, EndTime: 1300, Duration: 400, Project: "app"},              // no ai: untouched
	})
	cut := 800 * (1 - 1.0/3) // two thirds of the all-ai span
	if !closeTo(removed, cut) {
		t.Fatalf("removed = %v, want %v", removed, cut)
	}
	if !closeTo(spans[0].Duration, 800-cut) {
		t.Fatalf("an all-ai span keeps one third: %+v", spans[0])
	}
	if spans[0].StartTime != 0 || spans[0].EndTime != 800 {
		t.Fatalf("wall-clock bounds must survive the discount: %+v", spans[0])
	}
	if spans[1].Duration != 400 {
		t.Fatalf("hand-written time is untouched: %+v", spans[1])
	}
}

func beatJson(time float64, project string) map[string]any {
	return map[string]any{"time": time, "project": project, "entity": "/home/mia/p/main.go", "type": "file", "category": "coding"}
}

func writeHeartbeatPage(w http.ResponseWriter, page []map[string]any, hasMore bool) {
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"heartbeats": page, "has_more": hasMore})
}

func TestFetchHeartbeatsWindowSendsEpochParamsAndTrims(t *testing.T) {
	var gotStart, gotEnd string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/admin/v1/user/heartbeats", func(w http.ResponseWriter, r *http.Request) {
		gotStart, gotEnd = r.URL.Query().Get("start_date"), r.URL.Query().Get("end_date")
		writeHeartbeatPage(w, []map[string]any{
			beatJson(1000.2, "app"), // inside the widened integer window, before the real start
			beatJson(1500, "app"),
			beatJson(2000.5, "app"), // at/after the real end
		}, false)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &Client{BaseUrl: srv.URL, AdminKey: "test"}
	ok, beats := c.FetchHeartbeatsWindow(context.Background(), "u1", 1000.5, 2000.25)
	if !ok {
		t.Fatal("window fetch failed")
	}
	if gotStart != "1000" || gotEnd != "2001" {
		t.Fatalf("window must widen to enclosing whole seconds: start=%q end=%q", gotStart, gotEnd)
	}
	if len(beats) != 1 || beats[0].Time != 1500 {
		t.Fatalf("beats outside the exact half-open window must be trimmed: %+v", beats)
	}
}

func TestFetchHeartbeatsWindowOverlapsPagesAndDedupes(t *testing.T) {
	var all []map[string]any
	for i := range 250 {
		all = append(all, beatJson(float64(1000+i), "app"))
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/admin/v1/user/heartbeats", func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		end := min(offset+150, len(all)) // pages smaller than the asked limit, big enough to overlap
		writeHeartbeatPage(w, all[offset:end], end < len(all))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &Client{BaseUrl: srv.URL, AdminKey: "test"}
	ok, beats := c.FetchHeartbeatsWindow(context.Background(), "u1", 0, 5000)
	if !ok {
		t.Fatal("window fetch failed")
	}
	if len(beats) != 250 {
		t.Fatalf("overlapped pages must dedupe back to the unique beats: got %d, want 250", len(beats))
	}
}

func TestFetchProjectSpansWindowCreditsCaseVariants(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/admin/v1/user/heartbeats", func(w http.ResponseWriter, r *http.Request) {
		writeHeartbeatPage(w, []map[string]any{beatJson(1000, "Game"), beatJson(1060, "Game")}, false)
	})
	mux.HandleFunc("/api/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Write([]byte(`{"projects":[{"name":"game","total_seconds":0}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &Client{BaseUrl: srv.URL, AdminKey: "test"}
	res := c.FetchProjectSpansWindow(context.Background(), "u1", []string{"game"}, 0, 5000, nil)
	if !res.OK || res.Source != "heartbeats" {
		t.Fatalf("result: %+v", res)
	}
	if totalOf(res.Spans) != 60 {
		t.Fatalf("a case variant of a declared project must be credited: %+v", res.Spans)
	}
	if !slices.Contains(res.Notes, "hackatime_case_variants_credited:Game") {
		t.Fatalf("variant spellings must be surfaced as a note: %v", res.Notes)
	}
}

func TestFetchProjectSpansWindowTreatsDeniedOracleAsAuthoritative(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/admin/v1/user/heartbeats", func(w http.ResponseWriter, r *http.Request) {
		writeHeartbeatPage(w, nil, false)
	})
	mux.HandleFunc("/api/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"user has disabled public stats"}`, http.StatusForbidden)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &Client{BaseUrl: srv.URL, AdminKey: "test"}
	res := c.FetchProjectSpansWindow(context.Background(), "u1", []string{"app"}, 0, 5000, nil)
	if !res.OK || res.Source != "heartbeats" || len(res.Spans) != 0 {
		t.Fatalf("a 403 oracle must not wedge an authoritative zero from the admin walk: %+v", res)
	}
	if !slices.Contains(res.Notes, "hackatime_oracle_denied") {
		t.Fatalf("the denied oracle must be surfaced as a note: %v", res.Notes)
	}
}

func TestEntitySecondsDiscountsAiGaps(t *testing.T) {
	got := EntitySeconds([]Heartbeat{
		{Time: 1000, Project: "p", Entity: "a.go", Kind: "file", Category: "ai coding"},
		{Time: 1080, Project: "p", Entity: "a.go", Kind: "file", Category: "coding"},
		{Time: 1160, Project: "p", Entity: "a.go", Kind: "file", Category: "coding"},
	})
	if !closeTo(got["a.go"], 80*(1.0/3)+80) {
		t.Fatalf("a.go = %v, want the ai gap at one third plus the full coding gap", got["a.go"])
	}
}

func lapseBeatJson(time float64, project, clipName, clipId string) map[string]any {
	return map[string]any{"time": time, "project": project, "entity": clipName + " (" + clipId + ")", "type": "timelapse", "category": "timelapsing"}
}

// Lapse syncs a published clip's snapshots into Hackatime, and the clip is paid on
// its own lapse row. A clip the ship credits must not be paid a second time through
// its synced beats; a clip the ship does not credit keeps its Hackatime time.
func TestFetchProjectSpansWindowLeavesOutCreditedLapseBeats(t *testing.T) {
	var page []map[string]any
	page = append(page, beatJson(1000, "game"), beatJson(1060, "game"))
	for at := 2000.0; at <= 2600; at += 60 {
		page = append(page, lapseBeatJson(at, "game", "clip one", "clip1"))
	}
	page = append(page, lapseBeatJson(4000, "game", "clip two", "clip2"), lapseBeatJson(4060, "game", "clip two", "clip2"))
	mux := http.NewServeMux()
	mux.HandleFunc("/api/admin/v1/user/heartbeats", func(w http.ResponseWriter, r *http.Request) {
		writeHeartbeatPage(w, page, false)
	})
	mux.HandleFunc("/api/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.Write([]byte(`{"projects":[{"name":"game","total_seconds":960}]}`)) // 60 + (600+120) + (60+120): what Hackatime reports
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := &Client{BaseUrl: srv.URL, AdminKey: "test"}
	res := c.FetchProjectSpansWindow(context.Background(), "u1", []string{"game"}, 0, 5000, map[string]bool{"clip1": true})
	if !res.OK || res.Source != "heartbeats" {
		t.Fatalf("the oracle still counts the synced beats, so the capture must stay trusted: %+v", res)
	}
	if got := totalOf(res.Spans); got != 240 {
		t.Fatalf("credited = %v, want 240: 60s of coding plus the 180s of the clip the ship does not credit", got)
	}
	if res.LapseExcludedSeconds != 720 {
		t.Fatalf("LapseExcludedSeconds = %v, want the 720s the credited clip added to Hackatime", res.LapseExcludedSeconds)
	}

	all := c.FetchProjectSpansWindow(context.Background(), "u1", []string{"game"}, 0, 5000, nil)
	if totalOf(all.Spans) != 960 || all.LapseExcludedSeconds != 0 {
		t.Fatalf("with no credited clips every beat counts: %+v", all)
	}
}

func TestLapseClipIdOfOnlyMatchesTimelapseBeats(t *testing.T) {
	cases := []struct {
		beat Heartbeat
		want string
	}{
		{Heartbeat{Entity: "clip one (clip1)", Kind: "timelapse", Category: "timelapsing"}, "clip1"},
		{Heartbeat{Entity: "clip one (clip1)", Category: "timelapsing"}, "clip1"},
		{Heartbeat{Entity: "/home/user1/project12/notes (draft).md", Kind: "file", Category: "coding"}, ""},
		{Heartbeat{Entity: "clip without id", Kind: "timelapse"}, ""},
	}
	for _, tc := range cases {
		if got := lapseClipIdOf(tc.beat); got != tc.want {
			t.Fatalf("lapseClipIdOf(%+v) = %q, want %q", tc.beat, got, tc.want)
		}
	}
}
