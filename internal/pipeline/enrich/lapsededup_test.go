package enrich_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hackclub/ari-webhooks/internal/integrations/hackatime"
	"github.com/hackclub/ari-webhooks/internal/integrations/lapse"
)

// Lapse syncs a clip's snapshots into Hackatime when it is published, and the clip
// is credited on its own lapse row. When the snapshots do not sit inside the clip's
// [createdAt, createdAt+duration) window (a paused recording, a session opened early),
// the window subtraction misses them, so the synced beats themselves must be left out
// or the recording is paid twice.
func TestEnrichDoesNotPayACreditedClipTwice(t *testing.T) {
	repoUrl := serveRepoWith(t, map[string]string{"src/game.ts": filler("game")})
	f := setupEnrich(t, repoUrl, []string{"snake"})
	ctx := context.Background()

	fourDaysAgo := float64(time.Now().Add(-4 * 24 * time.Hour).Unix())
	beats := runOn("/home/user1/snake/src/game.ts", "", fourDaysAgo, 10) // 9 gaps x 120s = 18m of coding
	beats = append(beats, syncedSnapshots(fourDaysAgo+30000, 16)...)     // well after the clip's createdAt window
	f.pipeline.Hackatime = &hackatime.Client{BaseUrl: serveHackatime(t, beats), AdminKey: "test"}

	f.pipeline.Lapse = &lapse.Client{BaseUrl: serveOneClip(t, fourDaysAgo+20000, ""), ApiKey: "test"}

	if _, err := f.pool.Exec(ctx,
		`update "Maker" set "hackatimeUserId" = 'ht-1' where email = 'mia@example.com'`); err != nil {
		t.Fatal(err)
	}

	result, err := f.pipeline.Enrich(ctx, f.subId)
	if err != nil || !result.OK {
		t.Fatalf("capture must be healthy: ok=%v err=%v notes=%v", result.OK, err, result.Notes)
	}

	var hackatimeMinutes, lapseMinutes int
	if err := f.pool.QueryRow(ctx,
		`select "hackatimeMinutes", "lapseMinutes" from "HoursBreakdown" where "submissionId" = $1`,
		f.subId).Scan(&hackatimeMinutes, &lapseMinutes); err != nil {
		t.Fatal(err)
	}
	if hackatimeMinutes != 18 {
		t.Fatalf("hackatimeMinutes = %d, want 18: the 32m of synced snapshots belong to the clip", hackatimeMinutes)
	}
	if lapseMinutes != 30 {
		t.Fatalf("lapseMinutes = %d, want the clip's 30 minutes", lapseMinutes)
	}
	found := false
	for _, note := range result.Notes {
		if note == "lapse_heartbeats_excluded_32m" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the left-out time must leave a note: %v", result.Notes)
	}
}

// Coding while recording is one stretch of wall-clock time, so it is deduped against
// when the synced beats say the recording ran, not against the createdAt window: real
// coding that only happens to sit in that window keeps its credit.
func TestEnrichDedupesEditorTimeAgainstTheRealRecording(t *testing.T) {
	repoUrl := serveRepoWith(t, map[string]string{"src/game.ts": filler("game")})
	f := setupEnrich(t, repoUrl, []string{"snake"})
	ctx := context.Background()

	fourDaysAgo := float64(time.Now().Add(-4 * 24 * time.Hour).Unix())
	beats := runOn("/home/user1/snake/src/game.ts", "", fourDaysAgo+20000, 10) // 18m inside the createdAt window, nothing recorded then
	beats = append(beats, syncedSnapshots(fourDaysAgo+30000, 16)...)
	// 5 beats while recording: 480s of overlap plus the 120s re-entry credit, which sits outside it
	beats = append(beats, runOn("/home/user1/snake/src/game.ts", "", fourDaysAgo+30060, 5)...)
	f.pipeline.Hackatime = &hackatime.Client{BaseUrl: serveHackatime(t, beats), AdminKey: "test"}
	f.pipeline.Lapse = &lapse.Client{BaseUrl: serveOneClip(t, fourDaysAgo+20000, ""), ApiKey: "test"}
	if _, err := f.pool.Exec(ctx,
		`update "Maker" set "hackatimeUserId" = 'ht-1' where email = 'mia@example.com'`); err != nil {
		t.Fatal(err)
	}

	result, err := f.pipeline.Enrich(ctx, f.subId)
	if err != nil || !result.OK {
		t.Fatalf("capture must be healthy: ok=%v err=%v notes=%v", result.OK, err, result.Notes)
	}
	if result.HackatimeMinutes != 20 {
		t.Fatalf("hackatimeMinutes = %d, want 20: the 18m outside the recording plus the 2m re-entry credit", result.HackatimeMinutes)
	}
}

// A Lapse fetch that fails for one project leaves no clip rows written, so no beats may
// leave Hackatime either, or the recording would be lost from both rows.
func TestEnrichKeepsSyncedBeatsWhenLapseIsUnhealthy(t *testing.T) {
	repoUrl := serveRepoWith(t, map[string]string{"src/game.ts": filler("game")})
	f := setupEnrich(t, repoUrl, []string{"snake", "project12"})
	ctx := context.Background()

	fourDaysAgo := float64(time.Now().Add(-4 * 24 * time.Hour).Unix())
	beats := runOn("/home/user1/snake/src/game.ts", "", fourDaysAgo, 10)
	beats = append(beats, syncedSnapshots(fourDaysAgo+30000, 16)...)
	f.pipeline.Hackatime = &hackatime.Client{BaseUrl: serveHackatime(t, beats), AdminKey: "test"}
	f.pipeline.Lapse = &lapse.Client{BaseUrl: serveOneClip(t, fourDaysAgo+20000, "project12"), ApiKey: "test"}
	if _, err := f.pool.Exec(ctx,
		`update "Maker" set "hackatimeUserId" = 'ht-1' where email = 'mia@example.com'`); err != nil {
		t.Fatal(err)
	}

	result, err := f.pipeline.Enrich(ctx, f.subId)
	if err != nil {
		t.Fatal(err)
	}
	for _, note := range result.Notes {
		if strings.HasPrefix(note, "lapse_heartbeats_excluded") {
			t.Fatalf("beats left Hackatime although the clips will not be written: %v", result.Notes)
		}
	}
	if result.HackatimeMinutes != 50 {
		t.Fatalf("hackatimeMinutes = %d, want all 50 minutes kept in Hackatime", result.HackatimeMinutes)
	}
}

// 16 snapshots two minutes apart for clip1: 15 gaps x 120s plus the 120s re-entry
// credit, 32m that Hackatime counts for the recording
func syncedSnapshots(startTs float64, count int) []stubBeat {
	var out []stubBeat
	for i := range count {
		out = append(out, stubBeat{
			Time:     startTs + float64(i)*hackatime.HeartbeatCapS,
			Project:  "snake",
			Entity:   "clip one (clip1)",
			Kind:     "timelapse",
			Category: "timelapsing",
		})
	}
	return out
}

// serveOneClip serves a 30m clip1 for every project except failingProject, which errors
func serveOneClip(t *testing.T, createdAtTs float64, failingProject string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failingProject != "" && r.URL.Query().Get("projectKey") == failingProject {
			http.Error(w, "unavailable", http.StatusBadGateway)
			return
		}
		if r.URL.Query().Get("projectKey") != "snake" {
			w.Header().Set("content-type", "application/json")
			w.Write([]byte(`[]`))
			return
		}
		w.Header().Set("content-type", "application/json")
		fmt.Fprintf(w, `[{"id": "clip1", "name": "clip one", "duration": 1800, "createdAt": %q, "playbackUrl": "https://lapse.test/clip1"}]`,
			time.Unix(int64(createdAtTs), 0).UTC().Format(time.RFC3339))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}
