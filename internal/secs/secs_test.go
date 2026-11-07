package secs

import (
	"math"
	"reflect"
	"testing"
)

func TestSharedContractVector(t *testing.T) {
	sources := []int{5429, 1830, 29, 0}
	approvedSeconds := 0
	for _, s := range sources {
		approvedSeconds += s
	}
	if approvedSeconds != 7288 {
		t.Fatalf("approved_seconds = %d, want 7288", approvedSeconds)
	}
	if got := LegacyMinutes(approvedSeconds); got != 121 {
		t.Fatalf("approved_minutes = %d, want 121", got)
	}
	if got := LegacyHours(approvedSeconds); got != 2 {
		t.Fatalf("approved_hours = %v, want 2", got)
	}
	if got := SplitProportional(LegacyMinutes(approvedSeconds), sources); !reflect.DeepEqual(got, []int{90, 30, 1, 0}) {
		t.Fatalf("minutes_breakdown = %v, want [90 30 1 0]", got)
	}

	splits := []struct {
		total   int
		weights []int
		want    []int
	}{
		{100, []int{1, 1, 1}, []int{34, 33, 33}},
		{1, []int{1, 1}, []int{1, 0}},
		{7, []int{2, 5}, []int{2, 5}},
		{9, []int{0, 0}, []int{0, 0}},
		{0, []int{3, 4}, []int{0, 0}},
	}
	for _, c := range splits {
		if got := SplitProportional(c.total, c.weights); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("SplitProportional(%d, %v) = %v, want %v", c.total, c.weights, got, c.want)
		}
	}

	for seconds, want := range map[int]int{5400: 90, 5429: 90, 5430: 91, 29: 0, 30: 1} {
		if got := LegacyMinutes(seconds); got != want {
			t.Fatalf("LegacyMinutes(%d) = %d, want %d", seconds, got, want)
		}
	}
}

func TestPartsSumToTheRoundedTotal(t *testing.T) {
	total, parts := Parts([]float64{10.4, 10.4, 10.4, 0})
	if total != 31 || !reflect.DeepEqual(parts, []int{11, 10, 10, 0}) {
		t.Fatalf("Parts = %d %v, want 31 [11 10 10 0]", total, parts)
	}
	huge := SplitProportional(2_000_000_000, []int{9_000_000_000_000, 1})
	if huge[0]+huge[1] != 2_000_000_000 {
		t.Fatalf("large split lost units: %v", huge)
	}
}

func TestFromHours(t *testing.T) {
	cases := map[float64]int{1.5: 5400, 0: 0, -2: 0, 0.0001: 0, 0.00014: 1, 2.25: 8100}
	for hours, want := range cases {
		if got := FromHours(hours); got != want {
			t.Fatalf("FromHours(%v) = %d, want %d", hours, got, want)
		}
	}
	if FromHours(math.NaN()) != 0 || FromHours(math.Inf(1)) != 0 {
		t.Fatal("non-finite hours must be no time")
	}
}
