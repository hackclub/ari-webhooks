package secs

import (
	"math"
	"math/big"
	"sort"
)

func LegacyMinutes(seconds int) int {
	return (seconds + 30) / 60
}

func LegacyHours(seconds int) float64 {
	return math.Floor(float64(LegacyMinutes(seconds))/60*10+0.5) / 10
}

// halves round up
func Round(seconds float64) int {
	return int(math.Floor(seconds + 0.5))
}

// parts sum to exactly total; leftovers go to the largest remainders, earlier index first
func SplitProportional(total int, weights []int) []int {
	parts := make([]int, len(weights))
	weightSum := new(big.Int)
	for _, w := range weights {
		if w > 0 {
			weightSum.Add(weightSum, big.NewInt(int64(w)))
		}
	}
	if total <= 0 || weightSum.Sign() == 0 {
		return parts
	}
	remainders := make([]*big.Int, len(weights))
	order := make([]int, len(weights))
	leftover := total
	for i, w := range weights {
		order[i] = i
		remainders[i] = new(big.Int)
		if w <= 0 {
			continue
		}
		product := new(big.Int).Mul(big.NewInt(int64(total)), big.NewInt(int64(w)))
		quotient, remainder := new(big.Int).QuoRem(product, weightSum, new(big.Int))
		parts[i] = int(quotient.Int64())
		remainders[i] = remainder
		leftover -= parts[i]
	}
	sort.SliceStable(order, func(a, b int) bool {
		return remainders[order[a]].Cmp(remainders[order[b]]) > 0
	})
	for _, i := range order[:leftover] {
		parts[i]++
	}
	return parts
}

// float weights are ranked in milliseconds
func SplitFloat(total int, weights []float64) []int {
	scaled := make([]int, len(weights))
	for i, w := range weights {
		scaled[i] = max(Round(w*1000), 0)
	}
	return SplitProportional(total, scaled)
}

// the float sum is rounded once and every part derives from that total
func Parts(weights []float64) (int, []int) {
	sum := 0.0
	for _, w := range weights {
		sum += max(w, 0)
	}
	total := Round(sum)
	return total, SplitFloat(total, weights)
}

func FromHours(hours float64) int {
	if math.IsNaN(hours) || math.IsInf(hours, 0) || hours <= 0 {
		return 0
	}
	return Round(hours * 3600) // 3600: seconds in an hour
}
