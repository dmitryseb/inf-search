package lsm

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func BenchmarkLSMInsertSizes(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 1_000_000}
	trials := 5

	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.StopTimer()
			ops := make([]float64, 0, trials)

			for t := 0; t < trials; t++ {
				l := Init(1000)
				start := time.Now()
				for i := 0; i < n; i++ {
					v := "v"
					l.Put(fmt.Sprintf("%d", i), &v)
				}
				elapsed := time.Since(start)

				if elapsed > 0 {
					ops = append(ops, float64(n)/elapsed.Seconds())
				}
			}

			if len(ops) == 0 {
				return
			}
			sort.Float64s(ops)
			sum := 0.0
			for _, v := range ops {
				sum += v
			}
			avg := sum / float64(len(ops))
			med := ops[len(ops)/2]
			max := ops[len(ops)-1]

			b.ReportMetric(avg, "ops/s_avg")
			b.ReportMetric(med, "ops/s_med")
			b.ReportMetric(max, "ops/s_max")
		})
	}
}

func BenchmarkLSMReadAfterInsertSizes(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 1_000_000}
	trials := 5

	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.StopTimer()
			ops := make([]float64, 0, trials)

			for t := 0; t < trials; t++ {
				l := Init(1 << 30)
				for i := 0; i < n; i++ {
					v := "v"
					l.Put(fmt.Sprintf("%d", i), &v)
				}

				start := time.Now()
				for i := 0; i < n; i++ {
					_ = l.Get(fmt.Sprintf("%d", i))
				}
				elapsed := time.Since(start)

				if elapsed > 0 {
					ops = append(ops, float64(n)/elapsed.Seconds())
				}
			}

			if len(ops) == 0 {
				return
			}
			sort.Float64s(ops)
			sum := 0.0
			for _, v := range ops {
				sum += v
			}
			avg := sum / float64(len(ops))
			med := ops[len(ops)/2]
			max := ops[len(ops)-1]

			b.ReportMetric(avg, "ops/s_avg")
			b.ReportMetric(med, "ops/s_med")
			b.ReportMetric(max, "ops/s_max")
		})
	}
}
