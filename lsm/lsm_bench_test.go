package lsm

import (
	"fmt"
	"testing"
)

func BenchmarkLSMPut(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 1_000_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("%d", i)
			}
			v := "v"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := Init(1000)
				for _, k := range keys {
					l.Put(k, &v)
				}
			}
		})
	}
}

func BenchmarkLSMGet(b *testing.B) {
	sizes := []int{10, 100, 1_000, 10_000, 100_000, 1_000_000}

	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			keys := make([]string, n)
			for i := 0; i < n; i++ {
				keys[i] = fmt.Sprintf("%d", i)
			}
			v := "v"
			l := Init(1000)
			for _, k := range keys {
				l.Put(k, &v)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = l.Get(keys[i%len(keys)])
			}
		})
	}
}
