package invertedindex

import (
	"reflect"
	"testing"
)

func TestInvertedIndexGrams(t *testing.T) {
	idx := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx.AddDocument(1, "running fast with maps")
	idx.AddDocument(2, "run bloom filter")
	idx.AddDocument(3, "roaring bitmap index bloom")
	idx.AddDocument(4, "maps bitmap")

	for _, tc := range []struct {
		query string
		want  []int
	}{
		{"run AND map", []int{1}},
		{"run OR bitmap", []int{1, 2, 3, 4}},
		{"(run OR bloom) AND bitmap", []int{3}},
	} {
		got, err := idx.Search(tc.query)
		if err != nil {
			t.Fatalf("Search(%q): %v", tc.query, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("Search(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}

	got, err := idx.SearchPrefix("ru")
	if err != nil {
		t.Fatalf("SearchPrefix: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("SearchPrefix(ru) = %v, want [1 2]", got)
	}

	widx := NewInvertedIndexWithLSM(1024, t.TempDir())
	widx.AddDocument(1, "running fast")
	widx.AddDocument(2, "runner slow")
	widx.AddDocument(3, "bloom bitmap")
	got, err = widx.SearchWildcard("run*")
	if err != nil {
		t.Fatalf("SearchWildcard: %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("SearchWildcard(run*) = %v, want [1 2]", got)
	}
	got, err = widx.SearchWildcard("*oom")
	if err != nil {
		t.Fatalf("SearchWildcard: %v", err)
	}
	if !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("SearchWildcard(*oom) = %v, want [3]", got)
	}

	small := NewInvertedIndexWithLSM(2, t.TempDir())
	small.AddDocument(1, "running map")
	small.AddDocument(2, "run bloom")
	small.AddDocument(3, "map bloom")
	if err := small.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got, err = small.Search("run AND bloom")
	if err != nil {
		t.Fatalf("Search after Compact: %v", err)
	}
	if !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("after Compact = %v, want [2]", got)
	}
}
