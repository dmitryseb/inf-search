package invertedindex

import (
	"reflect"
	"testing"
)

func TestSearchBooleanQuery(t *testing.T) {
	idx := newTestIndex(t)
	idx.AddDocument(1, "running fast with maps")
	idx.AddDocument(2, "run bloom filter")
	idx.AddDocument(3, "roaring bitmap index bloom")
	idx.AddDocument(4, "maps bitmap")

	tests := []struct {
		name  string
		query string
		want  []int
	}{
		{name: "and", query: "run AND map", want: []int{1}},
		{name: "or", query: "run OR bitmap", want: []int{1, 2, 3, 4}},
		{name: "parentheses", query: "(run OR bloom) AND bitmap", want: []int{3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := idx.Search(tt.query)
			if err != nil {
				t.Fatalf("Search(%q) returned error: %v", tt.query, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Search(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestSearchBooleanQueryErrors(t *testing.T) {
	idx := newTestIndex(t)
	idx.AddDocument(1, "run map")

	tests := []string{
		"",
		"run OR",
		"(run OR map",
	}

	for _, q := range tests {
		t.Run(q, func(t *testing.T) {
			if _, err := idx.Search(q); err == nil {
				t.Fatalf("Search(%q) expected error, got nil", q)
			}
		})
	}
}

func TestSearchAfterCompact(t *testing.T) {
	idx := newTestIndex(t)
	idx.AddDocument(1, "running map")
	idx.AddDocument(2, "run bloom")
	idx.AddDocument(3, "map bloom")

	if err := idx.Compact(); err != nil {
		t.Fatalf("Compact() returned error: %v", err)
	}

	got, err := idx.Search("run AND bloom")
	if err != nil {
		t.Fatalf("Search() returned error: %v", err)
	}
	want := []int{2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Search() = %v, want %v", got, want)
	}
}

func TestSearchPrefix(t *testing.T) {
	idx := newTestIndex(t)
	idx.AddDocument(1, "runner running")
	idx.AddDocument(2, "run bloom")
	idx.AddDocument(3, "map bloom")

	got, err := idx.SearchPrefix("ru")
	if err != nil {
		t.Fatalf("SearchPrefix() returned error: %v", err)
	}
	want := []int{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchPrefix() = %v, want %v", got, want)
	}
}

func TestSearchWildcard(t *testing.T) {
	idx := newTestIndex(t)
	idx.AddDocument(1, "running fast")
	idx.AddDocument(2, "runner slow")
	idx.AddDocument(3, "bloom bitmap")

	got, err := idx.SearchWildcard("run*")
	if err != nil {
		t.Fatalf("SearchWildcard() returned error: %v", err)
	}
	want := []int{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchWildcard(run*) = %v, want %v", got, want)
	}

	got, err = idx.SearchWildcard("*oom")
	if err != nil {
		t.Fatalf("SearchWildcard() returned error: %v", err)
	}
	want = []int{3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SearchWildcard(*oom) = %v, want %v", got, want)
	}
}

func newTestIndex(t *testing.T) *InvertedIndex {
	t.Helper()
	return NewInvertedIndexWithLSM(2, t.TempDir())
}
