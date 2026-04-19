package invertedindex_positional

import (
	"reflect"
	"testing"
)

func TestPhraseSearch(t *testing.T) {
	idx := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx.AddDocument(1, "running fast with maps")
	idx.AddDocument(2, "fast running with maps")
	idx.AddDocument(3, "running quickly with maps")
	idx.AddDocument(4, "bloom filter bitmap index")
	idx.AddDocument(5, "bitmap index with bloom filter")

	for _, tc := range []struct {
		phrase string
		want   []int
	}{
		{"running fast", []int{1}},
		{"fast running", []int{2}},
		{"running maps", []int{2}},
		{"bloom filter", []int{4, 5}},
		{"bitmap index", []int{4, 5}},
		{"index bloom", []int{5}},
		{"maps", []int{1, 2, 3}},
		{"unknown phrase", nil},
	} {
		got, err := idx.SearchPhrase(tc.phrase)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("SearchPhrase(%q) = %v, want %v", tc.phrase, got, tc.want)
		}
	}
}

func TestPhraseRepeatedTerms(t *testing.T) {
	idx := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx.AddDocument(1, "run run bloom")
	idx.AddDocument(2, "run bloom bloom")
	idx.AddDocument(3, "run bloom run bloom")

	got, err := idx.SearchPhrase("run run")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("run run = %v, want [1]", got)
	}

	got, err = idx.SearchPhrase("run bloom")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("run bloom = %v, want [1 2 3]", got)
	}

	got, err = idx.SearchPhrase("bloom run")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("bloom run = %v, want [3]", got)
	}
}

func TestPhraseAfterCompact(t *testing.T) {
	idx := NewInvertedIndexWithLSM(2, t.TempDir())
	idx.AddDocument(1, "running fast maps")
	idx.AddDocument(2, "maps fast running")
	idx.AddDocument(3, "bitmap bloom index")
	if err := idx.Compact(); err != nil {
		t.Fatal(err)
	}

	got, err := idx.SearchPhrase("running fast")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("after Compact running fast = %v, want [1]", got)
	}

	got, err = idx.SearchPhrase("bitmap bloom")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("after Compact bitmap bloom = %v, want [3]", got)
	}
}

func TestEmptyPhrase(t *testing.T) {
	idx := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx.AddDocument(1, "run bloom")
	if _, err := idx.SearchPhrase(""); err == nil {
		t.Fatalf("expected error for empty phrase")
	}
	if _, err := idx.SearchPhrase("   "); err == nil {
		t.Fatalf("expected error for whitespace phrase")
	}
}
