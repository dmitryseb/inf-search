package invertedindex_dates

import (
	"reflect"
	"testing"
	"time"
)

func d(y int, m time.Month, day int) time.Time {
	return time.Date(y, m, day, 12, 0, 0, 0, time.UTC)
}

func ptr(t time.Time) *time.Time { return &t }

func TestInvertedIndexDates(t *testing.T) {
	idx := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx.AddDocument(1, "running fast with maps", d(2020, 1, 10), nil)
	idx.AddDocument(2, "run bloom filter", d(2021, 6, 15), nil)
	idx.AddDocument(3, "roaring bitmap index bloom", d(2020, 3, 1), nil)

	for _, tc := range []struct {
		query string
		want  []int
	}{
		{"[2020-01-01,2020-12-31]", []int{1, 3}},
		{"DATE[2020-01-01,2020-12-31]", []int{1, 3}},
		{"run AND [2020-01-01,2020-12-31]", []int{1}},
		{"bitmap OR [2021-01-01,2021-12-31]", []int{2, 3}},
	} {
		got, err := idx.Search(tc.query)
		if err != nil {
			t.Fatalf("Search(%q): %v", tc.query, err)
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("Search(%q) = %v, want %v", tc.query, got, tc.want)
		}
	}

	idx2 := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx2.AddDocument(1, "a", d(2022, 1, 1), nil)
	idx2.AddDocument(2, "b", d(2023, 1, 1), nil)
	got := idx2.SearchDateInRange(d(2022, 1, 1), d(2022, 12, 31))
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("SearchDateInRange = %v, want [1]", got)
	}

	idx3 := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx3.AddDocument(1, "alpha", d(2020, 1, 1), ptr(d(2020, 6, 30)))
	idx3.AddDocument(2, "beta", d(2020, 5, 1), nil)
	idx3.AddDocument(3, "gamma", d(2020, 3, 1), ptr(d(2020, 3, 31)))
	if g := idx3.SearchValidInRange(d(2020, 7, 1), d(2020, 7, 31)); !reflect.DeepEqual(g, []int{2}) {
		t.Fatalf("SearchValidInRange july = %v, want [2]", g)
	}
	if g := idx3.SearchValidInRange(d(2020, 3, 1), d(2020, 3, 31)); !reflect.DeepEqual(g, []int{1, 3}) {
		t.Fatalf("SearchValidInRange march = %v, want [1 3]", g)
	}

	idx4 := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx4.AddDocument(1, "a", d(2019, 1, 1), ptr(d(2020, 1, 1)))
	idx4.AddDocument(2, "b", d(2020, 6, 1), nil)
	idx4.AddDocument(3, "c", d(2021, 1, 1), nil)
	if g := idx4.SearchAppearedInRange(d(2020, 1, 1), d(2020, 12, 31)); !reflect.DeepEqual(g, []int{2}) {
		t.Fatalf("SearchAppearedInRange = %v, want [2]", g)
	}

	idx5 := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx5.AddDocument(1, "cat dog", d(2020, 1, 1), ptr(d(2020, 12, 31)))
	idx5.AddDocument(2, "cat dog", d(2021, 1, 1), ptr(d(2021, 6, 30)))
	idx5.AddDocument(3, "dog fish", d(2020, 6, 1), nil)
	got, err := idx5.Search("cat AND VALID[2020-06-01,2020-06-30]")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("VALID query = %v, want [1]", got)
	}
	got, err = idx5.Search("dog AND APPEARED[2021-01-01,2021-12-31]")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("APPEARED query = %v, want [2]", got)
	}

	idx6 := NewInvertedIndexWithLSM(1024, t.TempDir())
	idx6.AddDocument(1, "running fast with maps", d(2000, 1, 1), nil)
	idx6.AddDocument(2, "run bloom filter", d(2000, 1, 1), nil)
	idx6.AddDocument(3, "roaring bitmap index bloom", d(2000, 1, 1), nil)
	idx6.AddDocument(4, "maps bitmap", d(2000, 1, 1), nil)
	got, err = idx6.Search("run AND map")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []int{1}) {
		t.Fatalf("plain boolean = %v, want [1]", got)
	}
}
