package invertedindex_positional

import (
	"fmt"
	"sort"
)

func (idx *InvertedIndex) SearchPhrase(phrase string) ([]int, error) {
	terms := idx.normalizeAndTokenize(phrase)
	if len(terms) == 0 {
		return nil, fmt.Errorf("empty phrase")
	}

	postings := make([]posting, len(terms))
	for i, t := range terms {
		postings[i] = idx.loadPosting(t)
		if len(postings[i]) == 0 {
			return nil, nil
		}
	}

	smallest := 0
	for i := 1; i < len(postings); i++ {
		if len(postings[i]) < len(postings[smallest]) {
			smallest = i
		}
	}

	candidates := make([]uint32, 0, len(postings[smallest]))
	for id := range postings[smallest] {
		inAll := true
		for i, p := range postings {
			if i == smallest {
				continue
			}
			if _, ok := p[id]; !ok {
				inAll = false
				break
			}
		}
		if inAll {
			candidates = append(candidates, id)
		}
	}

	result := make([]int, 0, len(candidates))
	for _, id := range candidates {
		if phraseMatchesInDoc(postings, id) {
			result = append(result, int(id))
		}
	}
	sort.Ints(result)
	return result, nil
}

func phraseMatchesInDoc(postings []posting, docID uint32) bool {
	if len(postings) == 0 {
		return false
	}
	if len(postings) == 1 {
		return len(postings[0][docID]) > 0
	}

	first := postings[0][docID]
	if len(first) == 0 {
		return false
	}

	sets := make([]map[uint32]struct{}, len(postings))
	for i := 1; i < len(postings); i++ {
		positions := postings[i][docID]
		m := make(map[uint32]struct{}, len(positions))
		for _, p := range positions {
			m[p] = struct{}{}
		}
		sets[i] = m
	}

	for _, p0 := range first {
		match := true
		for i := 1; i < len(postings); i++ {
			if _, ok := sets[i][p0+uint32(i)]; !ok {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
