package invertedindex_positional

import (
	"bytes"
	"encoding/gob"
	"math"
	"strings"

	"github.com/bbalet/stopwords"
	"github.com/kljensen/snowball"
	"sampleGoProject/lsm"
)

type InvertedIndex struct {
	tree *lsm.LSM
}

type posting map[uint32][]uint32

func NewInvertedIndex() *InvertedIndex {
	return NewInvertedIndexWithLSM(1024, "invertedindex_positional_lsmdata")
}

func NewInvertedIndexWithLSM(maxSize int, dir string) *InvertedIndex {
	return &InvertedIndex{
		tree: lsm.InitWithDir(maxSize, dir),
	}
}

func (idx *InvertedIndex) AddDocument(docID int, text string) {
	if docID < 0 || docID > math.MaxUint32 {
		return
	}
	id := uint32(docID)

	tokens := idx.normalizeAndTokenize(text)
	termPositions := make(map[string][]uint32, len(tokens))
	for pos, token := range tokens {
		termPositions[token] = append(termPositions[token], uint32(pos))
	}

	for term, positions := range termPositions {
		p := idx.loadPosting(term)
		p[id] = positions
		idx.storePosting(term, p)
	}
}

func (idx *InvertedIndex) Compact() error {
	if idx.tree == nil {
		return nil
	}
	return idx.tree.Compact()
}

func (idx *InvertedIndex) normalizeAndTokenize(text string) []string {
	if text == "" {
		return nil
	}

	cleaned := stopwords.CleanString(text, "en", false)
	parts := strings.Fields(strings.ToLower(cleaned))
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		token := strings.TrimSpace(p)
		if token == "" {
			continue
		}
		stemmed, err := snowball.Stem(token, "english", true)
		if err != nil || stemmed == "" {
			out = append(out, token)
			continue
		}
		out = append(out, stemmed)
	}
	return out
}

func (idx *InvertedIndex) normalizeWord(word string) string {
	tokens := idx.normalizeAndTokenize(word)
	if len(tokens) == 0 {
		return ""
	}
	return tokens[0]
}

func (idx *InvertedIndex) loadPosting(term string) posting {
	if idx.tree == nil {
		return make(posting)
	}
	raw := idx.tree.Get(term)
	if raw == nil {
		return make(posting)
	}
	p := make(posting)
	if err := gob.NewDecoder(bytes.NewReader([]byte(*raw))).Decode(&p); err != nil {
		return make(posting)
	}
	return p
}

func (idx *InvertedIndex) storePosting(term string, p posting) {
	if idx.tree == nil || p == nil {
		return
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		return
	}
	s := buf.String()
	idx.tree.Put(term, &s)
}
