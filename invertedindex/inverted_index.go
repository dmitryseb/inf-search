package invertedindex

import (
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/bbalet/stopwords"
	"github.com/kljensen/snowball"
	"sampleGoProject/lsm"
)

type InvertedIndex struct {
	tree *lsm.LSM
}

func NewInvertedIndex() *InvertedIndex {
	return NewInvertedIndexWithLSM(1024, "invertedindex_lsmdata")
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

	for _, token := range idx.normalizeAndTokenize(text) {
		bm := idx.loadPosting(token)
		bm.Add(id)
		idx.storePosting(token, bm)
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
		if token != "" {
			stemmed, err := snowball.Stem(token, "english", true)
			if err != nil || stemmed == "" {
				out = append(out, token)
				continue
			}
			out = append(out, stemmed)
		}
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

func (idx *InvertedIndex) loadPosting(term string) *roaring.Bitmap {
	if idx.tree == nil {
		return roaring.New()
	}
	raw := idx.tree.Get(term)
	if raw == nil {
		return roaring.New()
	}
	bm := roaring.New()
	if _, err := bm.FromBuffer([]byte(*raw)); err != nil {
		return roaring.New()
	}
	return bm
}

func (idx *InvertedIndex) storePosting(term string, bm *roaring.Bitmap) {
	if idx.tree == nil || bm == nil {
		return
	}
	data, err := bm.ToBytes()
	if err != nil {
		return
	}
	s := string(data)
	idx.tree.Put(term, &s)
}
