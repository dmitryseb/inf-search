package invertedindex_dates

import (
	"math"
	"strings"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/bbalet/stopwords"
	"github.com/kljensen/snowball"
	"sampleGoProject/lsm"
	"time"
)

type InvertedIndex struct {
	tree       *lsm.LSM
	docs       map[uint32]DocDates
	startSlice *bitSlicedOrdinal
	endSlice   *bitSlicedOrdinal
	openEnded  *roaring.Bitmap
}

func NewInvertedIndex() *InvertedIndex {
	return NewInvertedIndexWithLSM(1024, "invertedindex_dates_lsmdata")
}

func NewInvertedIndexWithLSM(maxSize int, dir string) *InvertedIndex {
	return &InvertedIndex{
		tree:       lsm.InitWithDir(maxSize, dir),
		docs:       make(map[uint32]DocDates),
		startSlice: newBitSlicedOrdinal(),
		endSlice:   newBitSlicedOrdinal(),
		openEnded:  roaring.New(),
	}
}

func (idx *InvertedIndex) AddDocument(docID int, text string, validStart time.Time, validEnd *time.Time) {
	if docID < 0 || docID > math.MaxUint32 {
		return
	}
	id := uint32(docID)
	meta := DocDates{ValidStart: validStart, ValidEnd: validEnd}
	idx.docs[id] = meta
	idx.addDocToDateIndexes(id, meta)

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


func (idx *InvertedIndex) addDocToDateIndexes(id uint32, d DocDates) {
	idx.startSlice.addDoc(id, ordinalDayUTC(d.ValidStart))
	if d.ValidEnd != nil {
		idx.endSlice.addDoc(id, ordinalDayUTC(*d.ValidEnd))
	} else {
		idx.openEnded.Add(id)
	}
}

func (idx *InvertedIndex) bitmapAppearedInRange(from, to time.Time) *roaring.Bitmap {
	lo, hi := ordinalDayUTC(from), ordinalDayUTC(to)
	if lo > hi {
		lo, hi = hi, lo
	}
	return idx.startSlice.rangeBitmap(lo, hi)
}

func (idx *InvertedIndex) bitmapValidInRange(from, to time.Time) *roaring.Bitmap {
	qf, qt := ordinalDayUTC(from), ordinalDayUTC(to)
	if qf > qt {
		qf, qt = qt, qf
	}
	startOK := idx.startSlice.rangeBitmap(0, qt)
	endOK := idx.openEnded.Clone()
	endOK.Or(idx.endSlice.rangeBitmap(qf, farEndOrdinal))
	startOK.And(endOK)
	return startOK
}
