package invertedindex_dates

import (
	"github.com/RoaringBitmap/roaring/v2"
)

const dateOrdinalBits = 31

type bitSlicedOrdinal struct {
	maxBit   int
	universe *roaring.Bitmap
	planes   []*roaring.Bitmap
}

func newBitSlicedOrdinal() *bitSlicedOrdinal {
	bs := &bitSlicedOrdinal{
		maxBit:   dateOrdinalBits,
		universe: roaring.New(),
		planes:   make([]*roaring.Bitmap, dateOrdinalBits+1),
	}
	for i := range bs.planes {
		bs.planes[i] = roaring.New()
	}
	return bs
}

func (bs *bitSlicedOrdinal) addDoc(docID uint32, ord uint32) {
	bs.universe.Add(docID)
	for bit := 0; bit <= bs.maxBit; bit++ {
		if (ord>>bit)&1 != 0 {
			bs.planes[bit].Add(docID)
		}
	}
}

func (bs *bitSlicedOrdinal) prefixBitmap(A uint32, k uint32) *roaring.Bitmap {
	bm := bs.universe.Clone()
	for bit := bs.maxBit; bit >= int(k); bit-- {
		want := (A >> bit) & 1
		if want == 1 {
			bm.And(bs.planes[bit])
		} else {
			bm.AndNot(bs.planes[bit])
		}
	}
	return bm
}

func (bs *bitSlicedOrdinal) rangeBitmap(L, R uint32) *roaring.Bitmap {
	if L > R {
		return roaring.New()
	}
	out := roaring.New()
	for L <= R {
		k := uint32(0)
		for {
			nextK := k + 1
			blockSize := uint32(1) << nextK
			if L%blockSize != 0 {
				break
			}
			if L+blockSize-1 > R {
				break
			}
			k = nextK
		}
		blockSize := uint32(1) << k
		out.Or(bs.prefixBitmap(L, k))
		L += blockSize
	}
	return out
}
