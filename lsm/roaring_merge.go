package lsm

import (
	"github.com/RoaringBitmap/roaring/v2"
)

func mergeTwoRoaringStrings(a, b *string) (*string, error) {
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}
	left := roaring.New()
	if _, err := left.FromBuffer([]byte(*a)); err != nil {
		return nil, err
	}
	right := roaring.New()
	if _, err := right.FromBuffer([]byte(*b)); err != nil {
		return nil, err
	}
	left.Or(right)
	data, err := left.ToBytes()
	if err != nil {
		return nil, err
	}
	s := string(data)
	return &s, nil
}

func mergeVersionedRoaring(vals []VersionedValue) (VersionedValue, error) {
	var maxSeq uint32
	out := roaring.New()

	for _, v := range vals {
		if v.sequenceNumber > maxSeq {
			maxSeq = v.sequenceNumber
		}
		if v.value == nil {
			continue
		}
		bm := roaring.New()
		if _, err := bm.FromBuffer([]byte(*v.value)); err != nil {
			return VersionedValue{}, err
		}
		out.Or(bm)
	}

	data, err := out.ToBytes()
	if err != nil {
		return VersionedValue{}, err
	}
	s := string(data)
	return VersionedValue{value: &s, sequenceNumber: maxSeq}, nil
}
