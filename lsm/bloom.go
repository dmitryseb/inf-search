package lsm

import "hash/fnv"

type BloomFilter struct {
	mBits uint64
	bits  []uint64
}

const (
	bloomMixConst uint64 = 1791791791
	bloomShift           = 33
)

func NewBloomFilter(expectedItems int) *BloomFilter {
	if expectedItems < 1 {
		expectedItems = 1
	}

	m := max(uint64(expectedItems * 10), 64)

	wordCount := (m + 63) / 64
	return &BloomFilter{
		mBits: m,
		bits:  make([]uint64, wordCount),
	}
}

func (b *BloomFilter) AddString(s string) {
	h1, h2 := bloomHashes(s)
	b.setBit(h1 % b.mBits)
	b.setBit(h2 % b.mBits)
}

func (b *BloomFilter) MightContainString(s string) bool {
	h1, h2 := bloomHashes(s)
	return b.getBit(h1 % b.mBits) && b.getBit(h2 % b.mBits)
}

func (b *BloomFilter) setBit(bit uint64) {
	word := bit / 64
	shift := bit % 64
	b.bits[word] |= 1 << shift
}

func (b *BloomFilter) getBit(bit uint64) bool {
	word := bit / 64
	shift := bit % 64
	return b.bits[word]&(1<<shift) != 0
}

func bloomHashes(s string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	sum1 := h.Sum64()
	sum2 := (sum1 >> bloomShift) ^ (sum1 * bloomMixConst)
	if sum2 == 0 {
		sum2 = bloomMixConst
	}
	return sum1, sum2
}
