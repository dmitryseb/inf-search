package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/emirpasic/gods/trees/binaryheap"
)

type SSTable struct {
	path            string
	f               *os.File
	keyCount        int
	indexStart      uint64
	offsetsStart    uint64
	keyOffsetsStart uint64
	minKey          string
	maxKey          string
	bloom           *BloomFilter
}

func CreateSSTableFromMemTable(path string, table *MemTable) (*SSTable, error) {
	entries := table.SortedEntries()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(path)
		}
	}()

	keys := make([]string, 0, len(entries))
	offsets := make([]uint64, 0, len(entries))

	bloom := NewBloomFilter(len(entries), 0.01)
	for _, e := range entries {
		keys = append(keys, e.Key)
		offsets = append(offsets, 0)
		bloom.AddString(e.Key)
	}

	bw := bufio.NewWriter(f)
	cw := &countingWriter{w: bw}
	if _, err = cw.Write(headerBytes(uint32(len(entries)), bloom)); err != nil {
		return nil, err
	}

	for i, e := range entries {
		offsets[i] = cw.n
		if _, err = writeRecord(cw, e.Value); err != nil {
			return nil, err
		}
	}

	indexStart := cw.n
	indexLen, err := writeIndex(cw, keys, offsets)
	if err != nil {
		return nil, err
	}
	if err = writeFooter(cw, indexStart, uint32(indexLen)); err != nil {
		return nil, err
	}
	if err = bw.Flush(); err != nil {
		return nil, err
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	s := &SSTable{path: path, f: f}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func MergeSSTables(path string, tables ...*SSTable) (*SSTable, error) {
	expected := 0
	for _, t := range tables {
		expected += t.keyCount
	}
	bloom := NewBloomFilter(expected, 0.01)
	headerSize := 16 + 8*len(bloom.bits)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(path)
		}
	}()

	bw := bufio.NewWriter(f)
	cw := &countingWriter{w: bw}
	if _, err = cw.Write(make([]byte, headerSize)); err != nil {
		return nil, err
	}

	dir := filepath.Dir(path)
	mk := func() (*os.File, *bufio.Writer, error) {
		tf, err := os.CreateTemp(dir, "sst-*.tmp")
		if err != nil {
			return nil, nil, err
		}
		return tf, bufio.NewWriter(tf), nil
	}
	keysF, keysW, err := mk()
	if err != nil {
		return nil, err
	}
	offsF, offsW, err := mk()
	if err != nil {
		return nil, err
	}
	keyOffsF, keyOffsW, err := mk()
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, tf := range []*os.File{keysF, offsF, keyOffsF} {
			if tf != nil {
				_ = tf.Close()
				_ = os.Remove(tf.Name())
			}
		}
	}()

	var keyEntriesN uint64
	var u32 [4]byte
	var u64 [8]byte

	outCount := 0
	err = mergeKWay(tables, func(key string, best VersionedValue) error {
		bloom.AddString(key)
		outCount++

		recordOffset := cw.n
		if _, err := writeRecord(cw, best); err != nil {
			return err
		}

		start := uint32(keyEntriesN)
		kb := []byte(key)
		binary.LittleEndian.PutUint32(u32[:], uint32(len(kb)))
		if _, err := keysW.Write(u32[:]); err != nil {
			return err
		}
		if _, err := keysW.Write(kb); err != nil {
			return err
		}
		keyEntriesN += uint64(4 + len(kb))

		binary.LittleEndian.PutUint64(u64[:], recordOffset)
		if _, err := offsW.Write(u64[:]); err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(u32[:], start)
		_, err := keyOffsW.Write(u32[:])
		return err
	})
	if err != nil {
		return nil, err
	}

	if err = keysW.Flush(); err != nil {
		return nil, err
	}
	if err = offsW.Flush(); err != nil {
		return nil, err
	}
	if err = keyOffsW.Flush(); err != nil {
		return nil, err
	}
	if err = bw.Flush(); err != nil {
		return nil, err
	}

	indexStart := cw.n
	indexLen := keyEntriesN + uint64(outCount)*8 + uint64(outCount)*4

	for _, tf := range []*os.File{keysF, offsF, keyOffsF} {
		if _, err := tf.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if _, err := io.Copy(f, tf); err != nil {
			return nil, err
		}
	}

	if err = writeFooter(f, indexStart, uint32(indexLen)); err != nil {
		return nil, err
	}
	if _, err = f.WriteAt(headerBytes(uint32(outCount), bloom), 0); err != nil {
		return nil, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	s := &SSTable{path: path, f: f}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SSTable) Path() string { return s.path }

func (s *SSTable) Close() error {
	if s.f == nil {
		return nil
	}
	return s.f.Close()
}

func (s *SSTable) Get(key string) (VersionedValue, bool, error) {
	if s.bloom != nil && !s.bloom.MightContainString(key) {
		return VersionedValue{}, false, nil
	}
	idx, ok, err := s.findKeyIndex(key)
	if err != nil || !ok {
		return VersionedValue{}, false, err
	}
	offset, err := s.offsetAt(idx)
	if err != nil {
		return VersionedValue{}, false, err
	}
	v, err := s.readRecordAt(offset)
	if err != nil {
		return VersionedValue{}, false, err
	}
	return v, true, nil
}

func (s *SSTable) load() error {
	size, err := fileSize(s.f)
	if err != nil {
		return err
	}
	if size < footerSizeBytes {
		return errors.New("sstable: file too small")
	}

	footer := make([]byte, footerSizeBytes)
	if _, err := s.f.ReadAt(footer, int64(size-footerSizeBytes)); err != nil {
		return err
	}
	indexStart := binary.LittleEndian.Uint64(footer[0:8])
	indexLen := binary.LittleEndian.Uint32(footer[8:12])

	keyCount, bloom, err := readHeaderAt(s.f, 0)
	if err != nil {
		return err
	}

	metaSize := uint64(keyCount)*8 + uint64(keyCount)*4
	if uint64(indexLen) < metaSize {
		return errors.New("sstable: index too small")
	}
	keyEntriesLen := uint64(indexLen) - metaSize
	offsetsStart := indexStart + keyEntriesLen
	keyOffsetsStart := offsetsStart + uint64(keyCount)*8

	s.keyCount = int(keyCount)
	s.indexStart = indexStart
	s.offsetsStart = offsetsStart
	s.keyOffsetsStart = keyOffsetsStart
	s.bloom = bloom

	if s.keyCount > 0 {
		minK, err := s.keyAt(0)
		if err != nil {
			return err
		}
		maxK, err := s.keyAt(s.keyCount - 1)
		if err != nil {
			return err
		}
		s.minKey = minK
		s.maxKey = maxK
	}
	return nil
}

func (s *SSTable) findKeyIndex(key string) (int, bool, error) {
	l, r := 0, s.keyCount
	for l < r {
		mid := (l + r) / 2
		mk, err := s.keyAt(mid)
		if err != nil {
			return 0, false, err
		}
		if mk < key {
			l = mid + 1
		} else {
			r = mid
		}
	}
	if l >= s.keyCount {
		return 0, false, nil
	}
	k, err := s.keyAt(l)
	if err != nil {
		return 0, false, err
	}
	return l, k == key, nil
}

func (s *SSTable) keyAt(i int) (string, error) {
	rel, err := s.keyOffsetAt(i)
	if err != nil {
		return "", err
	}
	off := int64(s.indexStart) + int64(rel)

	var u32 [4]byte
	if _, err := s.f.ReadAt(u32[:], off); err != nil {
		return "", err
	}
	keyLen := int(binary.LittleEndian.Uint32(u32[:]))
	keyBytes := make([]byte, keyLen)
	if _, err := s.f.ReadAt(keyBytes, off+4); err != nil {
		return "", err
	}
	return string(keyBytes), nil
}

func (s *SSTable) keyOffsetAt(i int) (uint32, error) {
	var u32 [4]byte
	if _, err := s.f.ReadAt(u32[:], int64(s.keyOffsetsStart)+int64(i*4)); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(u32[:]), nil
}

func (s *SSTable) offsetAt(i int) (uint64, error) {
	var u64 [8]byte
	if _, err := s.f.ReadAt(u64[:], int64(s.offsetsStart)+int64(i*8)); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(u64[:]), nil
}

func (s *SSTable) readRecordAt(offset uint64) (VersionedValue, error) {
	off := int64(offset)

	var hv [1]byte
	if _, err := s.f.ReadAt(hv[:], off); err != nil {
		return VersionedValue{}, err
	}
	pos := off + 1

	var valuePtr *string
	if hv[0] == 1 {
		var u32 [4]byte
		if _, err := s.f.ReadAt(u32[:], pos); err != nil {
			return VersionedValue{}, err
		}
		pos += 4

		valLen := int(binary.LittleEndian.Uint32(u32[:]))
		valBytes := make([]byte, valLen)
		if _, err := s.f.ReadAt(valBytes, pos); err != nil {
			return VersionedValue{}, err
		}
		pos += int64(valLen)

		valStr := string(valBytes)
		valuePtr = &valStr
	}

	var seq [4]byte
	if _, err := s.f.ReadAt(seq[:], pos); err != nil {
		return VersionedValue{}, err
	}
	return VersionedValue{value: valuePtr, sequenceNumber: binary.LittleEndian.Uint32(seq[:])}, nil
}

const (
	footerSizeBytes = 12
)

func readHeaderAt(f *os.File, off int64) (keyCount uint32, bloom *BloomFilter, err error) {
	var hdr [16]byte
	if _, err = f.ReadAt(hdr[:], off); err != nil {
		return 0, nil, err
	}

	keyCount = binary.LittleEndian.Uint32(hdr[0:4])
	mBits := binary.LittleEndian.Uint32(hdr[4:8])
	wordCount := binary.LittleEndian.Uint32(hdr[8:12])

	words := make([]uint64, wordCount)
	var u64 [8]byte
	for i := range words {
		if _, err := f.ReadAt(u64[:], off+16+int64(i)*8); err != nil {
			return 0, nil, err
		}
		words[i] = binary.LittleEndian.Uint64(u64[:])
	}

	bloom = &BloomFilter{
		mBits: uint64(mBits),
		bits:  words,
	}
	return keyCount, bloom, nil
}

func headerBytes(keyCount uint32, bloom *BloomFilter) []byte {
	b := make([]byte, 16+8*len(bloom.bits))
	binary.LittleEndian.PutUint32(b[0:4], keyCount)
	binary.LittleEndian.PutUint32(b[4:8], uint32(bloom.mBits))
	binary.LittleEndian.PutUint32(b[8:12], uint32(len(bloom.bits)))
	binary.LittleEndian.PutUint32(b[12:16], 0)

	pos := 16
	for _, word := range bloom.bits {
		binary.LittleEndian.PutUint64(b[pos:pos+8], word)
		pos += 8
	}
	return b
}

func writeFooter(w io.Writer, indexStart uint64, indexLen uint32) error {
	var ftr [footerSizeBytes]byte
	binary.LittleEndian.PutUint64(ftr[0:8], indexStart)
	binary.LittleEndian.PutUint32(ftr[8:12], indexLen)
	_, err := w.Write(ftr[:])
	return err
}

func writeRecord(w io.Writer, v VersionedValue) (int, error) {
	var u32 [4]byte
	var hv [1]byte
	if v.value != nil {
		hv[0] = 1
	}
	n := 0
	if _, err := w.Write(hv[:]); err != nil {
		return n, err
	}
	n++

	if v.value != nil {
		valueBytes := []byte(*v.value)
		binary.LittleEndian.PutUint32(u32[:], uint32(len(valueBytes)))
		if _, err := w.Write(u32[:]); err != nil {
			return n, err
		}
		n += 4
		if _, err := w.Write(valueBytes); err != nil {
			return n, err
		}
		n += len(valueBytes)
	}

	var seq [4]byte
	binary.LittleEndian.PutUint32(seq[:], v.sequenceNumber)
	if _, err := w.Write(seq[:]); err != nil {
		return n, err
	}
	n += 4
	return n, nil
}

func writeIndex(w io.Writer, keys []string, offsets []uint64) (int, error) {
	keyOffsets := make([]uint32, len(keys))

	n := 0
	var u32 [4]byte
	for i, k := range keys {
		keyOffsets[i] = uint32(n)

		kb := []byte(k)
		binary.LittleEndian.PutUint32(u32[:], uint32(len(kb)))
		if _, err := w.Write(u32[:]); err != nil {
			return 0, err
		}
		n += 4
		if _, err := w.Write(kb); err != nil {
			return 0, err
		}
		n += len(kb)
	}

	var u64 [8]byte
	for _, off := range offsets {
		binary.LittleEndian.PutUint64(u64[:], off)
		if _, err := w.Write(u64[:]); err != nil {
			return 0, err
		}
		n += 8
	}

	for _, ko := range keyOffsets {
		binary.LittleEndian.PutUint32(u32[:], ko)
		if _, err := w.Write(u32[:]); err != nil {
			return 0, err
		}
		n += 4
	}

	return n, nil
}

func fileSize(f *os.File) (uint64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

type countingWriter struct {
	w io.Writer
	n uint64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

type it struct {
	t   *SSTable
	i   int
	key string
}

func mergeKWay(tables []*SSTable, emit func(key string, best VersionedValue) error) error {
	heap := binaryheap.NewWith(func(a, b any) int {
		ai := a.(*it)
		bi := b.(*it)
		switch {
		case ai.key < bi.key:
			return -1
		case ai.key > bi.key:
			return 1
		default:
			return 0
		}
	})

	for _, t := range tables {
		if t.keyCount == 0 {
			continue
		}
		k, err := t.keyAt(0)
		if err != nil {
			return err
		}
		heap.Push(&it{t: t, i: 0, key: k})
	}

	for heap.Size() > 0 {
		v, _ := heap.Pop()
		cur := v.(*it)
		key := cur.key

		offset, err := cur.t.offsetAt(cur.i)
		if err != nil {
			return err
		}
		best, err := cur.t.readRecordAt(offset)
		if err != nil {
			return err
		}
		if cur.i+1 < cur.t.keyCount {
			k, err := cur.t.keyAt(cur.i + 1)
			if err != nil {
				return err
			}
			heap.Push(&it{t: cur.t, i: cur.i + 1, key: k})
		}

		for heap.Size() > 0 {
			top, _ := heap.Peek()
			if top.(*it).key != key {
				break
			}
			v, _ := heap.Pop()
			cur = v.(*it)
			offset, err = cur.t.offsetAt(cur.i)
			if err != nil {
				return err
			}
			val, err := cur.t.readRecordAt(offset)
			if err != nil {
				return err
			}
			if val.sequenceNumber > best.sequenceNumber {
				best = val
			}
			if cur.i+1 < cur.t.keyCount {
				k, err := cur.t.keyAt(cur.i + 1)
				if err != nil {
					return err
				}
				heap.Push(&it{t: cur.t, i: cur.i + 1, key: k})
			}
		}

		if err := emit(key, best); err != nil {
			return err
		}
	}
	return nil
}
