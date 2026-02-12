package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type LSM struct {
	maxSize          int
	dir              string
	maxFilesPerLevel int

	memTable       *MemTable
	constMemTable  *MemTable
	files          [][]*SSTable
	nonOverlap     []bool
	sequenceNumber uint32
	nextFileID     uint64

	mutex      sync.RWMutex
	compacting bool
}

func Init(maxSize int) *LSM {
	return &LSM{
		maxSize:          maxSize,
		dir:              "lsmdata",
		maxFilesPerLevel: 6,
		memTable:         NewMemTable(),
	}
}

func InitWithDir(maxSize int, dir string) *LSM {
	l := Init(maxSize)
	l.dir = dir
	return l
}

func (l *LSM) Put(key string, value *string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	seq := l.sequenceNumber
	l.sequenceNumber++

	l.memTable.Put(key, value, seq)
	if l.maxSize < l.memTable.Size() && !l.compacting {
		l.compacting = true
		go l.Compact()
	}
}

func (l *LSM) Get(key string) *string {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	if v, ok := l.memTable.Get(key); ok {
		return v.value
	}
	if l.constMemTable != nil {
		if v, ok := l.constMemTable.Get(key); ok {
			return v.value
		}
	}

	for levelIdx, level := range l.files {
		no := levelIdx < len(l.nonOverlap) && l.nonOverlap[levelIdx]
		if no && len(level) > 0 {
			i := sort.Search(len(level), func(i int) bool { return level[i].maxKey >= key })
			if i < len(level) && level[i].minKey <= key {
				v, ok, err := level[i].Get(key)
				if err == nil && ok {
					return v.value
				}
			}
			continue
		}

		for i := len(level) - 1; i >= 0; i-- {
			f := level[i]
			if f.keyCount > 0 && (key < f.minKey || key > f.maxKey) {
				continue
			}
			v, ok, err := f.Get(key)
			if err == nil && ok {
				return v.value
			}
		}
	}

	return nil
}

func (l *LSM) Compact() error {
	l.mutex.Lock()
	if l.memTable.Size() == 0 {
		l.compacting = false
		l.mutex.Unlock()
		return nil
	}

	l.constMemTable = l.memTable
	l.memTable = NewMemTable()
	snapshot := l.constMemTable

	path := l.newFilePathLocked(0)
	l.mutex.Unlock()

	sst, err := CreateSSTableFromMemTable(path, snapshot)
	if err != nil {
		l.mutex.Lock()
		l.compacting = false
		l.mutex.Unlock()
		return err
	}

	l.mutex.Lock()
	l.constMemTable = nil
	l.ensureLevelLocked(0)
	l.files[0] = append(l.files[0], sst)
	if len(l.files[0]) == 1 {
		l.nonOverlap[0] = true
	} else if l.nonOverlap[0] {
		prev := l.files[0][len(l.files[0])-2]
		if prev.keyCount > 0 && sst.keyCount > 0 && prev.maxKey >= sst.minKey {
			l.nonOverlap[0] = false
		}
	}
	err = l.maybeCompactLevelsLocked()
	l.compacting = false
	l.mutex.Unlock()
	return err
}

func (l *LSM) ensureLevelLocked(level int) {
	for len(l.files) <= level {
		l.files = append(l.files, nil)
		l.nonOverlap = append(l.nonOverlap, true)
	}
}

func (l *LSM) newFilePathLocked(level int) string {
	id := l.nextFileID
	l.nextFileID++
	name := fmt.Sprintf("L%d-%06d.sst", level, id)
	return filepath.Join(l.dir, name)
}

func (l *LSM) maybeCompactLevelsLocked() error {
	for level := 0; level < len(l.files); level++ {
		if len(l.files[level]) <= l.maxFilesPerLevel {
			continue
		}

		nextLevel := level + 1
		l.ensureLevelLocked(nextLevel)
		tables := append([]*SSTable(nil), l.files[level]...)

		outPath := l.newFilePathLocked(nextLevel)
		merged, err := MergeSSTables(outPath, tables...)
		if err != nil {
			return err
		}

		for _, t := range tables {
			t.Close()
			os.Remove(t.Path())
		}

		l.files[level] = nil
		l.files[nextLevel] = append(l.files[nextLevel], merged)
		l.nonOverlap[level] = true
		l.nonOverlap[nextLevel] = true
	}
	return nil
}
