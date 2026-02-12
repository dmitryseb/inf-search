package lsm

import "sort"

type VersionedValue struct {
	value          *string
	sequenceNumber uint32
}

type MemTable struct {
	values map[string]VersionedValue
}

type MemTableEntry struct {
	Key   string
	Value VersionedValue
}

func NewMemTable() *MemTable {
	return &MemTable{
		values: make(map[string]VersionedValue),
	}
}

func (t *MemTable) Put(key string, value *string, sequence uint32) {
	t.values[key] = VersionedValue{
		value:          value,
		sequenceNumber: sequence,
	}
}

func (t *MemTable) Get(key string) (VersionedValue, bool) {
	if value, ok := t.values[key]; ok {
		return value, true
	}
	return VersionedValue{}, false
}

func (t *MemTable) Size() int {
	return len(t.values)
}

func (t *MemTable) SortedEntries() []MemTableEntry {
	entries := make([]MemTableEntry, 0, len(t.values))
	for k, v := range t.values {
		entries = append(entries, MemTableEntry{Key: k, Value: v})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})
	return entries
}
