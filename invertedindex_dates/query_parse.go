package invertedindex_dates

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

type rangeKind int

const (
	rangeDateAttr rangeKind = iota
	rangeValid
	rangeAppeared
)

func parseDateRangeTail(s string, start int) (from, to time.Time, end int, err error) {
	const layout = "2006-01-02"
	i := start
	for i < len(s) && unicode.IsSpace(rune(s[i])) {
		i++
	}
	comma := strings.IndexByte(s[i:], ',')
	if comma < 0 {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("date range: missing comma")
	}
	comma += i
	from, err = time.ParseInLocation(layout, strings.TrimSpace(s[i:comma]), time.UTC)
	if err != nil {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("date range from: %w", err)
	}
	j := comma + 1
	closeIdx := strings.IndexByte(s[j:], ']')
	if closeIdx < 0 {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("date range: missing ]")
	}
	closeIdx += j
	to, err = time.ParseInLocation(layout, strings.TrimSpace(s[j:closeIdx]), time.UTC)
	if err != nil {
		return time.Time{}, time.Time{}, 0, fmt.Errorf("date range to: %w", err)
	}
	return from, to, closeIdx + 1, nil
}
