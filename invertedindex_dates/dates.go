package invertedindex_dates

import (
	"time"
)

func ordinalDayUTC(t time.Time) uint32 {
	u := dateUTC(t)
	return uint32(u.Unix() / 86400)
}

var farEndOrdinal = ordinalDayUTC(time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC))

type DocDates struct {
	ValidStart time.Time
	ValidEnd   *time.Time
}

func dateUTC(t time.Time) time.Time {
	y, m, d := t.In(time.UTC).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
