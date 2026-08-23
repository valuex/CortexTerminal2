package signalr

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// DateTime / DateTimeOffset ext-type ids used by the C# MessagePack library.
//
// C# MessagePack uses ext-type 0x12 for DateTime (UTC instant, no offset)
// and 0x13 for DateTimeOffset (UTC instant + offset from UTC). Both carry
// the same 8-byte int64 tick payload counting 100-ns intervals since
// 0001-01-01T00:00:00Z (the .NET DateTime.MinValue). DateTimeOffset adds a
// trailing 5-byte offset (signed 16-bit minutes offset, big-endian, padded
// with three zero bytes).
//
// Ext ids are int8 in the msgpack/v5 registry — they fit in a signed byte
// since the C# library only uses the small positive range.
const (
	ExtIDDateTime       int8 = 0x12
	ExtIDDateTimeOffset int8 = 0x13

	extTypeDateTime       byte = 0x12
	extTypeDateTimeOffset byte = 0x13
)

// dotnetEpoch is the .NET DateTime.MinValue. Go's time.Time epoch is
// 0001-01-01 too, so we can compute ticks via Sub().
var dotnetEpoch = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

// ticksPerDay is the number of .NET ticks in one civil day: 86400s × 10^7.
const ticksPerDay int64 = 864000000000 // 86400 * 1e7

// DotnetTicks returns .NET-style 100ns ticks since 0001-01-01T00:00:00Z
// for the given time. The time is normalised to UTC first — .NET ticks
// are always UTC-anchored regardless of the source DateTimeKind.
//
// We compute via days×864000000000 + ns-of-day/100 to avoid the
// int64-nanosecond overflow that hits ~year 2262. .NET ticks for the
// year 2024 are ~6.4e18, which is within int64 (max ~9.2e18), but the
// raw Nanoseconds() value (~6.4e19) is not.
func DotnetTicks(t time.Time) int64 {
	t = t.UTC()
	days := daysSinceDotnetEpoch(t)
	nsOfDay := int64(t.Hour()*3600*1e9+t.Minute()*60*1e9+t.Second()*1e9) + int64(t.Nanosecond())
	ticksOfDay := nsOfDay / 100
	return days*ticksPerDay + ticksOfDay
}

// DotnetTicksToGo converts .NET 100ns ticks to a Go time.Time. The time
// is anchored at 0001-01-01 UTC (the .NET DateTime.MinValue).
func DotnetTicksToGo(ticks int64) time.Time {
	days := ticks / ticksPerDay
	remTicks := ticks % ticksPerDay
	if remTicks < 0 {
		// ticks before dotnetEpoch: normalise so day count + remainder are non-negative.
		days--
		remTicks += ticksPerDay
	}
	// Add days to the epoch, then add remainder as nanoseconds.
	base := dotnetEpoch.AddDate(0, 0, int(days))
	// remainder is in 100ns units; convert to nanoseconds safely via Duration.
	return base.Add(time.Duration(remTicks) * 100)
}

// daysSinceDotnetEpoch counts the number of calendar days from
// 0001-01-01 (the .NET epoch) to t. We use the proleptic Gregorian
// day-number formula. This avoids any int64-nanosecond arithmetic.
//
// Layout:
//
//	days = (y-1)*365 + (y-1)/4 - (y-1)/100 + (y-1)/400   // years before y
//	     + dayOfYear(y, m, d) - 1                          // days into year y
//
// where dayOfYear is just a month-length table with a leap-year rule.
func daysSinceDotnetEpoch(t time.Time) int64 {
	y := int64(t.Year())
	m := int64(t.Month())
	d := int64(t.Day())

	yearsBefore := y - 1
	days := yearsBefore*365 + yearsBefore/4 - yearsBefore/100 + yearsBefore/400

	// Cumulative month lengths starting from January in a non-leap year.
	// Index 0 is a dummy so we can index directly by month.
	monthCum := [13]int64{
		0,
		0,  // Jan
		31, // Feb 1
		31 + 28,
		31 + 59,
		31 + 90,
		31 + 120,
		31 + 151,
		31 + 181,
		31 + 212,
		31 + 243,
		31 + 273,
		31 + 304,
	}
	days += monthCum[m]
	if m > 2 && isLeap(y) {
		days++
	}
	days += d - 1
	return days
}

func isLeap(y int64) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

// EncodeDateTimeOffsetPayload returns the 13-byte payload of a
// DateTimeOffset ext (8-byte ticks + 5-byte offset). The C# library reads
// the offset as a signed 16-bit minutes value, big-endian, padded with
// three zero bytes.
func EncodeDateTimeOffsetPayload(t time.Time, offsetMin int16) []byte {
	ticks := DotnetTicks(t.UTC())
	buf := make([]byte, 13)
	binary.BigEndian.PutUint64(buf[0:8], uint64(ticks))
	binary.BigEndian.PutUint16(buf[8:10], uint16(offsetMin))
	// bytes 10..12 are zero (padding).
	return buf
}

// EncodeDateTimePayload returns the 8-byte payload of a DateTime ext.
func EncodeDateTimePayload(t time.Time) []byte {
	ticks := DotnetTicks(t.UTC())
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf[0:8], uint64(ticks))
	return buf
}

// DateTime is the SignalR/MessagePack timestamp wrapper used by the
// portable wire format. It marshals as ext-type 0x12 (DateTime, UTC
// instant, no offset) and unmarshals from both 0x12 and 0x13 (the latter
// silently drops the offset).
//
// For lossless DateTimeOffset round-trip, use DateTimeOffset.
type DateTime struct {
	time.Time
}

// NewDateTime wraps t in the ext-0x12 codec. The time is normalised to UTC
// at encode time.
func NewDateTime(t time.Time) DateTime { return DateTime{Time: t} }

// NewDateTimePtr wraps t as *DateTime for use in struct fields that need
// to be addressable for msgpack.Marshaler's MarshalMsgpack call.
func NewDateTimePtr(t time.Time) *DateTime {
	d := NewDateTime(t)
	return &d
}

// MarshalMsgpack implements msgpack.Marshaler — returns the 8-byte payload
// for ext-type 0x12.
func (d DateTime) MarshalMsgpack() ([]byte, error) {
	return EncodeDateTimePayload(d.Time), nil
}

// UnmarshalMsgpack implements msgpack.Unmarshaler — accepts ext-type 0x12
// (8 bytes) or 0x13 (13 bytes).
func (d *DateTime) UnmarshalMsgpack(data []byte) error {
	switch len(data) {
	case 8:
		ticks := int64(binary.BigEndian.Uint64(data))
		d.Time = DotnetTicksToGo(ticks)
		return nil
	case 13:
		ticks := int64(binary.BigEndian.Uint64(data[0:8]))
		offset := int16(binary.BigEndian.Uint16(data[8:10]))
		loc := time.FixedZone("offset", int(offset)*60)
		d.Time = DotnetTicksToGo(ticks).In(loc)
		return nil
	default:
		return fmt.Errorf("msgpack: DateTime ext payload must be 8 or 13 bytes, got %d", len(data))
	}
}

// DateTimeOffset is the lossless SignalR/MessagePack timestamp wrapper that
// preserves the offset. Marshals as ext-type 0x13.
type DateTimeOffset struct {
	time.Time
	OffsetMinutes int16
}

// NewDateTimeOffset builds a wrapper from t and its offset in minutes east
// of UTC.
func NewDateTimeOffset(t time.Time, offsetMin int16) DateTimeOffset {
	return DateTimeOffset{Time: t, OffsetMinutes: offsetMin}
}

// MarshalMsgpack implements msgpack.Marshaler — emits ext-type 0x13 with
// the 13-byte payload.
func (d DateTimeOffset) MarshalMsgpack() ([]byte, error) {
	return EncodeDateTimeOffsetPayload(d.Time, d.OffsetMinutes), nil
}

// UnmarshalMsgpack implements msgpack.Unmarshaler — accepts ext-type 0x12
// (8 bytes, treats as UTC) or 0x13 (13 bytes, lossless).
func (d *DateTimeOffset) UnmarshalMsgpack(data []byte) error {
	switch len(data) {
	case 8:
		ticks := int64(binary.BigEndian.Uint64(data))
		d.Time = DotnetTicksToGo(ticks)
		d.OffsetMinutes = 0
		return nil
	case 13:
		ticks := int64(binary.BigEndian.Uint64(data[0:8]))
		offset := int16(binary.BigEndian.Uint16(data[8:10]))
		d.Time = DotnetTicksToGo(ticks)
		d.OffsetMinutes = offset
		return nil
	default:
		return fmt.Errorf("msgpack: DateTimeOffset ext payload must be 8 or 13 bytes, got %d", len(data))
	}
}

// init registers both timestamp wrappers with the msgpack ext registry.
// The `*T` pointer is what RegisterExt needs to look up the type — Go
// uses it to determine the Go type for fields declared as `DateTime` or
// `*DateTime`.
func init() {
	msgpack.RegisterExt(ExtIDDateTime, &DateTime{})
	msgpack.RegisterExt(ExtIDDateTimeOffset, &DateTimeOffset{})
}

// Expose the byte ext-id constants for tests / readers that don't want to
// import msgpack.
const (
	ExtTypeDateTime       = extTypeDateTime
	ExtTypeDateTimeOffset = extTypeDateTimeOffset
)