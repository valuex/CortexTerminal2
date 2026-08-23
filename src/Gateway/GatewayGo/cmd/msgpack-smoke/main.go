// Command msgpack-smoke verifies the MessagePack codec produces wire bytes
// that match the C# MessagePack library:
//
//  1. DateTime ext-type 0x12 carries 8-byte big-endian int64 ticks since
//     0001-01-01T00:00:00Z (100ns intervals).
//  2. DateTimeOffset ext-type 0x13 carries 13 bytes: 8-byte ticks +
//     5-byte offset (signed 16-bit minutes BE + 3 zero bytes).
//  3. A known timestamp round-trips losslessly through the wrapper.
//  4. A HandshakeRequest {"protocol":"messagepack","version":1} encodes
//     to the same msgpack map that the C# library produces.
//  5. The WireCodec round-trips an Invocation frame.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ALL OK")
}

func run() error {
	// 1. Encode DateTime payload via the public wrapper.
	ts := time.Date(2024, 1, 15, 12, 34, 56, 0, time.UTC)
	dtBuf, err := signalr.NewDateTime(ts).MarshalMsgpack()
	if err != nil {
		return err
	}
	if len(dtBuf) != 8 {
		return fmt.Errorf("DateTime payload must be 8 bytes, got %d", len(dtBuf))
	}
	gotTicks := int64(binary.BigEndian.Uint64(dtBuf))
	wantTicks := signalr.DotnetTicks(ts)
	if gotTicks != wantTicks {
		return fmt.Errorf("DateTime ticks mismatch: got %d want %d", gotTicks, wantTicks)
	}
	fmt.Printf("DateTime ticks: %d (hex %s)\n", gotTicks, hex.EncodeToString(dtBuf))

	// 2. Encode DateTimeOffset payload (Beijing +08:00).
	beijing := time.FixedZone("CST", 8*60)
	tsLocal := time.Date(2024, 1, 15, 20, 34, 56, 0, beijing)
	dtoBuf, err := signalr.NewDateTimeOffset(tsLocal, 8*60).MarshalMsgpack()
	if err != nil {
		return err
	}
	if len(dtoBuf) != 13 {
		return fmt.Errorf("DateTimeOffset payload must be 13 bytes, got %d", len(dtoBuf))
	}
	gotOffset := int16(binary.BigEndian.Uint16(dtoBuf[8:10]))
	if gotOffset != 8*60 {
		return fmt.Errorf("DateTimeOffset offset mismatch: got %d want %d", gotOffset, 8*60)
	}

	// 3. Round-trip through the wrapper.
	var decoded signalr.DateTimeOffset
	if err := decoded.UnmarshalMsgpack(dtoBuf); err != nil {
		return err
	}
	if decoded.OffsetMinutes != 8*60 {
		return fmt.Errorf("round-trip offset mismatch: got %d want %d", decoded.OffsetMinutes, 8*60)
	}
	if !decoded.Time.Equal(tsLocal) {
		return fmt.Errorf("round-trip time mismatch: got %v want %v", decoded.Time, tsLocal)
	}
	fmt.Printf("DateTimeOffset round-trip: %v (offset=%d min)\n", decoded.Time, decoded.OffsetMinutes)

	// 4. HandshakeRequest encoded as msgpack. The C# SignalR MessagePack
// library writes both Protocol AND Version fields (as a 2-element map),
// even when Version is the default value. Verify the wire shape is
// `{protocol:"messagepack", version:1}`.
	hsBuf, err := msgpack.Marshal(signalr.HandshakeRequest{Protocol: "messagepack", Version: 1})
	if err != nil {
		return err
	}
	fmt.Printf("Handshake msgpack: %s\n", hex.EncodeToString(hsBuf))
	want := "82a870726f746f636f6cab6d6573736167657061636ba776657273696f6e01"
	if hex.EncodeToString(hsBuf) != want {
		return fmt.Errorf("handshake msgpack mismatch:\n  got  %s\n  want %s", hex.EncodeToString(hsBuf), want)
	}

	// 5. WireCodec round-trip. EncodeMessage returns a length-prefixed frame;
//    the connection's read loop strips the varint length before calling
//    DecodeMessage, so the smoke test mirrors that.
	type SampleFrame struct {
		Type   byte   `msgpack:"type"`
		Target string `msgpack:"target"`
		InvID  string `msgpack:"invocationId"`
		Args   []any  `msgpack:"arguments"`
	}
	codec := signalr.NewWireCodecForTest()
	invOut, err := codec.EncodeMessage(SampleFrame{
		Type: byte(signalr.MessageTypeInvocation), Target: "echo", InvID: "1", Args: []any{"hello"}})
	if err != nil {
		return err
	}
	if len(invOut) == 0 {
		return fmt.Errorf("EncodeMessage produced empty output")
	}
	// strip the varint length prefix (mirrors the connection's read loop).
	_, body, err := signalr.SplitMsgpackFrame(invOut)
	if err != nil {
		return fmt.Errorf("split frame: %w", err)
	}
	var dec SampleFrame
	if err := codec.DecodeMessage(body, &dec); err != nil {
		return err
	}
	if dec.Target != "echo" || dec.InvID != "1" {
		return fmt.Errorf("round-trip mismatch: got %+v", dec)
	}
	fmt.Printf("WireCodec round-trip OK (frame=%d bytes, body=%d bytes)\n", len(invOut), len(body))

	// 6. DateTime wrapper round-trip through msgpack.Marshal/Unmarshal.
//    Fields holding an ext codec must be pointer-typed so msgpack can
//    call MarshalMsgpack on an addressable value (the ext registry maps
//    by *DateTime, so the field type is *DateTime).
	type DTFrame struct {
		When *signalr.DateTime `msgpack:"when"`
	}
	src := &DTFrame{When: signalr.NewDateTimePtr(time.Date(2025, 6, 1, 9, 0, 0, 0, time.UTC))}
	srcBuf, err := msgpack.Marshal(src)
	if err != nil {
		return err
	}
	fmt.Printf("DateTime msgpack frame: %s\n", hex.EncodeToString(srcBuf))
	var dst DTFrame
	if err := msgpack.Unmarshal(srcBuf, &dst); err != nil {
		return err
	}
	if dst.When == nil || !dst.When.Time.Equal(src.When.Time) {
		return fmt.Errorf("DateTime wrapper round-trip mismatch: got %v want %v", dst.When, src.When)
	}
	fmt.Printf("DateTime wrapper round-trip OK\n")

	return nil
}