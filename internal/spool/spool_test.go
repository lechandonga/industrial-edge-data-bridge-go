package spool

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

func rd(device string, seq uint64) []model.Reading {
	id := device + ":1"
	_ = id
	return []model.Reading{{DeviceID: device, FrameID: frameID(device, seq), Metric: "temperature", Value: 1}}
}

func frameID(device string, seq uint64) string {
	return string(append(append([]byte(device), ':'), digit(seq)...))
}

func digit(n uint64) []byte {
	if n == 0 {
		return []byte{'0'}
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return b
}

// TestBlockAndDropOldest 直接验证两种有界策略的语义。
func TestBlockAndDropOldest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	sp, err := Open(dir, 2, PolicyBlock)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := sp.Enqueue(rd("d", 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sp.Enqueue(rd("d", 2)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sp.Enqueue(rd("d", 3)); !errors.Is(err, ErrOverflow) {
		t.Fatalf("want ErrOverflow, got %v", err)
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}

	dir2 := filepath.Join(t.TempDir(), "s2")
	sp2, err := Open(dir2, 2, PolicyDropOldest)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _ = sp2.Enqueue(rd("d", 1))
	_, _, _ = sp2.Enqueue(rd("d", 2))
	accepted, ev, err := sp2.Enqueue(rd("d", 3))
	if err != nil || !accepted {
		t.Fatalf("drop-oldest 第3帧应被接受")
	}
	if ev == nil || ev.FrameID != "d:1" {
		t.Fatalf("应丢弃 d:1, got %+v", ev)
	}
	if pend := sp2.Pending(); len(pend) != 2 || pend[0][0].FrameID != "d:2" {
		t.Fatalf("丢弃后 FIFO 应为 d:2,d:3")
	}
	if err := sp2.Ack("d:2"); err != nil {
		t.Fatal(err)
	}
	sp2.Close()

	// 重启后：d:2 已投递（判重），d:3 仍待发
	sp3, err := Open(dir2, 2, PolicyDropOldest)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()
	if !sp3.WasDelivered("d:2") {
		t.Fatal("d:2 应为已投递")
	}
	if pend := sp3.Pending(); len(pend) != 1 || pend[0][0].FrameID != "d:3" {
		t.Fatalf("重启后应只剩 d:3 待发, got %v", pend)
	}
	// 已投递帧重复入队不生效
	if ok, _, _ := sp3.Enqueue([]model.Reading{{FrameID: "d:2"}}); ok {
		t.Fatal("已投递帧不应被重复入队")
	}
}

var _ model.Reading
