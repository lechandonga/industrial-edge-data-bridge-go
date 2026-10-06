// Package sink 定义归一数据的下游接收端及失败重传转发器。
package sink

import (
	"sync"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// Sink 是下游接收端抽象。返回错误代表“下游不可用/未确认”，数据必须留在本地缓冲。
type Sink interface {
	Send(readings []model.Reading) error
	Name() string
}

// LocalSink 是纯本地模拟接收端：可通过 SetAvailable 制造断网/恢复。
// delivered 按帧指纹去重，即使上游重复推送也只生效一次。
type LocalSink struct {
	mu        sync.Mutex
	name      string
	available bool
	delivered map[string]struct{}
	received  []model.Reading
	failN     int
	// failFirst: 设置为正数时，前 N 次 Send 强制失败，用于模拟瞬时抖动。
	failFirst int
}

func NewLocalSink(name string, available bool) *LocalSink {
	return &LocalSink{name: name, available: available, delivered: map[string]struct{}{}}
}

func (s *LocalSink) Name() string { return s.name }

func (s *LocalSink) SetAvailable(a bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = a
}

// FailNextN 让接下来 n 次发送返回错误（模拟瞬时不可用）。
func (s *LocalSink) FailNextN(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failFirst = n
}

func (s *LocalSink) Send(readings []model.Reading) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failN++
	if s.failFirst > 0 {
		s.failFirst--
		return &ErrUnavailable{Sink: s.name, Reason: "injected transient failure"}
	}
	if !s.available {
		return &ErrUnavailable{Sink: s.name, Reason: "sink is offline"}
	}
	frameID := ""
	if len(readings) > 0 {
		frameID = readings[0].FrameID
	}
	if _, dup := s.delivered[frameID]; dup {
		return nil // 整帧已确认过：幂等返回成功，不重复入库
	}
	for _, r := range readings {
		s.received = append(s.received, r)
	}
	s.delivered[frameID] = struct{}{}
	return nil
}

// Received 返回已确认接收的读数快照。
func (s *LocalSink) Received() []model.Reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Reading, len(s.received))
	copy(out, s.received)
	return out
}

type ErrUnavailable struct {
	Sink   string
	Reason string
}

func (e *ErrUnavailable) Error() string { return "sink " + e.Sink + " unavailable: " + e.Reason }
