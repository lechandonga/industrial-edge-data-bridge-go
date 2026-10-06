// Package spool 是设备断连或下游不可用时的本地持久化缓冲。
// 采用“一条 WAL 记录一个事件”的追加日志：enqueue/deliver/evict 全部落盘，
// 进程重启后按日志重放出“待发送队列 + 已投递指纹集合”，做到不丢不重。
package spool

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// OverflowPolicy 定义缓冲达到容量上限时的行为，任何策略都不允许静默丢数据。
type OverflowPolicy int

const (
	// PolicyBlock 入队直接返回 ErrOverflow，由上游背压（调用方可感知、可重试）。
	PolicyBlock OverflowPolicy = iota
	// PolicyDropOldest 丢弃最旧的一条并记录到 EvictionLog，再放入新数据。
	PolicyDropOldest
)

var ErrOverflow = errors.New("spool buffer overflow (backpressure)")

type opKind string

const (
	opEnqueue opKind = "enqueue"
	opDeliver opKind = "deliver"
	opEvict   opKind = "evict"
)

type walRecord struct {
	Op       opKind          `json:"op"`
	FrameID  string          `json:"frame_id"`
	Readings []model.Reading `json:"readings"`
}

// Eviction 记录因 DropOldest 策略显式丢弃的数据，供审计与指标暴露。
type Eviction struct {
	FrameID  string
	Readings []model.Reading
	Reason   model.RejectReason
	Detail   string
}

// Spool 是并发安全的有界持久化队列。
type Spool struct {
	mu       sync.Mutex
	path     string
	f        *os.File
	w        *bufio.Writer
	capacity int
	policy   OverflowPolicy

	order      []string // 待发送指纹，FIFO
	pending    map[string][]model.Reading
	delivered  map[string]struct{} // 已确认投递，重启后用于幂等去重
	evictions  []Eviction
	enqueued   int64
	deliveredN int64
	evictedN   int64
}

// Open 打开（或重建）缓冲目录。capacity<=0 表示无界（仅用于测试/演示）。
func Open(dir string, capacity int, policy OverflowPolicy) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "spool.wal")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &Spool{
		path: path, f: f, w: bufio.NewWriter(f), capacity: capacity, policy: policy,
		pending: map[string][]model.Reading{}, delivered: map[string]struct{}{},
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Spool) recover() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("corrupt spool wal: %w", err)
		}
		id := rec.FrameID
		switch rec.Op {
		case opEnqueue:
			if _, done := s.delivered[id]; !done {
				if _, queued := s.pending[id]; !queued {
					s.pending[id] = rec.Readings
					s.order = append(s.order, id)
				}
			}
		case opDeliver:
			s.delivered[id] = struct{}{}
			if _, ok := s.pending[id]; ok {
				delete(s.pending, id)
				s.removeFromOrder(id)
			}
		case opEvict:
			if _, ok := s.pending[id]; ok {
				delete(s.pending, id)
				s.removeFromOrder(id)
			}
		}
	}
	return sc.Err()
}

// WasDelivered 判断某指纹是否已经成功投递（含重启前），用于重复上报去重。
func (s *Spool) WasDelivered(frameID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.delivered[frameID]
	return ok
}

// IsQueued 判断指纹是否仍在待发队列（设备重连后重复上报同帧时去重）。
func (s *Spool) IsQueued(frameID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[frameID]
	return ok
}

// Enqueue 原子入队一帧（含一个或多个测点读数）。
// 重复指纹返回 false（不重复入队）；满缓冲按策略处理，绝不静默丢弃。
func (s *Spool) Enqueue(readings []model.Reading) (accepted bool, evicted *Eviction, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := readings[0].FrameID
	if _, done := s.delivered[id]; done {
		return false, nil, nil
	}
	if _, queued := s.pending[id]; queued {
		return false, nil, nil
	}

	if s.capacity > 0 && len(s.order) >= s.capacity {
		switch s.policy {
		case PolicyBlock:
			return false, nil, ErrOverflow
		case PolicyDropOldest:
			oldID := s.order[0]
			old := s.pending[oldID]
			if err := s.appendLocked(walRecord{Op: opEvict, FrameID: oldID, Readings: old}); err != nil {
				return false, nil, err
			}
			delete(s.pending, oldID)
			s.order = s.order[1:]
			ev := Eviction{FrameID: oldID, Readings: old, Reason: model.ReasonBufferOverflow,
				Detail: fmt.Sprintf("buffer capacity %d reached; oldest frame dropped by PolicyDropOldest", s.capacity)}
			s.evictions = append(s.evictions, ev)
			s.evictedN++
			evicted = &ev
		}
	}

	if err := s.appendLocked(walRecord{Op: opEnqueue, FrameID: id, Readings: readings}); err != nil {
		return false, nil, err
	}
	s.pending[id] = readings
	s.order = append(s.order, id)
	s.enqueued++
	return true, evicted, nil
}

// Pending 按 FIFO 返回待发送帧（每帧含一个或多个测点读数）。
func (s *Spool) Pending() [][]model.Reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]model.Reading, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.pending[id])
	}
	return out
}

// Ack 标记整帧已成功投递。
func (s *Spool) Ack(frameID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.delivered[frameID]; ok {
		return nil
	}
	batch := s.pending[frameID]
	if err := s.appendLocked(walRecord{Op: opDeliver, FrameID: frameID, Readings: batch}); err != nil {
		return err
	}
	s.delivered[frameID] = struct{}{}
	if _, ok := s.pending[frameID]; ok {
		delete(s.pending, frameID)
		s.removeFromOrder(frameID)
	}
	s.deliveredN++
	s.maybeCompactLocked()
	return nil
}

func (s *Spool) removeFromOrder(id string) {
	for idx, v := range s.order {
		if v == id {
			s.order = append(s.order[:idx], s.order[idx+1:]...)
			return
		}
	}
}

func (s *Spool) appendLocked(rec walRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

// maybeCompactLocked 当 delivered 集合明显大于待发队列时重写日志，避免无限增长。
func (s *Spool) maybeCompactLocked() {
	if len(s.delivered) < 256 || len(s.delivered) < len(s.pending)*4 {
		return
	}
	tmp := s.path + ".compact"
	tmpF, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return // 压缩失败不影响正确性，下次再试
	}
	bw := bufio.NewWriter(tmpF)
	write := func(rec walRecord) {
		b, _ := json.Marshal(rec)
		b = append(b, '\n')
		bw.Write(b)
	}
	for _, id := range s.order {
		write(walRecord{Op: opEnqueue, FrameID: id, Readings: s.pending[id]})
	}
	// 保留最近 128 个已投递指纹用于重启去重，其余随压缩自然过期。
	// delivered 无顺序信息，简单按字典序截断，保证确定性。
	doneIDs := make([]string, 0, len(s.delivered))
	for id := range s.delivered {
		doneIDs = append(doneIDs, id)
	}
	sort.Strings(doneIDs)
	if len(doneIDs) > 128 {
		doneIDs = doneIDs[len(doneIDs)-128:]
	}
	newDelivered := make(map[string]struct{}, len(doneIDs))
	for _, id := range doneIDs {
		newDelivered[id] = struct{}{}
		// deliver 记录需要携带 reading，重建时只用其 FrameID，故写占位读数。
		write(walRecord{Op: opDeliver, FrameID: id})
	}
	bw.Flush()
	tmpF.Sync()
	tmpF.Close()
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return
	}
	s.f.Close()
	s.f, _ = os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0o644)
	s.w = bufio.NewWriter(s.f)
	s.delivered = newDelivered
}

// Evictions 返回因显式丢弃策略产生的审计记录快照。
func (s *Spool) Evictions() []Eviction {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Eviction, len(s.evictions))
	copy(out, s.evictions)
	return out
}

func (s *Spool) Stats() (queued, enqueued, delivered, evicted int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.order)), s.enqueued, s.deliveredN, s.evictedN
}

func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w.Flush()
	return s.f.Close()
}
