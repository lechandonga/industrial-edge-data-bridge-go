// Package buffer provides a durable, deduplicating, bounded local buffer.
// Entries survive process restarts (WAL file on disk). When full, the
// configured policy applies explicitly: backpressure (error) or drop-oldest
// (reported, never silent).
package buffer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// ErrBufferFull is returned by Put under PolicyBackpressure when at capacity.
var ErrBufferFull = errors.New("buffer full: apply backpressure upstream")

// FullPolicy decides what happens when the buffer reaches capacity.
type FullPolicy int

const (
	// PolicyBackpressure rejects new entries with ErrBufferFull.
	PolicyBackpressure FullPolicy = iota
	// PolicyDropOldest evicts the oldest pending entry; the drop is returned
	// via Put's Dropped result so callers can log/alert. Never silent.
	PolicyDropOldest
)

// PutResult reports the outcome of a Put call.
type PutResult struct {
	Duplicate     bool // batch id already known; not stored again
	DroppedOldest int  // entries evicted to make room (PolicyDropOldest)
}

type walRecord struct {
	Op    string       `json:"op"` // "put" or "ack"
	Batch *model.Batch `json:"batch,omitempty"`
	ID    string       `json:"id,omitempty"`
}

// Buffer is a thread-safe, durable FIFO with dedup by BatchID.
type Buffer struct {
	mu       sync.Mutex
	path     string
	wal      *os.File
	capacity int
	policy   FullPolicy
	order    []string          // pending batch ids, FIFO
	entries  map[string]*model.Batch
	seen     map[string]bool // includes acked ids for dedup across replays
}

// Open creates or recovers a buffer stored in dir. Recovery replays the WAL:
// pending entries are restored, acked ids stay in the dedup set so already
// delivered batches are never re-sent after a restart.
func Open(dir string, capacity int, policy FullPolicy) (*Buffer, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be > 0")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	b := &Buffer{
		path:     filepath.Join(dir, "buffer.wal"),
		capacity: capacity,
		policy:   policy,
		entries:  map[string]*model.Batch{},
		seen:     map[string]bool{},
	}
	if err := b.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(b.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	b.wal = f
	return b, nil
}

func (b *Buffer) recover() error {
	f, err := os.Open(b.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("corrupt WAL record: %w", err)
		}
		switch rec.Op {
		case "put":
			if b.seen[rec.Batch.BatchID] {
				continue
			}
			b.seen[rec.Batch.BatchID] = true
			b.entries[rec.Batch.BatchID] = rec.Batch
			b.order = append(b.order, rec.Batch.BatchID)
		case "ack":
			if _, ok := b.entries[rec.ID]; ok {
				delete(b.entries, rec.ID)
				for i, id := range b.order {
					if id == rec.ID {
						b.order = append(b.order[:i], b.order[i+1:]...)
						break
					}
				}
			}
		}
	}
	return sc.Err()
}

func (b *Buffer) appendWAL(rec walRecord) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := b.wal.Write(append(data, '\n')); err != nil {
		return err
	}
	return b.wal.Sync()
}

// Put stores a batch durably. Duplicates (by BatchID) are acknowledged as
// no-ops, so repeated reports never create duplicates downstream.
func (b *Buffer) Put(batch *model.Batch) (PutResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.seen[batch.BatchID] {
		return PutResult{Duplicate: true}, nil
	}
	var res PutResult
	for len(b.order) >= b.capacity {
		if b.policy == PolicyBackpressure {
			return res, ErrBufferFull
		}
		oldest := b.order[0]
		if err := b.appendWAL(walRecord{Op: "ack", ID: oldest}); err != nil {
			return res, err
		}
		b.order = b.order[1:]
		delete(b.entries, oldest)
		res.DroppedOldest++
	}
	if err := b.appendWAL(walRecord{Op: "put", Batch: batch}); err != nil {
		return res, err
	}
	b.seen[batch.BatchID] = true
	b.entries[batch.BatchID] = batch
	b.order = append(b.order, batch.BatchID)
	return res, nil
}

// Pending returns up to n oldest pending batches in FIFO order.
func (b *Buffer) Pending(n int) []*model.Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > len(b.order) {
		n = len(b.order)
	}
	out := make([]*model.Batch, 0, n)
	for _, id := range b.order[:n] {
		out = append(out, b.entries[id])
	}
	return out
}

// Ack marks a batch as delivered; it will not be replayed again.
func (b *Buffer) Ack(batchID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.entries[batchID]; !ok {
		return nil
	}
	if err := b.appendWAL(walRecord{Op: "ack", ID: batchID}); err != nil {
		return err
	}
	delete(b.entries, batchID)
	for i, id := range b.order {
		if id == batchID {
			b.order = append(b.order[:i], b.order[i+1:]...)
			break
		}
	}
	return nil
}

// Len returns the number of pending (not yet acked) batches.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.order)
}

// Close flushes and closes the WAL file.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.wal == nil {
		return nil
	}
	err := b.wal.Close()
	b.wal = nil
	return err
}
