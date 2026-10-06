// Package sim provides a local simulated device and downstream receiver so
// the whole pipeline is reproducible without hardware or network.
package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// Device simulates a field device that can go offline and re-report batches.
type Device struct {
	ID        string
	Protocol  string
	Version   string
	online    atomic.Bool
	seq       atomic.Int64
	Fields    map[string]float64
	Timestamp int64 // unix millis; set by test
}

func NewDevice(id, protocol, version string, fields map[string]float64, tsMillis int64) *Device {
	d := &Device{ID: id, Protocol: protocol, Version: version, Fields: fields, Timestamp: tsMillis}
	d.online.Store(true)
	return d
}

func (d *Device) SetOnline(online bool) { d.online.Store(online) }

// Report builds one raw payload. batchID may be empty to auto-generate;
// passing an explicit id simulates a device re-reporting the same batch.
func (d *Device) Report(batchID string) ([]byte, error) {
	if !d.online.Load() {
		return nil, fmt.Errorf("device %s offline", d.ID)
	}
	if batchID == "" {
		batchID = fmt.Sprintf("%s-%d", d.ID, d.seq.Add(1))
	}
	fields, err := json.Marshal(d.Fields)
	if err != nil {
		return nil, err
	}
	payload := fmt.Sprintf(`{"batch_id":%q,"device_id":%q,"version":%q,"ts":%d,"fields":%s}`,
		batchID, d.ID, d.Version, d.Timestamp, fields)
	return []byte(payload), nil
}

// Sink simulates a downstream receiver with controllable availability and a
// dedup-checked delivery log.
type Sink struct {
	mu        sync.Mutex
	available bool
	received  []*model.Batch
	seenIDs   map[string]int
}

func NewSink() *Sink {
	return &Sink{available: true, seenIDs: map[string]int{}}
}

func (s *Sink) SetAvailable(avail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.available = avail
}

var ErrUnavailable = errors.New("downstream unavailable")

func (s *Sink) Send(batch *model.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.available {
		return ErrUnavailable
	}
	s.seenIDs[batch.BatchID]++
	s.received = append(s.received, batch)
	return nil
}

// Received returns a copy of delivered batches in delivery order.
func (s *Sink) Received() []*model.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*model.Batch(nil), s.received...)
}

// DuplicateCount reports how many batches were delivered more than once.
func (s *Sink) DuplicateCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.seenIDs {
		if c > 1 {
			n++
		}
	}
	return n
}
