// Package gateway wires collectors, normalization, the durable buffer and the
// downstream forwarder together.
package gateway

import (
	"log"
	"sync"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/buffer"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
)

// Sink is the downstream receiver. Send must fail when downstream is
// unavailable; the forwarder will retry later from the buffer.
type Sink interface {
	Send(batch *model.Batch) error
}

// Gateway accepts raw device payloads, normalizes and buffers them, and
// forwards to the sink with at-least-once, deduplicated delivery.
type Gateway struct {
	registry *protocol.Registry
	buf      *buffer.Buffer
	sink     Sink
	logger   *log.Logger
	mu       sync.Mutex
	rejects  []*model.RejectError
	dropped  int
}

func New(reg *protocol.Registry, buf *buffer.Buffer, sink Sink, logger *log.Logger) *Gateway {
	if logger == nil {
		logger = log.Default()
	}
	return &Gateway{registry: reg, buf: buf, sink: sink, logger: logger}
}

// IngestRaw parses, validates, converts and durably buffers one raw payload.
// Rejections are returned as *model.RejectError and recorded; never silent.
func (g *Gateway) IngestRaw(protocolName string, raw []byte) error {
	batch, err := g.registry.ParseAndNormalize(protocolName, raw)
	if err != nil {
		if re, ok := model.IsReject(err); ok {
			g.mu.Lock()
			g.rejects = append(g.rejects, re)
			g.mu.Unlock()
			g.logger.Printf("REJECT %s", re)
		}
		return err
	}
	res, err := g.buf.Put(batch)
	if err != nil {
		return err
	}
	if res.Duplicate {
		g.logger.Printf("DEDUP batch=%s already seen, skipped", batch.BatchID)
		return nil
	}
	if res.DroppedOldest > 0 {
		g.mu.Lock()
		g.dropped += res.DroppedOldest
		g.mu.Unlock()
		g.logger.Printf("BUFFER_FULL drop-oldest evicted=%d (capacity reached)", res.DroppedOldest)
	}
	return nil
}

// FlushPending forwards pending buffered batches in FIFO order. It stops at
// the first sink failure; unacked batches stay buffered for later retry,
// including after process restarts.
func (g *Gateway) FlushPending() (sent int, err error) {
	for {
		pending := g.buf.Pending(1)
		if len(pending) == 0 {
			return sent, nil
		}
		batch := pending[0]
		if err := g.sink.Send(batch); err != nil {
			g.logger.Printf("DOWNSTREAM_UNAVAILABLE batch=%s err=%v, will retry from buffer", batch.BatchID, err)
			return sent, err
		}
		if err := g.buf.Ack(batch.BatchID); err != nil {
			return sent, err
		}
		sent++
	}
}

// Rejects returns a copy of all recorded rejections.
func (g *Gateway) Rejects() []*model.RejectError {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*model.RejectError(nil), g.rejects...)
}

// DroppedCount returns batches evicted by the drop-oldest policy.
func (g *Gateway) DroppedCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.dropped
}
