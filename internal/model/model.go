// Package model defines the unified data model and distinguishable reject reasons.
package model

import (
	"errors"
	"fmt"
	"time"
)

// RejectReason identifies why a record was rejected.
type RejectReason string

const (
	ReasonMissingField        RejectReason = "missing_field"
	ReasonBadFieldType        RejectReason = "bad_field_type"
	ReasonTimestampOutOfRange RejectReason = "timestamp_out_of_range"
	ReasonUnknownVersion      RejectReason = "unknown_version"
	ReasonMissingRule         RejectReason = "missing_rule"
	ReasonValueOutOfRange     RejectReason = "value_out_of_range"
)

// RejectError carries the reject reason plus traceability context.
type RejectError struct {
	Reason   RejectReason
	Field    string
	Detail   string
	BatchID  string
	Protocol string
	Version  string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("reject[%s] protocol=%s version=%s batch=%s field=%q: %s",
		e.Reason, e.Protocol, e.Version, e.BatchID, e.Field, e.Detail)
}

// IsReject extracts a RejectError from err if present.
func IsReject(err error) (*RejectError, bool) {
	var re *RejectError
	if errors.As(err, &re) {
		return re, true
	}
	return nil, false
}

// Point is one normalized data point.
type Point struct {
	DeviceID  string
	Metric    string
	Value     float64
	Unit      string
	Timestamp time.Time
}

// Batch is a normalized batch, keeping the raw payload for traceability.
type Batch struct {
	BatchID  string
	Protocol string
	Version  string
	Raw      []byte
	Points   []Point
}

var (
	minTimestamp = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	maxSkew      = 24 * time.Hour
)

// ValidatePoint validates one point, returning a distinguishable reject reason.
func ValidatePoint(p Point, b *Batch) error {
	mk := func(reason RejectReason, field, detail string) *RejectError {
		return &RejectError{Reason: reason, Field: field, Detail: detail,
			BatchID: b.BatchID, Protocol: b.Protocol, Version: b.Version}
	}
	if p.DeviceID == "" {
		return mk(ReasonMissingField, "device_id", "device id is empty")
	}
	if p.Metric == "" {
		return mk(ReasonMissingField, "metric", "metric name is empty")
	}
	if p.Timestamp.IsZero() {
		return mk(ReasonMissingField, "timestamp", "timestamp is zero")
	}
	if p.Timestamp.Before(minTimestamp) || p.Timestamp.After(time.Now().Add(maxSkew)) {
		return mk(ReasonTimestampOutOfRange, "timestamp",
			fmt.Sprintf("timestamp %s outside [%s, now+%s]", p.Timestamp.UTC(), minTimestamp, maxSkew))
	}
	return nil
}
