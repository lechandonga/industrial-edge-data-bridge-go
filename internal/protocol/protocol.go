// Package protocol parses raw device payloads and normalizes them into the
// unified model using explicit per-(protocol, version) conversion rules.
package protocol

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// FieldRule describes how to normalize one metric for a protocol version.
type FieldRule struct {
	Metric string  // normalized metric name
	Scale  float64 // multiply raw value by this
	Offset float64 // then add this (unit conversion, e.g. F->C)
	Unit   string  // normalized unit after conversion
	Min    float64 // normalized valid range
	Max    float64
}

// VersionRule binds a protocol version to its field rules.
type VersionRule struct {
	Protocol string
	Version  string
	Fields   map[string]FieldRule // keyed by raw field name
}

// Registry holds all known conversion rules. Unknown versions are rejected,
// never guessed.
type Registry struct {
	rules map[string]VersionRule // key: protocol + "@" + version
}

func NewRegistry() *Registry {
	return &Registry{rules: map[string]VersionRule{}}
}

func (r *Registry) Register(vr VersionRule) {
	r.rules[vr.Protocol+"@"+vr.Version] = vr
}

func (r *Registry) lookup(protocol, version string) (VersionRule, bool) {
	vr, ok := r.rules[protocol+"@"+version]
	return vr, ok
}

// rawMessage is the on-the-wire JSON format used by the simulated devices.
type rawMessage struct {
	BatchID  string          `json:"batch_id"`
	DeviceID string          `json:"device_id"`
	Version  string          `json:"version"`
	TS       int64           `json:"ts"` // unix millis
	Fields   json.RawMessage `json:"fields"`
}

// ParseAndNormalize parses a raw payload for the given protocol and converts
// every field per the registered rules. Any anomaly returns a *model.RejectError.
func (r *Registry) ParseAndNormalize(protocolName string, raw []byte) (*model.Batch, error) {
	reject := func(reason model.RejectReason, field, detail string) error {
		return &model.RejectError{Reason: reason, Field: field, Detail: detail, Protocol: protocolName}
	}

	var msg rawMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, reject(model.ReasonBadFieldType, "payload", "not valid JSON: "+err.Error())
	}
	if msg.BatchID == "" {
		return nil, reject(model.ReasonMissingField, "batch_id", "batch id is empty")
	}
	if msg.DeviceID == "" {
		return nil, reject(model.ReasonMissingField, "device_id", "device id is empty")
	}
	if msg.Version == "" {
		return nil, reject(model.ReasonMissingField, "version", "version is empty")
	}

	batch := &model.Batch{
		BatchID:  msg.BatchID,
		Protocol: protocolName,
		Version:  msg.Version,
		Raw:      append([]byte(nil), raw...),
	}

	vr, ok := r.lookup(protocolName, msg.Version)
	if !ok {
		return nil, &model.RejectError{Reason: model.ReasonUnknownVersion, Field: "version",
			Detail: fmt.Sprintf("no rules registered for %s@%s", protocolName, msg.Version),
			BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(msg.Fields, &fields); err != nil {
		return nil, &model.RejectError{Reason: model.ReasonBadFieldType, Field: "fields",
			Detail: "fields is not an object", BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
	}

	ts := time.UnixMilli(msg.TS).UTC()
	for rawName, rule := range vr.Fields {
		rawVal, present := fields[rawName]
		if !present {
			return nil, &model.RejectError{Reason: model.ReasonMissingField, Field: rawName,
				Detail: "required field missing from payload",
				BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
		}
		var num float64
		if err := json.Unmarshal(rawVal, &num); err != nil {
			return nil, &model.RejectError{Reason: model.ReasonBadFieldType, Field: rawName,
				Detail: fmt.Sprintf("expected number, got %s", string(rawVal)),
				BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
		}
		value := num*rule.Scale + rule.Offset
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return nil, &model.RejectError{Reason: model.ReasonBadFieldType, Field: rawName,
				Detail: "normalized value is not finite",
				BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
		}
		if value < rule.Min || value > rule.Max {
			return nil, &model.RejectError{Reason: model.ReasonValueOutOfRange, Field: rawName,
				Detail: fmt.Sprintf("normalized value %v outside [%v, %v]", value, rule.Min, rule.Max),
				BatchID: msg.BatchID, Protocol: protocolName, Version: msg.Version}
		}
		p := model.Point{
			DeviceID:  msg.DeviceID,
			Metric:    rule.Metric,
			Value:     value,
			Unit:      rule.Unit,
			Timestamp: ts,
		}
		if err := model.ValidatePoint(p, batch); err != nil {
			return nil, err
		}
		batch.Points = append(batch.Points, p)
	}
	return batch, nil
}

// DefaultRegistry returns a registry with the built-in simulated protocols:
//
//   - "modsim" v1: temp = raw int * 0.1 (C), pressure = raw * 0.01 (kPa)
//   - "modsim" v2: temp reported in Fahrenheit -> Celsius, pressure in psi -> kPa
//   - "jsonline" v1: temp already Celsius, humidity percent
func DefaultRegistry() *Registry {
	r := NewRegistry()
	r.Register(VersionRule{
		Protocol: "modsim", Version: "v1",
		Fields: map[string]FieldRule{
			"temp":     {Metric: "temperature", Scale: 0.1, Unit: "celsius", Min: -40, Max: 120},
			"pressure": {Metric: "pressure", Scale: 0.01, Unit: "kpa", Min: 0, Max: 1000},
		},
	})
	r.Register(VersionRule{
		Protocol: "modsim", Version: "v2",
		Fields: map[string]FieldRule{
			// Fahrenheit -> Celsius: (F - 32) / 1.8
			"temp":     {Metric: "temperature", Scale: 1.0 / 1.8, Offset: -32.0 / 1.8, Unit: "celsius", Min: -40, Max: 120},
			"pressure": {Metric: "pressure", Scale: 6.894757, Unit: "kpa", Min: 0, Max: 1000},
		},
	})
	r.Register(VersionRule{
		Protocol: "jsonline", Version: "v1",
		Fields: map[string]FieldRule{
			"temp":     {Metric: "temperature", Scale: 1, Unit: "celsius", Min: -40, Max: 120},
			"humidity": {Metric: "humidity", Scale: 1, Unit: "percent", Min: 0, Max: 100},
		},
	})
	return r
}
