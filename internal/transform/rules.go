package transform

import (
	"fmt"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/protocol"
)

const (
	UnitTemperature = "C"   // 标准温度单位：摄氏度
	UnitPressure    = "hPa" // 标准压力单位：百帕
)

// tsFromUnix 解释 modbus 的数值时间戳；v1 单位秒，v2 单位毫秒。
func tsFromUnix(unit string) func(*protocol.DecodedFrame) (time.Time, error) {
	return func(dec *protocol.DecodedFrame) (time.Time, error) {
		if !dec.HasTimestamp {
			return time.Time{}, fmt.Errorf("timestamp field is required")
		}
		switch unit {
		case "s":
			return time.Unix(int64(dec.TimestampRaw), 0).UTC(), nil
		case "ms":
			return time.UnixMilli(int64(dec.TimestampRaw)).UTC(), nil
		default:
			return time.Time{}, fmt.Errorf("unknown timestamp unit %q", unit)
		}
	}
}

func tsFromRFC3339(dec *protocol.DecodedFrame) (time.Time, error) {
	if !dec.HasTimestamp {
		return time.Time{}, fmt.Errorf("timestamp field is required")
	}
	t, err := time.Parse(time.RFC3339, dec.TimestampStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("ts is not RFC3339: %w", err)
	}
	return t.UTC(), nil
}

// DefaultRules 返回本地模拟环境内置的全部已知版本规则。
func DefaultRules() *RuleBook {
	b := NewRuleBook()

	b.Register(&Rule{
		Protocol: "modbus", Version: "1",
		parseTime: tsFromUnix("s"),
		Fields: []FieldRule{
			{SourceKey: "40001", Metric: "temperature", Scale: 0.1, Offset: 0, Unit: UnitTemperature, Min: -40, Max: 125},
			{SourceKey: "40002", Metric: "pressure", Scale: 1, Offset: 0, Unit: UnitPressure, Min: 800, Max: 1200},
		},
	})
	b.Register(&Rule{
		Protocol: "modbus", Version: "2",
		parseTime: tsFromUnix("ms"),
		Fields: []FieldRule{
			{SourceKey: "40001", Metric: "temperature", Scale: 1, Offset: 0, Unit: UnitTemperature, Min: -40, Max: 125},
			// MPa -> hPa: 1 MPa = 10000 hPa
			{SourceKey: "40002", Metric: "pressure", Scale: 10000, Offset: 0, Unit: UnitPressure, Min: 800, Max: 1200},
		},
	})
	b.Register(&Rule{
		Protocol: "opcua", Version: "1",
		parseTime: tsFromRFC3339,
		Fields: []FieldRule{
			// deci°C -> °C
			{SourceKey: "temperature", Metric: "temperature", Scale: 0.1, Offset: 0, Unit: UnitTemperature, Min: -40, Max: 125},
			// Pa -> hPa
			{SourceKey: "pressure", Metric: "pressure", Scale: 0.01, Offset: 0, Unit: UnitPressure, Min: 800, Max: 1200},
		},
	})
	return b
}
