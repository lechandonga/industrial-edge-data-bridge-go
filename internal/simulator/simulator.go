// Package simulator 提供纯本地的模拟设备：产出符合各协议格式的原始报文，
// 并支持断连、重连、重放上一帧（重复上报）等故障注入。
package simulator

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lechandonga/industrial-edge-data-bridge-go/internal/model"
)

// Device 模拟单台设备。Handler 是采集网关侧的入帧回调。
type Device struct {
	mu       sync.Mutex
	id       string
	protocol string
	version  string
	seq      uint64
	online   bool
	lastRaw  []byte
	now      func() time.Time
}

func New(id, proto, version string, now func() time.Time) *Device {
	if now == nil {
		now = time.Now
	}
	return &Device{id: id, protocol: proto, version: version, online: true, now: now}
}

func (d *Device) ID() string   { return d.id }
func (d *Device) Online() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.online }
func (d *Device) Disconnect()  { d.mu.Lock(); d.online = false; d.mu.Unlock() }
func (d *Device) Reconnect()   { d.mu.Lock(); d.online = true; d.mu.Unlock() }

func (d *Device) nextFrame(raw []byte) model.RawFrame {
	d.seq++
	d.lastRaw = append([]byte(nil), raw...)
	return model.RawFrame{
		Protocol: d.protocol, Version: d.version, DeviceID: d.id,
		Seq: d.seq, ReceivedAt: d.now(), Raw: raw,
	}
}

// Poll 返回一帧新采样；设备离线时返回 ok=false（采集端表现为断连，不产生数据）。
func (d *Device) Poll(payload map[string]any) (frame model.RawFrame, ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.online {
		return model.RawFrame{}, false
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	return d.nextFrame(raw), true
}

// ReplayLast 重放最近一次采样的原始字节（相同序号），模拟设备重连后的重复上报。
func (d *Device) ReplayLast() (model.RawFrame, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.online || d.lastRaw == nil {
		return model.RawFrame{}, false
	}
	return model.RawFrame{
		Protocol: d.protocol, Version: d.version, DeviceID: d.id,
		Seq: d.seq, ReceivedAt: d.now(), Raw: append([]byte(nil), d.lastRaw...),
	}, true
}

// Raw 构造一条裸帧，用于直接注入畸形/版本异常报文。
func Raw(id, proto, version string, seq uint64, raw []byte, now time.Time) model.RawFrame {
	return model.RawFrame{Protocol: proto, Version: version, DeviceID: id, Seq: seq, ReceivedAt: now, Raw: raw}
}

// 以下是构造各类合法/非法载荷的便捷函数。

func ModbusV1Payload(tempDeciC, pressureKPa float64, tsSec int64) map[string]any {
	return map[string]any{
		"device_id": "", // 由外层帧标识设备，payload 内可为空；解码器要求字段存在时才检查
		"ts":        json.Number(fmt.Sprintf("%d", tsSec)),
		"regs": map[string]any{
			"40001": json.Number(fmt.Sprintf("%g", tempDeciC)),
			"40002": json.Number(fmt.Sprintf("%g", pressureKPa)),
		},
	}
}

// ModbusV1PayloadWithDevice 解码器要求 payload.device_id 非空，正式构造使用此函数。
func ModbusV1PayloadWithDevice(deviceID string, tempDeciC, pressureKPa float64, tsSec int64) map[string]any {
	p := ModbusV1Payload(tempDeciC, pressureKPa, tsSec)
	p["device_id"] = deviceID
	return p
}

func ModbusV2PayloadWithDevice(deviceID string, tempC, pressureMPa float64, tsMs int64) map[string]any {
	return map[string]any{
		"device_id": deviceID,
		"ts":        json.Number(fmt.Sprintf("%d", tsMs)),
		"regs": map[string]any{
			"40001": json.Number(fmt.Sprintf("%g", tempC)),
			"40002": json.Number(fmt.Sprintf("%g", pressureMPa)),
		},
	}
}

func OPCUAV1PayloadWithDevice(deviceID, tempDeciC, pressurePa, tsRFC3339 string) map[string]any {
	return map[string]any{
		"device_id": deviceID,
		"ts":        tsRFC3339,
		"nodes": map[string]any{
			"temperature": map[string]any{"value": json.Number(tempDeciC), "quality": "GOOD"},
			"pressure":    map[string]any{"value": json.Number(pressurePa), "quality": "GOOD"},
		},
	}
}
