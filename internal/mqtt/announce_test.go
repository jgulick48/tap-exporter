package mqtt

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jgulick48/tap-exporter/internal/state"
)

const T0 = 1_800_000_000.0

type fakePub struct{ pub map[string]string }

func (f *fakePub) Publish(topic, payload string, qos byte, retain bool) { f.pub[topic] = payload }

func pr(node int, ts float64) string {
	b, _ := json.Marshal(map[string]any{
		"event_type": "power_report", "gateway": 4609, "node": node,
		"timestamp":  time.Unix(int64(ts), 0).UTC().Format(time.RFC3339),
		"voltage_in": 35.0, "voltage_out": 34.5, "current": 8.0,
		"dc_dc_duty_cycle": 1.0, "temperature": 12.3, "rssi": 150,
	})
	return string(b)
}

func TestAnnounceAndPublish(t *testing.T) {
	s := state.New(t.TempDir(), 180, nil, nil)
	s.Handle(pr(2, T0), T0)
	f := &fakePub{map[string]string{}}
	m := NewAnnouncer(f, "tigo", "homeassistant")
	snap := s.Snapshot(T0)
	m.Announce(snap)
	m.Publish(snap)

	var cfg map[string]any
	if err := json.Unmarshal([]byte(f.pub["homeassistant/sensor/tigo_g4609n2_power/config"]), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg["state_topic"] != "tigo/node/g4609n2" {
		t.Fatalf("state_topic %v", cfg["state_topic"])
	}
	if cfg["device"].(map[string]any)["via_device"] != "tigo_tap" {
		t.Fatalf("via_device %v", cfg["device"])
	}
	if cfg["availability_topic"] != "tigo/status" || cfg["suggested_display_precision"] != 0.0 {
		t.Fatalf("cfg %v", cfg)
	}
	var st map[string]any
	if err := json.Unmarshal([]byte(f.pub["tigo/node/g4609n2"]), &st); err != nil {
		t.Fatal(err)
	}
	if st["power"] != 276.0 {
		t.Fatalf("power %v", st["power"])
	}
	if st["rssi"] != 150.0 {
		t.Fatalf("rssi %v", st["rssi"])
	}
	if st["barcode"] != nil || st["online"] != true {
		t.Fatalf("payload %v", st)
	}
	var arr map[string]any
	if err := json.Unmarshal([]byte(f.pub["tigo/array"]), &arr); err != nil {
		t.Fatal(err)
	}
	if arr["online"] != 1.0 {
		t.Fatalf("online %v", arr["online"])
	}
	if _, ok := f.pub["homeassistant/sensor/tigo_array_energy/config"]; !ok {
		t.Fatal("array energy config missing")
	}
}

func TestReannounceOnlyOnReset(t *testing.T) {
	s := state.New(t.TempDir(), 180, nil, nil)
	s.Handle(pr(2, T0), T0)
	f := &fakePub{map[string]string{}}
	m := NewAnnouncer(f, "tigo", "homeassistant")
	snap := s.Snapshot(T0)
	m.Announce(snap)
	delete(f.pub, "homeassistant/sensor/tigo_array_power/config")
	m.Announce(snap)
	if len(f.pub["homeassistant/sensor/tigo_array_power/config"]) != 0 {
		t.Fatal("re-announced without reset")
	}
	m.Reset()
	m.Announce(snap)
	if _, ok := f.pub["homeassistant/sensor/tigo_array_power/config"]; !ok {
		t.Fatal("not re-announced after reset")
	}
}

func TestRenameReannounces(t *testing.T) {
	s := state.New(t.TempDir(), 180, map[string]string{"X-1": "Roof"}, nil)
	s.Handle(pr(2, T0), T0)
	f := &fakePub{map[string]string{}}
	m := NewAnnouncer(f, "tigo", "homeassistant")
	m.Announce(s.Snapshot(T0))
	s.Handle(`{"event_type":"infrastructure_report","nodes":{"4609":{"2":{"barcode":"X-1"}}}}`, T0)
	m.Announce(s.Snapshot(T0))
	var cfg struct {
		Device struct {
			Name   string `json:"name"`
			Serial string `json:"serial_number"`
		} `json:"device"`
	}
	json.Unmarshal([]byte(f.pub["homeassistant/sensor/tigo_g4609n2_power/config"]), &cfg)
	if cfg.Device.Name != "Tigo Roof" || cfg.Device.Serial != "X-1" {
		t.Fatalf("device %+v", cfg.Device)
	}
}

func TestRoundLikePython(t *testing.T) {
	cases := []struct {
		x    float64
		nd   int
		want float64
	}{{0.125, 2, 0.12}, {2.5, 0, 2}, {3.5, 0, 4}, {276.04, 1, 276.0}, {0.34567, 3, 0.346}, {-1.25, 1, -1.2}}
	for _, c := range cases {
		if got := round(c.x, c.nd); math.Abs(got-c.want) > 1e-12 {
			t.Errorf("round(%v,%d)=%v want %v", c.x, c.nd, got, c.want)
		}
	}
}

func TestIsoLocal(t *testing.T) {
	got := isoLocal(T0)
	if len(got) != 25 { // 2027-01-15T08:00:00+hh:mm
		t.Fatalf("iso %q", got)
	}
	if !strings.Contains(isoLocal(T0+0.5), ".500000") {
		t.Fatal("fraction")
	}
}

func TestTLSConfigServerName(t *testing.T) {
	c := Config{Host: "mqtt.svc", Port: 8883, ServerName: "broker.example.com"}
	if !c.UseTLS() {
		t.Fatal("expected TLS on 8883")
	}
	tc, err := c.TLSConfig()
	if err != nil || tc.ServerName != "broker.example.com" {
		t.Fatalf("%v %v", tc, err)
	}
	if (Config{Port: 1883}).UseTLS() {
		t.Fatal("unexpected TLS")
	}
}

func TestAnnounceGainAndOnline(t *testing.T) {
	s := state.New(t.TempDir(), 180, nil, nil)
	s.Handle(pr(2, T0), T0)
	f := &fakePub{map[string]string{}}
	m := NewAnnouncer(f, "tigo", "homeassistant")
	snap := s.Snapshot(T0)
	m.Announce(snap)
	m.Publish(snap)

	get := func(topic string) map[string]any {
		t.Helper()
		raw, ok := f.pub[topic]
		if !ok {
			t.Fatalf("%s not published", topic)
		}
		var v map[string]any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	b := get("homeassistant/binary_sensor/tigo_g4609n2_online/config")
	for k, want := range map[string]any{
		"name": "Online", "unique_id": "tigo_g4609n2_online", "object_id": "tigo_g4609n2_online",
		"state_topic": "tigo/node/g4609n2", "value_template": "{{ 'ON' if value_json.online else 'OFF' }}",
		"availability_topic": "tigo/status", "device_class": "connectivity", "entity_category": "diagnostic",
	} {
		if b[k] != want {
			t.Errorf("binary_sensor %s = %v, want %v", k, b[k], want)
		}
	}
	if b["device"].(map[string]any)["identifiers"].([]any)[0] != "tigo_g4609n2" {
		t.Errorf("device %v", b["device"])
	}
	for _, c := range []struct {
		topic, tmpl, unit, dclass, sclass string
		prec                              float64
	}{
		{"tigo_array_gain_power", "{{ value_json.gain_power }}", "W", "power", "measurement", 0},
		{"tigo_array_gain_energy", "{{ value_json.gain_energy_kwh }}", "kWh", "energy", "total_increasing", 2},
		{"tigo_array_unoptimized_power", "{{ value_json.unoptimized_power }}", "W", "power", "measurement", 0},
		{"tigo_g4609n2_gain_power", "{{ value_json.gain_power }}", "W", "power", "measurement", 0},
		{"tigo_g4609n2_gain_energy", "{{ value_json.gain_energy_kwh }}", "kWh", "energy", "total_increasing", 3},
	} {
		cfg := get("homeassistant/sensor/" + c.topic + "/config")
		if cfg["value_template"] != c.tmpl || cfg["unit_of_measurement"] != c.unit || cfg["device_class"] != c.dclass ||
			cfg["state_class"] != c.sclass || cfg["suggested_display_precision"] != c.prec {
			t.Errorf("%s: %v", c.topic, cfg)
		}
	}
	if get("homeassistant/sensor/tigo_array_gain_power/config")["state_topic"] != "tigo/array" {
		t.Error("array gain state topic")
	}

	node := get("tigo/node/g4609n2")
	if node["gain_power"] != 0.0 || node["gain_energy_kwh"] != 0.0 || node["string"] != "all" {
		t.Errorf("node payload %v", node)
	}
	arr := get("tigo/array")
	if arr["unoptimized_power"] != 276.0 || arr["gain_power"] != 0.0 || arr["gain_energy_kwh"] != 0.0 {
		t.Errorf("array payload %v", arr)
	}
}
