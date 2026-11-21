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
	s := state.New(t.TempDir(), 180, nil)
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
	s := state.New(t.TempDir(), 180, nil)
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
	s := state.New(t.TempDir(), 180, map[string]string{"X-1": "Roof"})
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
