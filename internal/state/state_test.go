package state

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
	"time"
)

const T0 = 1_800_000_000.0

// PR builds a power_report line like the Python test helper.
func pr(node int, vin, vout, cur, ts float64, gw int) string {
	b, _ := json.Marshal(map[string]any{
		"event_type": "power_report", "gateway": gw, "node": node,
		"timestamp":  time.Unix(int64(ts), 0).UTC().Format("2006-01-02T15:04:05.999999999+00:00"),
		"voltage_in": vin, "voltage_out": vout, "current": cur,
		"dc_dc_duty_cycle": 1.0, "temperature": 12.3, "rssi": 150,
	})
	return string(b)
}

func prd(node int, ts float64) string { return pr(node, 35.0, 34.5, 8.0, ts, 4609) }

func near(t *testing.T, got, want, tol float64, what string) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s: got %v want %v", what, got, want)
	}
}

func newState(t *testing.T) *State {
	return New(t.TempDir(), 180, map[string]string{"4-A1B2C3D": "East 1", "4609/3": "West 3"})
}

func TestPowerAndEnergy(t *testing.T) {
	s := newState(t)
	s.Handle(prd(2, T0), T0)
	s.Handle(pr(2, 35.0, 34.5, 10.0, T0+60, 4609), T0+60)
	snap := s.Snapshot(T0 + 61)
	n := snap.Nodes[0]
	if !n.Online {
		t.Fatal("expected online")
	}
	near(t, n.Power, 345.0, 1e-7, "power")
	near(t, n.Values["current_in"], 345.0/35.0, 1e-7, "current_in")
	// trapezoid: (276 + 345)/2 W for 60 s
	near(t, n.EnergyWh, (276+345)/2.0/60, 1e-6, "energy")
	near(t, snap.ArrayPower, 345.0, 1e-7, "array power")
	if n.Name != "Node 4609/2" {
		t.Fatalf("name %q", n.Name)
	}
}

func TestGapNotIntegratedAndOffline(t *testing.T) {
	s := newState(t)
	s.Handle(prd(2, T0), T0)
	s.Handle(prd(2, T0+3600), T0+3600)
	if e := s.Snapshot(T0 + 3600).Nodes[0].EnergyWh; e != 0.0 {
		t.Fatalf("energy %v", e)
	}
	snap := s.Snapshot(T0 + 3600 + 181)
	if snap.Nodes[0].Online {
		t.Fatal("expected offline")
	}
	if snap.ArrayPower != 0.0 {
		t.Fatalf("array power %v", snap.ArrayPower)
	}
	if len(snap.Nodes[0].Values) != 0 {
		t.Fatalf("values %v", snap.Nodes[0].Values)
	}
}

func TestClockSkewUsesArrival(t *testing.T) {
	s := newState(t)
	s.Handle(prd(5, T0+86400), T0)
	if ts := s.Snapshot(T0).Nodes[0].TS; ts != T0 {
		t.Fatalf("ts %v", ts)
	}
	// too old is also replaced by arrival time
	s2 := newState(t)
	s2.Handle(prd(5, T0-601), T0)
	if ts := s2.Snapshot(T0).Nodes[0].TS; ts != T0 {
		t.Fatalf("old ts %v", ts)
	}
}

func TestInfrastructureNamesAndBad(t *testing.T) {
	s := newState(t)
	s.Handle(prd(3, T0), T0)
	s.Handle(`{"event_type":"infrastructure_report",
		"gateways":{"4609":{"address":"04:C0:5B:30:00:01:02:03","version":"x"}},
		"nodes":{"4609":{"2":{"address":"04:C0:5B:40:00:A1:B2:C3","barcode":"4-A1B2C3D"}}}}`, T0)
	s.Handle(`{"event_type":"power_report","gateway":1}`, T0)
	s.Handle("garbage", T0)
	snap := s.Snapshot(T0)
	names := map[string]string{}
	for _, x := range snap.Nodes {
		names[x.Key] = x.Name
	}
	want := map[string]string{"4609/2": "East 1", "4609/3": "West 3"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("names %v", names)
	}
	if snap.Events != (Events{PowerReport: 1, InfrastructureReport: 1, Other: 0, Bad: 1}) {
		t.Fatalf("events %+v", snap.Events)
	}
	if snap.Gateways["4609"].Version != "x" {
		t.Fatalf("gateways %+v", snap.Gateways)
	}
}

func TestArrayEnergyFallsToZero(t *testing.T) {
	s := newState(t)
	s.Handle(prd(2, T0), T0)
	s.Tick(T0 + 100)
	s.Tick(T0 + 200) // node stale at +181: array power -> 0
	near(t, s.arrayEnergy, 276*100.0/3600+276*100.0/3600/2*1, 1e-6, "array energy")
}

func TestEnergyPersist(t *testing.T) {
	s := newState(t)
	s.Handle(prd(2, T0), T0)
	s.Handle(prd(2, T0+60), T0+60)
	if err := s.SaveEnergy(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.energyFile + ".tmp"); err == nil {
		t.Fatal("tmp file left behind")
	}
	dir := s.energyFile[:len(s.energyFile)-len("/energy.json")]
	s2 := New(dir, 180, nil)
	near(t, s2.nodes["4609/2"].energyWh, s.nodes["4609/2"].energyWh, 1e-7, "loaded energy")
	s2.Handle(prd(2, T0+120), T0+120) // first report after restart: no integration
	near(t, s2.nodes["4609/2"].energyWh, s.nodes["4609/2"].energyWh, 1e-7, "energy after restart")
}

func TestParseNames(t *testing.T) {
	got := ParseNames(" A = x ,B=y, bad, =z,C=")
	if !reflect.DeepEqual(got, map[string]string{"A": "x", "B": "y"}) {
		t.Fatalf("%v", got)
	}
}

func TestChangedFlag(t *testing.T) {
	s := newState(t)
	if s.TakeChanged() {
		t.Fatal("unexpected")
	}
	s.Handle(prd(2, T0), T0) // new node
	if !s.TakeChanged() || s.TakeChanged() {
		t.Fatal("flag semantics")
	}
	s.Handle(prd(2, T0+10), T0+10) // same node: no change
	if s.TakeChanged() {
		t.Fatal("unexpected change")
	}
}

func TestParseTS(t *testing.T) {
	for _, in := range []string{"2027-01-15T08:00:00Z", "2027-01-15T08:00:00+00:00", "2027-01-15T08:00:00.123456789Z"} {
		if _, ok := parseTS(in); !ok {
			t.Errorf("%s not parsed", in)
		}
	}
	for _, in := range []any{"nope", nil, 5.0} {
		if _, ok := parseTS(in); ok {
			t.Errorf("%v parsed", in)
		}
	}
}
