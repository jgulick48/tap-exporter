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
	return New(t.TempDir(), 180, map[string]string{"4-A1B2C3D": "East 1", "4609/3": "West 3"}, nil)
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
	s2 := New(dir, 180, nil, nil)
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

func TestParseStrings(t *testing.T) {
	got := ParseStrings(" East = 4-A, 4609/2 ;; West=4609/3,, ; bad ; =x; Empty= ")
	want := []StringDef{{"East", []string{"4-A", "4609/2"}}, {"West", []string{"4609/3"}}, {"Empty", nil}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v", got)
	}
	if len(ParseStrings("")) != 0 {
		t.Fatal("empty spec")
	}
}

// panels feeds one report per (V, I) pair at T0: vout = V, current = I, so
// output power is V*I and current_in = I.
func feed(s *State, ts float64, panels ...[2]float64) {
	for i, p := range panels {
		s.Handle(pr(i+1, p[0], p[0], p[1], ts, 4609), ts)
	}
}

func TestGainModel(t *testing.T) {
	cases := []struct {
		name      string
		panels    [][2]float64
		unopt     float64
		gain      float64
		current   float64
		nodeGains []float64
	}{
		{"unshaded", [][2]float64{{35, 8}, {35, 8}, {35, 8}}, 840, 0, 8, []float64{0, 0, 0}},
		{"one shaded", [][2]float64{{35, 8}, {35, 8}, {35, 3}}, 560, 105, 8, []float64{0, 0, 105}},
		{"mild mismatch", [][2]float64{{35, 8}, {35, 8}, {35, 7.5}}, 787.5, 35, 7.5, []float64{17.5, 17.5, 0}},
		{"no current", [][2]float64{{35, 0}}, 0, 0, 0, []float64{0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := New(t.TempDir(), 180, nil, nil)
			feed(s, T0, c.panels...)
			snap := s.Snapshot(T0)
			near(t, snap.ArrayUnoptPower, c.unopt, 1e-6, "unopt")
			near(t, snap.ArrayGainPower, c.gain, 1e-6, "gain")
			if len(snap.Strings) != 1 || snap.Strings[0].Name != "all" || snap.Strings[0].Nodes != len(c.panels) {
				t.Fatalf("strings %+v", snap.Strings)
			}
			near(t, snap.Strings[0].StringCurrent, c.current, 1e-9, "I*")
			near(t, snap.Strings[0].GainPower, c.gain, 1e-6, "string gain")
			for i, g := range c.nodeGains {
				near(t, snap.Nodes[i].GainPower, g, 1e-6, "node gain")
			}
		})
	}
}

func TestGainStrings(t *testing.T) {
	strs := ParseStrings("B=4609/3,4609/4; A=4-A1B2C3D, 4609/2")
	s := New(t.TempDir(), 180, nil, strs)
	s.Handle(`{"event_type":"infrastructure_report","nodes":{"4609":{"1":{"barcode":"4-A1B2C3D"}}}}`, T0)
	// A: nodes 1 (by barcode) and 2 (by key): (35,8),(35,3). B: nodes 3,4: (35,8),(35,8). Node 5 unlisted.
	feed(s, T0, [2]float64{35, 8}, [2]float64{35, 3}, [2]float64{35, 8}, [2]float64{35, 8}, [2]float64{20, 5})
	snap := s.Snapshot(T0)
	if len(snap.Strings) != 3 {
		t.Fatalf("strings %+v", snap.Strings)
	}
	byName := map[string]StringSnap{}
	for _, x := range snap.Strings {
		byName[x.Name] = x
	}
	// A: I=8 -> 8*35 = 280; I=3 -> 3*70 = 210 -> I*=8, gain 105
	near(t, byName["A"].UnoptimizedPower, 280, 1e-6, "A unopt")
	near(t, byName["A"].GainPower, 105, 1e-6, "A gain")
	near(t, byName["A"].Power, 385, 1e-6, "A power")
	near(t, byName["B"].UnoptimizedPower, 560, 1e-6, "B unopt")
	near(t, byName["B"].GainPower, 0, 1e-6, "B gain")
	near(t, byName["unassigned"].UnoptimizedPower, 100, 1e-6, "unassigned unopt")
	near(t, snap.ArrayUnoptPower, 940, 1e-6, "array unopt")
	near(t, snap.ArrayGainPower, 105, 1e-6, "array gain")
	if snap.Strings[0].Name != "A" || snap.Strings[2].Name != "unassigned" || snap.Nodes[4].String != "unassigned" || snap.Nodes[0].String != "A" {
		t.Fatalf("order/assignment %+v", snap.Strings)
	}
}

func TestGainOfflineExcluded(t *testing.T) {
	s := New(t.TempDir(), 180, nil, nil)
	feed(s, T0, [2]float64{35, 8}, [2]float64{35, 3})
	// node 1 keeps reporting; node 2 goes stale
	s.Handle(pr(1, 35, 35, 8, T0+200, 4609), T0+200)
	snap := s.Snapshot(T0 + 200)
	near(t, snap.ArrayGainPower, 0, 1e-9, "gain without the shaded node")
	near(t, snap.ArrayUnoptPower, 280, 1e-9, "unopt")
	if snap.Nodes[1].Online || snap.Nodes[1].GainPower != 0 {
		t.Fatalf("offline node %+v", snap.Nodes[1])
	}
	if snap.Strings[0].Nodes != 1 {
		t.Fatalf("nodes %d", snap.Strings[0].Nodes)
	}
}

func TestGainEnergyAndPersist(t *testing.T) {
	dir := t.TempDir()
	s := New(dir, 180, nil, nil)
	feed(s, T0, [2]float64{35, 8}, [2]float64{35, 3}) // gain 105 W
	s.Tick(T0 + 60)                                   // still 105 W: 105*60/3600 Wh
	want := 105 * 60.0 / 3600
	near(t, s.arrayGainWh, want, 1e-9, "array gain Wh")
	near(t, s.nodes["4609/2"].gainWh, want, 1e-9, "node gain Wh")
	near(t, s.nodes["4609/1"].gainWh, 0, 1e-12, "node 1 gain Wh")
	// gain steps to 0 (shaded panel recovers): trapezoid averages 105 and 0
	s.Handle(pr(2, 35, 35, 8, T0+120, 4609), T0+120)
	near(t, s.arrayGainWh, want+105/2.0*60/3600, 1e-9, "trapezoid")
	snap := s.Snapshot(T0 + 120)
	near(t, snap.ArrayGainWh, s.arrayGainWh, 1e-12, "snapshot gain Wh")
	near(t, snap.Nodes[1].GainWh, s.nodes["4609/2"].gainWh, 1e-12, "snapshot node gain Wh")
	if err := s.SaveEnergy(); err != nil {
		t.Fatal(err)
	}
	s2 := New(dir, 180, nil, nil)
	near(t, s2.arrayGainWh, s.arrayGainWh, 1e-12, "reloaded array gain")
	near(t, s2.nodes["4609/2"].gainWh, s.nodes["4609/2"].gainWh, 1e-12, "reloaded node gain")
}

func TestLoadOldEnergyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/energy.json", []byte(`{"array_wh":12.5,"nodes":{"4609/2":3.5}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New(dir, 180, nil, nil)
	near(t, s.arrayEnergy, 12.5, 0, "array")
	near(t, s.nodes["4609/2"].energyWh, 3.5, 0, "node")
	near(t, s.arrayGainWh, 0, 0, "gain")
}
