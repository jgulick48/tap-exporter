// Package state holds everything known about the TAP bus: optimizers, energy
// counters and taptap process status. All access goes through the State lock.
package state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MaxIntegrationGap: Wh accumulate only across gaps shorter than this (seconds);
// longer means missing data.
const MaxIntegrationGap = 120.0

// Now returns the current time as Unix seconds.
func Now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

// ParseNames parses "KEY=Name,KEY=Name".
func ParseNames(spec string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if k != "" && v != "" {
			out[k] = v
		}
	}
	return out
}

// parseTS parses an ISO8601 timestamp to Unix seconds; ok is false when invalid.
func parseTS(v any) (float64, bool) {
	s, isStr := v.(string)
	if !isStr {
		return 0, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		// No zone designator: interpret as local time (like Python's fromisoformat).
		t, err = time.ParseInLocation("2006-01-02T15:04:05.999999999", s, time.Local)
		if err != nil {
			return 0, false
		}
	}
	return float64(t.UnixNano()) / 1e9, true
}

type node struct {
	gateway, node int
	barcode       string
	values        map[string]float64
	ts            float64
	reports       int
	energyWh      float64
	power         float64
}

func (n *node) key() string { return fmt.Sprintf("%d/%d", n.gateway, n.node) }
func (n *node) uid() string { return fmt.Sprintf("g%dn%d", n.gateway, n.node) }

// Events counts taptap events by type. Field order is the output order.
type Events struct {
	PowerReport          int `json:"power_report"`
	InfrastructureReport int `json:"infrastructure_report"`
	Other                int `json:"other"`
	Bad                  int `json:"bad"`
}

// Gateway as reported by taptap's infrastructure report.
type Gateway struct {
	Address any `json:"address"`
	Version any `json:"version"`
}

// NodeSnap is one optimizer in a Snapshot.
type NodeSnap struct {
	Key      string             `json:"key"`
	UID      string             `json:"uid"`
	Gateway  int                `json:"gateway"`
	Node     int                `json:"node"`
	Barcode  *string            `json:"barcode"`
	Name     string             `json:"name"`
	Online   bool               `json:"online"`
	Values   map[string]float64 `json:"values"`
	Power    float64            `json:"power"`
	EnergyWh float64            `json:"energy_wh"`
	TS       float64            `json:"ts"`
	Reports  int                `json:"reports"`
}

// BarcodeOrEmpty returns the barcode or "".
func (n NodeSnap) BarcodeOrEmpty() string {
	if n.Barcode == nil {
		return ""
	}
	return *n.Barcode
}

// Snapshot is a consistent copy of the state, also served as /state.
type Snapshot struct {
	Now           float64            `json:"now"`
	Nodes         []NodeSnap         `json:"nodes"`
	ArrayPower    float64            `json:"array_power"`
	ArrayEnergyWh float64            `json:"array_energy_wh"`
	Online        int                `json:"online"`
	Events        Events             `json:"events"`
	LastEvent     float64            `json:"last_event"`
	TaptapRunning bool               `json:"taptap_running"`
	Connected     bool               `json:"connected"`
	Restarts      int                `json:"restarts"`
	Gateways      map[string]Gateway `json:"gateways"`
}

// State is everything known about the bus.
type State struct {
	mu          sync.Mutex
	nodes       map[string]*node
	gateways    map[string]Gateway
	timeout     float64
	names       map[string]string
	energyFile  string
	arrayEnergy float64
	arrayPower  float64
	arrayTS     float64
	events      Events
	lastEvent   float64
	running     bool
	connected   bool
	restarts    int
	changed     atomic.Bool // set when the node list / names change (re-announce to HA)
}

// New creates a State and loads persisted energy counters from stateDir.
func New(stateDir string, timeout float64, names map[string]string) *State {
	if names == nil {
		names = map[string]string{}
	}
	s := &State{
		nodes:      map[string]*node{},
		gateways:   map[string]Gateway{},
		timeout:    timeout,
		names:      names,
		energyFile: filepath.Join(stateDir, "energy.json"),
	}
	s.loadEnergy()
	return s
}

// ---- change flag ---------------------------------------------------------

// MarkChanged requests a Home Assistant re-announce.
func (s *State) MarkChanged() { s.changed.Store(true) }

// TakeChanged reports whether the changed flag was set, clearing it.
func (s *State) TakeChanged() bool { return s.changed.Swap(false) }

// ---- taptap status (written from other goroutines) -----------------------

func (s *State) SetTaptapRunning(v bool) { s.mu.Lock(); s.running = v; s.mu.Unlock() }
func (s *State) SetConnected(v bool)     { s.mu.Lock(); s.connected = v; s.mu.Unlock() }
func (s *State) TaptapRunning() bool     { s.mu.Lock(); defer s.mu.Unlock(); return s.running }
func (s *State) IncRestarts()            { s.mu.Lock(); s.restarts++; s.mu.Unlock() }

// TaptapStopped marks taptap as not running and the adapter as disconnected.
func (s *State) TaptapStopped() {
	s.mu.Lock()
	s.running, s.connected = false, false
	s.mu.Unlock()
}

// ---- persistence ---------------------------------------------------------

func (s *State) loadEnergy() {
	b, err := os.ReadFile(s.energyFile)
	if err != nil {
		return
	}
	var d struct {
		ArrayWh float64            `json:"array_wh"`
		Nodes   map[string]float64 `json:"nodes"`
	}
	if json.Unmarshal(b, &d) != nil {
		return
	}
	s.arrayEnergy = d.ArrayWh
	for key, wh := range d.Nodes {
		gw, nd, ok := parseKey(key)
		if !ok {
			continue
		}
		n := s.nodes[key]
		if n == nil {
			n = &node{gateway: gw, node: nd, values: map[string]float64{}}
			s.nodes[key] = n
		}
		n.energyWh = wh
	}
}

func parseKey(key string) (gw, nd int, ok bool) {
	a, b, found := strings.Cut(key, "/")
	if !found {
		return 0, 0, false
	}
	var err1, err2 error
	gw, err1 = strconv.Atoi(strings.TrimSpace(a))
	nd, err2 = strconv.Atoi(strings.TrimSpace(b))
	return gw, nd, err1 == nil && err2 == nil
}

// SaveEnergy writes the energy counters atomically (tmp + rename).
func (s *State) SaveEnergy() error {
	s.mu.Lock()
	d := struct {
		ArrayWh float64            `json:"array_wh"`
		Nodes   map[string]float64 `json:"nodes"`
	}{s.arrayEnergy, make(map[string]float64, len(s.nodes))}
	for k, n := range s.nodes {
		d.Nodes[k] = n.energyWh
	}
	s.mu.Unlock()
	b, err := json.Marshal(d)
	if err == nil {
		tmp := s.energyFile + ".tmp"
		if err = os.WriteFile(tmp, b, 0o644); err == nil {
			err = os.Rename(tmp, s.energyFile)
		}
	}
	if err != nil {
		slog.Warn("saving energy counters failed", "err", err)
	}
	return err
}

// ---- events --------------------------------------------------------------

func (s *State) nameOf(n *node) string {
	if v := s.names[n.barcode]; v != "" && n.barcode != "" {
		return v
	}
	if v := s.names[n.key()]; v != "" {
		return v
	}
	if n.barcode != "" {
		return n.barcode
	}
	return "Node " + n.key()
}

// Handle processes one line of taptap output. now is Unix seconds.
func (s *State) Handle(line string, now float64) {
	var ev map[string]any
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		var v any
		if json.Unmarshal([]byte(line), &v) == nil {
			return // valid JSON but not an object
		}
		if len(line) > 200 {
			line = line[:200]
		}
		slog.Debug("not JSON", "line", line)
		return
	}
	kind, _ := ev["event_type"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastEvent = now
	s.connected = true
	switch kind {
	case "power_report":
		if s.power(ev, now) {
			s.events.PowerReport++
		} else {
			s.events.Bad++
		}
	case "infrastructure_report":
		s.events.InfrastructureReport++
		s.infrastructure(ev)
	default:
		s.events.Other++
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(x))
		return i, err == nil
	}
	return 0, false
}

// power handles a power_report; the lock is held.
func (s *State) power(ev map[string]any, now float64) bool {
	var ok [8]bool
	var gw, nd, rssi int
	var vin, vout, cur, duty, temp float64
	gw, ok[0] = toInt(ev["gateway"])
	nd, ok[1] = toInt(ev["node"])
	vin, ok[2] = toFloat(ev["voltage_in"])
	vout, ok[3] = toFloat(ev["voltage_out"])
	cur, ok[4] = toFloat(ev["current"])
	duty, ok[5] = toFloat(ev["dc_dc_duty_cycle"])
	temp, ok[6] = toFloat(ev["temperature"])
	rssi, ok[7] = toInt(ev["rssi"])
	for _, o := range ok {
		if !o {
			slog.Warn("bad power report", "event", ev)
			return false
		}
	}
	ts, valid := parseTS(ev["timestamp"])
	if !valid || ts == 0 {
		ts = now
	}
	if ts > now+60 || ts < now-600 { // taptap's slot clock is off; use arrival time
		ts = now
	}
	key := fmt.Sprintf("%d/%d", gw, nd)
	n := s.nodes[key]
	if n == nil {
		n = &node{gateway: gw, node: nd, values: map[string]float64{}}
		s.nodes[key] = n
		s.changed.Store(true)
	}
	power := vout * cur
	if n.ts != 0 && ts-n.ts > 0 && ts-n.ts <= MaxIntegrationGap {
		n.energyWh += (n.power + power) / 2 * (ts - n.ts) / 3600.0
	}
	if ts >= n.ts {
		n.ts = ts
		n.power = power
		curIn := 0.0
		if vin > 0.5 {
			curIn = power / vin
		}
		n.values = map[string]float64{
			"voltage_in": vin, "voltage_out": vout, "current_out": cur,
			"current_in": curIn,
			"power":      power, "duty_cycle": duty, "temperature": temp, "rssi": float64(rssi),
		}
	}
	n.reports++
	s.arrayUpdate(now)
	return true
}

// infrastructure handles an infrastructure_report; the lock is held.
func (s *State) infrastructure(ev map[string]any) {
	if gws, ok := ev["gateways"].(map[string]any); ok {
		for gw, v := range gws {
			if g, ok := v.(map[string]any); ok {
				s.gateways[gw] = Gateway{Address: g["address"], Version: g["version"]}
			}
		}
	}
	nodes, _ := ev["nodes"].(map[string]any)
	for gws, v := range nodes {
		perNode, ok := v.(map[string]any)
		if !ok {
			continue
		}
		gw, err := strconv.Atoi(strings.TrimSpace(gws))
		if err != nil {
			continue
		}
		for nds, info := range perNode {
			nd, err := strconv.Atoi(strings.TrimSpace(nds))
			if err != nil {
				continue
			}
			key := fmt.Sprintf("%d/%d", gw, nd)
			n := s.nodes[key]
			if n == nil {
				n = &node{gateway: gw, node: nd, values: map[string]float64{}}
				s.nodes[key] = n
			}
			m, _ := info.(map[string]any)
			if bc, _ := m["barcode"].(string); bc != "" && bc != n.barcode {
				n.barcode = bc
				s.changed.Store(true)
			}
		}
	}
}

// arrayUpdate: array power = sum of online optimizers; integrate it for array energy.
func (s *State) arrayUpdate(now float64) {
	p := 0.0
	for _, n := range s.nodes {
		if n.ts != 0 && now-n.ts <= s.timeout {
			p += n.power
		}
	}
	if s.arrayTS != 0 && now-s.arrayTS > 0 && now-s.arrayTS <= MaxIntegrationGap {
		s.arrayEnergy += (s.arrayPower + p) / 2 * (now - s.arrayTS) / 3600.0
	}
	s.arrayPower, s.arrayTS = p, now
}

// Tick is called periodically: lets the array power fall to 0 when reports stop.
func (s *State) Tick(now float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.arrayUpdate(now)
}

func (s *State) online(n *node, now float64) bool {
	return n.ts != 0 && now-n.ts <= s.timeout
}

// Snapshot returns a consistent copy of the state as of now.
func (s *State) Snapshot(now float64) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	all := make([]*node, 0, len(s.nodes))
	for _, n := range s.nodes {
		all = append(all, n)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].gateway != all[j].gateway {
			return all[i].gateway < all[j].gateway
		}
		return all[i].node < all[j].node
	})
	snap := Snapshot{Now: now, Nodes: make([]NodeSnap, 0, len(all)), Gateways: map[string]Gateway{}}
	for _, n := range all {
		on := s.online(n, now)
		ns := NodeSnap{
			Key: n.key(), UID: n.uid(), Gateway: n.gateway, Node: n.node,
			Name: s.nameOf(n), Online: on, Values: map[string]float64{},
			EnergyWh: n.energyWh, TS: n.ts, Reports: n.reports,
		}
		if n.barcode != "" {
			bc := n.barcode
			ns.Barcode = &bc
		}
		if on {
			for k, v := range n.values {
				ns.Values[k] = v
			}
			ns.Power = n.power
			snap.Online++
		}
		snap.ArrayPower += ns.Power
		snap.Nodes = append(snap.Nodes, ns)
	}
	snap.ArrayEnergyWh = s.arrayEnergy
	snap.Events = s.events
	snap.LastEvent = s.lastEvent
	snap.TaptapRunning, snap.Connected, snap.Restarts = s.running, s.connected, s.restarts
	for k, v := range s.gateways {
		snap.Gateways[k] = v
	}
	return snap
}
