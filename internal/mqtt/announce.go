// Package mqtt publishes state to MQTT with Home Assistant discovery.
package mqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/jgulick48/tap-exporter/internal/state"
)

// Publisher is the subset of an MQTT client the announce/publish logic needs.
type Publisher interface {
	Publish(topic string, payload string, qos byte, retain bool)
}

type sensorDef struct {
	key, name, unit, dclass, sclass string
	prec                            int
}

var sensors = []sensorDef{
	{"power", "Power", "W", "power", "measurement", 0},
	{"voltage_in", "Input voltage", "V", "voltage", "measurement", 1},
	{"voltage_out", "Output voltage", "V", "voltage", "measurement", 1},
	{"current_in", "Input current", "A", "current", "measurement", 2},
	{"current_out", "Output current", "A", "current", "measurement", 2},
	{"duty_cycle", "Duty cycle", "%", "", "measurement", 0},
	{"temperature", "Temperature", "°C", "temperature", "measurement", 1},
	{"rssi", "Signal", "", "", "measurement", 0},
}

type device struct {
	Identifiers  []string `json:"identifiers"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"manufacturer"`
	Model        string   `json:"model"`
	ViaDevice    string   `json:"via_device,omitempty"`
	SerialNumber string   `json:"serial_number,omitempty"`
}

// sensorConfig is a Home Assistant MQTT discovery payload (key order as in the Python version).
type sensorConfig struct {
	Name              string `json:"name"`
	UniqueID          string `json:"unique_id"`
	ObjectID          string `json:"object_id"`
	StateTopic        string `json:"state_topic"`
	ValueTemplate     string `json:"value_template"`
	AvailabilityTopic string `json:"availability_topic"`
	Device            device `json:"device"`
	Unit              string `json:"unit_of_measurement,omitempty"`
	DeviceClass       string `json:"device_class,omitempty"`
	StateClass        string `json:"state_class,omitempty"`
	Precision         *int   `json:"suggested_display_precision,omitempty"`
	EntityCategory    string `json:"entity_category,omitempty"`
}

var arrayDevice = device{
	Identifiers: []string{"tigo_tap"}, Name: "Tigo array", Manufacturer: "Tigo", Model: "TAP (read by taptap)",
}

// Announcer builds and publishes discovery configs and state payloads.
type Announcer struct {
	Pub    Publisher
	Topic  string // state topic prefix, e.g. "tigo"
	Disc   string // discovery prefix, e.g. "homeassistant"
	Avail  string // availability topic
	mu     sync.Mutex
	annced map[string]bool
}

// NewAnnouncer creates an Announcer; avail is "<topic>/status".
func NewAnnouncer(pub Publisher, topic, disc string) *Announcer {
	return &Announcer{Pub: pub, Topic: topic, Disc: disc, Avail: topic + "/status", annced: map[string]bool{}}
}

// Reset forgets what was announced, so the next Announce republishes everything.
func (a *Announcer) Reset() {
	a.mu.Lock()
	a.annced = map[string]bool{}
	a.mu.Unlock()
}

func toJSON(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "{}"
	}
	return string(bytes.TrimRight(buf.Bytes(), "\n"))
}

func (a *Announcer) sensor(uid, name, topic, tmpl string, dev device, unit, dclass, sclass string, prec *int, category string) {
	a.entity("sensor", uid, name, topic, tmpl, dev, unit, dclass, sclass, prec, category)
}

// entity publishes a discovery config for a component ("sensor" or "binary_sensor").
func (a *Announcer) entity(component, uid, name, topic, tmpl string, dev device, unit, dclass, sclass string, prec *int, category string) {
	cfg := sensorConfig{
		Name: name, UniqueID: uid, ObjectID: uid, StateTopic: topic, ValueTemplate: tmpl,
		AvailabilityTopic: a.Avail, Device: dev, Unit: unit, DeviceClass: dclass, StateClass: sclass,
		Precision: prec, EntityCategory: category,
	}
	a.Pub.Publish(fmt.Sprintf("%s/%s/%s/config", a.Disc, component, uid), toJSON(cfg), 1, true)
}

func ip(i int) *int { return &i }

// Announce publishes discovery configs for the array and any optimizer not announced yet.
func (a *Announcer) Announce(s state.Snapshot) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.annced["array"] {
		t := a.Topic + "/array"
		dev := arrayDevice
		a.sensor("tigo_array_power", "Power", t, "{{ value_json.power }}", dev, "W", "power", "measurement", ip(0), "")
		a.sensor("tigo_array_energy", "Energy", t, "{{ value_json.energy_kwh }}", dev, "kWh", "energy", "total_increasing", ip(2), "")
		a.sensor("tigo_array_gain_power", "Optimizer gain", t, "{{ value_json.gain_power }}", dev, "W", "power", "measurement", ip(0), "")
		a.sensor("tigo_array_gain_energy", "Optimizer gain energy", t, "{{ value_json.gain_energy_kwh }}", dev, "kWh", "energy", "total_increasing", ip(2), "")
		a.sensor("tigo_array_unoptimized_power", "Unoptimized power estimate", t, "{{ value_json.unoptimized_power }}", dev, "W", "power", "measurement", ip(0), "")
		a.sensor("tigo_array_online", "Optimizers online", t, "{{ value_json.online }}", dev, "", "", "measurement", ip(0), "")
		a.sensor("tigo_array_last_report", "Last report", t, "{{ value_json.last_report }}", dev, "", "timestamp", "", nil, "diagnostic")
		a.annced["array"] = true
	}
	for _, x := range s.Nodes {
		sig := "node:" + x.UID + "|" + x.Name
		if a.annced[sig] {
			continue
		}
		ndev := device{
			Identifiers: []string{"tigo_" + x.UID}, Name: "Tigo " + x.Name, Manufacturer: "Tigo",
			Model: "TS4 optimizer", ViaDevice: "tigo_tap", SerialNumber: x.BarcodeOrEmpty(),
		}
		t := a.Topic + "/node/" + x.UID
		for _, sd := range sensors {
			category := ""
			if sd.key == "rssi" || sd.key == "duty_cycle" {
				category = "diagnostic"
			}
			tmpl := fmt.Sprintf("{{ value_json.%s if value_json.%s is not none else None }}", sd.key, sd.key)
			switch sd.key {
			case "power":
				tmpl = "{{ value_json.power }}"
			case "duty_cycle":
				tmpl = "{{ (value_json.duty_cycle * 100) | round(0) if value_json.duty_cycle is not none else None }}"
			}
			a.sensor(fmt.Sprintf("tigo_%s_%s", x.UID, sd.key), sd.name, t, tmpl, ndev, sd.unit, sd.dclass, sd.sclass, ip(sd.prec), category)
		}
		a.sensor(fmt.Sprintf("tigo_%s_energy", x.UID), "Energy", t, "{{ value_json.energy_kwh }}", ndev, "kWh", "energy", "total_increasing", ip(3), "")
		a.sensor(fmt.Sprintf("tigo_%s_gain_power", x.UID), "Optimizer gain", t, "{{ value_json.gain_power }}", ndev, "W", "power", "measurement", ip(0), "")
		a.sensor(fmt.Sprintf("tigo_%s_gain_energy", x.UID), "Optimizer gain energy", t, "{{ value_json.gain_energy_kwh }}", ndev, "kWh", "energy", "total_increasing", ip(3), "")
		a.entity("binary_sensor", fmt.Sprintf("tigo_%s_online", x.UID), "Online", t, "{{ 'ON' if value_json.online else 'OFF' }}", ndev, "", "connectivity", "", nil, "diagnostic")
		a.annced[sig] = true
	}
}

// round mimics Python's round(x, nd): correctly rounded decimal, ties to even.
func round(x float64, nd int) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return x
	}
	r, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'f', nd, 64), 64)
	return r
}

type arrayPayload struct {
	Power      float64 `json:"power"`
	EnergyKWh  float64 `json:"energy_kwh"`
	Online     int     `json:"online"`
	Known      int     `json:"known"`
	LastReport *string `json:"last_report"`
	UnoptPower float64 `json:"unoptimized_power"`
	GainPower  float64 `json:"gain_power"`
	GainKWh    float64 `json:"gain_energy_kwh"`
}

type nodePayload struct {
	Power      float64  `json:"power"`
	VoltageIn  *float64 `json:"voltage_in"`
	VoltageOut *float64 `json:"voltage_out"`
	CurrentIn  *float64 `json:"current_in"`
	CurrentOut *float64 `json:"current_out"`
	DutyCycle  *float64 `json:"duty_cycle"`
	Temp       *float64 `json:"temperature"`
	RSSI       *float64 `json:"rssi"`
	EnergyKWh  float64  `json:"energy_kwh"`
	Online     bool     `json:"online"`
	Barcode    *string  `json:"barcode"`
	GainPower  float64  `json:"gain_power"`
	GainKWh    float64  `json:"gain_energy_kwh"`
	String     string   `json:"string"`
}

// isoLocal formats Unix seconds like Python's datetime.fromtimestamp(ts).astimezone().isoformat().
func isoLocal(ts float64) string {
	t := time.UnixMicro(int64(math.Round(ts * 1e6))).Local()
	layout := "2006-01-02T15:04:05-07:00"
	if t.Nanosecond() != 0 {
		layout = "2006-01-02T15:04:05.000000-07:00"
	}
	return t.Format(layout)
}

// Publish publishes the array and per-optimizer state payloads (retained, QoS 0).
func (a *Announcer) Publish(s state.Snapshot) {
	last := 0.0
	for _, x := range s.Nodes {
		last = math.Max(last, x.TS)
	}
	ap := arrayPayload{
		Power: round(s.ArrayPower, 1), EnergyKWh: round(s.ArrayEnergyWh/1000, 4),
		Online: s.Online, Known: len(s.Nodes),
		UnoptPower: round(s.ArrayUnoptPower, 1), GainPower: round(s.ArrayGainPower, 1), GainKWh: round(s.ArrayGainWh/1000, 4),
	}
	if last != 0 {
		l := isoLocal(last)
		ap.LastReport = &l
	}
	a.Pub.Publish(a.Topic+"/array", toJSON(ap), 0, true)
	for _, x := range s.Nodes {
		get := func(k string) *float64 {
			if v, ok := x.Values[k]; ok {
				r := round(v, 3)
				return &r
			}
			return nil
		}
		np := nodePayload{
			Power: round(x.Power, 1), VoltageIn: get("voltage_in"), VoltageOut: get("voltage_out"),
			CurrentIn: get("current_in"), CurrentOut: get("current_out"), DutyCycle: get("duty_cycle"),
			Temp: get("temperature"), RSSI: get("rssi"),
			EnergyKWh: round(x.EnergyWh/1000, 5), Online: x.Online, Barcode: x.Barcode,
			GainPower: round(x.GainPower, 1), GainKWh: round(x.GainWh/1000, 5), String: x.String,
		}
		a.Pub.Publish(a.Topic+"/node/"+x.UID, toJSON(np), 0, true)
	}
}
