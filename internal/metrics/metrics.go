// Package metrics renders the Prometheus text exposition and serves the HTTP endpoints.
package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/jgulick48/tap-exporter/internal/state"
)

var gaugeMetrics = []struct{ key, name, help string }{
	{"voltage_in", "tigo_voltage_in_volts", "Panel (optimizer input) voltage."},
	{"voltage_out", "tigo_voltage_out_volts", "Optimizer output voltage."},
	{"current_out", "tigo_current_out_amps", "Optimizer output (string) current."},
	{"current_in", "tigo_current_in_amps", "Panel current, from power / input voltage."},
	{"power", "tigo_power_watts", "Optimizer output power."},
	{"duty_cycle", "tigo_duty_cycle_ratio", "DC-DC converter duty cycle (1 = pass-through)."},
	{"temperature", "tigo_temperature_celsius", "Optimizer temperature."},
	{"rssi", "tigo_rssi", "Signal strength of the optimizer's last report as seen by the TAP."},
}

func special(v float64) (string, bool) {
	switch {
	case math.IsNaN(v):
		return "nan", true
	case math.IsInf(v, 1):
		return "inf", true
	case math.IsInf(v, -1):
		return "-inf", true
	}
	return "", false
}

// fmtG is Python's "{:.6g}".
func fmtG(v float64) string {
	if s, ok := special(v); ok {
		return s
	}
	return strconv.FormatFloat(v, 'g', 6, 64)
}

// fmtF3 is Python's "{:.3f}".
func fmtF3(v float64) string {
	if s, ok := special(v); ok {
		return s
	}
	return strconv.FormatFloat(v, 'f', 3, 64)
}

var escaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Render produces the Prometheus text format for a snapshot.
func Render(s state.Snapshot) string {
	var out strings.Builder
	line := func(format string, a ...any) { fmt.Fprintf(&out, format+"\n", a...) }
	head := func(name, typ, help string) {
		line("# HELP %s %s", name, help)
		line("# TYPE %s %s", name, typ)
	}
	labels := func(x state.NodeSnap) string {
		return fmt.Sprintf(`gateway="%d",node="%d",barcode="%s",name="%s"`,
			x.Gateway, x.Node, escaper.Replace(x.BarcodeOrEmpty()), escaper.Replace(x.Name))
	}

	for _, m := range gaugeMetrics {
		head(m.name, "gauge", m.help)
		for _, x := range s.Nodes {
			if v, ok := x.Values[m.key]; ok {
				line("%s{%s} %s", m.name, labels(x), fmtG(v))
			}
		}
	}
	head("tigo_online", "gauge", "1 if the optimizer reported within NODE_TIMEOUT.")
	for _, x := range s.Nodes {
		line("tigo_online{%s} %d", labels(x), b2i(x.Online))
	}
	head("tigo_last_report_timestamp_seconds", "gauge", "Time of the optimizer's last power report.")
	for _, x := range s.Nodes {
		if x.TS != 0 {
			line("tigo_last_report_timestamp_seconds{%s} %s", labels(x), fmtF3(x.TS))
		}
	}
	head("tigo_reports_total", "counter", "Power reports received from the optimizer since start.")
	for _, x := range s.Nodes {
		line("tigo_reports_total{%s} %d", labels(x), x.Reports)
	}
	head("tigo_energy_wh_total", "counter", "Energy from the optimizer, integrated from its reports.")
	for _, x := range s.Nodes {
		line("tigo_energy_wh_total{%s} %s", labels(x), fmtF3(x.EnergyWh))
	}
	head("tigo_array_power_watts", "gauge", "Sum of the online optimizers' output power.")
	line("tigo_array_power_watts %s", fmtG(s.ArrayPower))
	head("tigo_array_energy_wh_total", "counter", "Array energy, integrated from the array power.")
	line("tigo_array_energy_wh_total %s", fmtF3(s.ArrayEnergyWh))
	head("tigo_nodes", "gauge", "Optimizers known / online.")
	line(`tigo_nodes{state="known"} %d`, len(s.Nodes))
	line(`tigo_nodes{state="online"} %d`, s.Online)
	head("tigo_events_total", "counter", "taptap events by type (bad = power reports that failed to parse).")
	line(`tigo_events_total{type="power_report"} %d`, s.Events.PowerReport)
	line(`tigo_events_total{type="infrastructure_report"} %d`, s.Events.InfrastructureReport)
	line(`tigo_events_total{type="other"} %d`, s.Events.Other)
	line(`tigo_events_total{type="bad"} %d`, s.Events.Bad)
	head("tigo_last_event_timestamp_seconds", "gauge", "Time of the last event from taptap.")
	line("tigo_last_event_timestamp_seconds %s", fmtF3(s.LastEvent))
	head("tigo_taptap_up", "gauge", "1 while the taptap process is running.")
	line("tigo_taptap_up %d", b2i(s.TaptapRunning))
	head("tigo_adapter_connected", "gauge", "1 while taptap is connected to the RS485 adapter.")
	line("tigo_adapter_connected %d", b2i(s.Connected))
	head("tigo_taptap_restarts_total", "counter", "taptap restarts.")
	line("tigo_taptap_restarts_total %d", s.Restarts)
	return out.String()
}

// Handler serves /metrics, /healthz, /state and an index for any other path.
func Handler(st *state.State) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		ctype, code := "text/plain", http.StatusOK
		switch p := r.URL.Path; {
		case strings.HasPrefix(p, "/metrics"):
			body, ctype = []byte(Render(st.Snapshot(state.Now()))), "text/plain; version=0.0.4"
		case strings.HasPrefix(p, "/healthz"):
			if st.TaptapRunning() {
				body = []byte("ok\n")
			} else {
				body, code = []byte("taptap not running\n"), http.StatusServiceUnavailable
			}
		case strings.HasPrefix(p, "/state"):
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", " ")
			if err := enc.Encode(st.Snapshot(state.Now())); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			body, ctype = bytes.TrimRight(buf.Bytes(), "\n"), "application/json"
		default:
			body = []byte("tap-exporter: /metrics /state /healthz\n")
		}
		w.Header().Set("Content-Type", ctype)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(code)
		w.Write(body)
	})
}
