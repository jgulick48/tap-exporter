package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jgulick48/tap-exporter/internal/state"
)

const T0 = 1_800_000_000.0

func pr(node int, ts float64) string {
	b, _ := json.Marshal(map[string]any{
		"event_type": "power_report", "gateway": 4609, "node": node,
		"timestamp":  time.Unix(int64(ts), 0).UTC().Format(time.RFC3339),
		"voltage_in": 35.0, "voltage_out": 34.5, "current": 8.0,
		"dc_dc_duty_cycle": 1.0, "temperature": 12.3, "rssi": 150,
	})
	return string(b)
}

func TestMetricsRender(t *testing.T) {
	s := state.New(t.TempDir(), 180, map[string]string{"4-A1B2C3D": "East 1", "4609/3": "West 3"})
	s.Handle(pr(2, T0), T0)
	txt := Render(s.Snapshot(T0))
	for _, want := range []string{
		`tigo_power_watts{gateway="4609",node="2",barcode="",name="Node 4609/2"} 276` + "\n",
		"tigo_array_power_watts 276\n",
		`tigo_nodes{state="online"} 1`,
		"# HELP tigo_rssi Signal strength of the optimizer's last report as seen by the TAP.\n# TYPE tigo_rssi gauge\n",
		`tigo_events_total{type="bad"} 0`,
		"tigo_taptap_up 0\n",
	} {
		if !strings.Contains(txt, want) {
			t.Errorf("missing %q in:\n%s", want, txt)
		}
	}
	if !strings.HasSuffix(txt, "\n") || strings.HasSuffix(txt, "\n\n") {
		t.Error("bad trailing newline")
	}
	for _, l := range strings.Split(strings.TrimSuffix(txt, "\n"), "\n") {
		if strings.HasPrefix(l, "#") {
			continue
		}
		if !strings.Contains(l, " ") {
			t.Errorf("bad line %q", l)
		}
	}
}

func TestNumberFormat(t *testing.T) {
	cases := map[float64]string{276: "276", 0.1234567: "0.123457", 1e-5: "1e-05", 1234567: "1.23457e+06", 0: "0"}
	for in, want := range cases {
		if got := fmtG(in); got != want {
			t.Errorf("fmtG(%v)=%q want %q", in, got, want)
		}
	}
	if fmtF3(1.00049) != "1.000" {
		t.Error("fmtF3")
	}
}

func TestEscape(t *testing.T) {
	if got := escaper.Replace("a\"b\\c\nd"); got != `a\"b\\c\nd` {
		t.Fatal(got)
	}
}

func TestHandler(t *testing.T) {
	s := state.New(t.TempDir(), 180, nil)
	h := Handler(s)
	get := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	if w := get("/healthz"); w.Code != http.StatusServiceUnavailable || w.Body.String() != "taptap not running\n" {
		t.Fatalf("healthz down: %d %q", w.Code, w.Body)
	}
	s.SetTaptapRunning(true)
	if w := get("/healthz"); w.Code != 200 || w.Body.String() != "ok\n" {
		t.Fatalf("healthz up: %d %q", w.Code, w.Body)
	}
	if w := get("/metrics"); w.Code != 200 || w.Header().Get("Content-Type") != "text/plain; version=0.0.4" {
		t.Fatalf("metrics: %d %v", w.Code, w.Header())
	}
	w := get("/state")
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("state: %v %v", err, w.Header())
	}
	if w := get("/"); w.Body.String() != "tap-exporter: /metrics /state /healthz\n" {
		t.Fatal(w.Body)
	}
}
