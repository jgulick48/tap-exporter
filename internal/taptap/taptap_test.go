package taptap

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jgulick48/tap-exporter/internal/state"
)

func TestStderrConnectedTracking(t *testing.T) {
	st := state.New(t.TempDir(), 180, nil)
	for line, want := range map[string]bool{
		"INFO Entering read loop":            true,
		"WARN error opening source: refused": false,
		"INFO Source opened":                 true,
		"WARN reconnecting in 10s":           false,
	} {
		handleStderrLine(st, line)
		if got := st.Snapshot(0).Connected; got != want {
			t.Errorf("%q: connected=%v want %v", line, got, want)
		}
	}
	st.SetConnected(true)
	handleStderrLine(st, "something unrelated")
	if !st.Snapshot(0).Connected {
		t.Error("unrelated line changed state")
	}
}

func TestRunProcessesOutputAndStops(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fake-taptap")
	body := "#!/bin/sh\necho 'Source opened' >&2\n" +
		`echo '{"event_type":"power_report","gateway":1,"node":2,"timestamp":"2099-01-01T00:00:00Z","voltage_in":30,"voltage_out":29,"current":2,"dc_dc_duty_cycle":1,"temperature":20,"rssi":100}'` +
		"\nexec sleep 30\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	st := state.New(dir, 180, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Run(ctx, st, Config{Binary: script, Host: "h", Port: 1, StateFile: filepath.Join(dir, "t.json")})
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for st.Snapshot(0).Events.PowerReport == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no power report processed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !st.TaptapRunning() {
		t.Fatal("expected running")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if snap := st.Snapshot(0); snap.TaptapRunning || snap.Connected || snap.Restarts != 0 {
		t.Fatalf("after stop: %+v", snap)
	}
}

func TestRunRestartsOnExit(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "fail")
	os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o755)
	st := state.New(dir, 180, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, st, Config{Binary: script, Host: "h", Port: 1, StateFile: "x"}); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for st.Snapshot(0).Restarts == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no restart counted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}
