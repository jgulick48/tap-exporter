// tap-exporter: Tigo TS4 optimizer data from the TAP RS485 bus.
//
// Runs `taptap observe` (github.com/litinoveweedle/taptap) against an
// RS485-to-TCP adapter wired in parallel with the CCA <-> TAP bus, and turns
// its JSON lines into Prometheus /metrics, /state, /healthz and MQTT with Home
// Assistant discovery.
//
// Configuration (environment):
//
//	TAPTAP_HOST, TAPTAP_PORT (4196)   the adapter
//	TAPTAP_BIN (taptap), STATE_DIR (/data)
//	LISTEN_ADDRESS (:9812), NODE_TIMEOUT (180), NODE_NAMES, LOG_LEVEL (info)
//	STRINGS (empty = one string "all")   series strings for the optimizer gain
//	estimate: "Name=ID,ID;Name2=ID,..." where ID is a barcode or gateway/node
//	MQTT_HOST (empty = MQTT off), MQTT_PORT (8883), MQTT_SERVER_NAME,
//	MQTT_CA_FILE, MQTT_CERT_FILE, MQTT_KEY_FILE, MQTT_USER, MQTT_PASSWORD,
//	MQTT_CLIENT_ID (tap-exporter), MQTT_TOPIC (tigo), DISCOVERY_PREFIX
//	(homeassistant), MQTT_INTERVAL (15) seconds between state updates
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jgulick48/tap-exporter/internal/metrics"
	"github.com/jgulick48/tap-exporter/internal/mqtt"
	"github.com/jgulick48/tap-exporter/internal/state"
	"github.com/jgulick48/tap-exporter/internal/taptap"
)

// Set via -ldflags "-X main.version=... -X main.commit=...".
var (
	version = "dev"
	commit  = "unknown"
)

// env returns the variable, or def when it is unset or empty.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envFloat(name, def string) (float64, error) {
	v, err := strconv.ParseFloat(strings.TrimSpace(env(name, def)), 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

func envInt(name, def string) (int, error) {
	v, err := strconv.Atoi(strings.TrimSpace(env(name, def)))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return v, nil
}

func logLevel(s string) slog.Level {
	switch strings.ToUpper(s) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARNING", "WARN":
		return slog.LevelWarn
	case "ERROR", "CRITICAL":
		return slog.LevelError
	}
	return slog.LevelInfo
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel(env("LOG_LEVEL", "INFO"))})))
	host := os.Getenv("TAPTAP_HOST")
	if host == "" {
		return errors.New("TAPTAP_HOST is required")
	}
	slog.Info("tap-exporter starting", "version", version, "commit", commit)

	stateDir := env("STATE_DIR", "/data")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}
	timeout, err := envFloat("NODE_TIMEOUT", "180")
	if err != nil {
		return err
	}
	tapPort, err := envInt("TAPTAP_PORT", "4196")
	if err != nil {
		return err
	}
	st := state.New(stateDir, timeout, state.ParseNames(os.Getenv("NODE_NAMES")),
		state.ParseStrings(os.Getenv("STRINGS")))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	listen := env("LISTEN_ADDRESS", ":9812")
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: metrics.Handler(st), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
		}
	}()
	slog.Info("listening", "address", listen)

	var mq *mqtt.Client
	if mhost := os.Getenv("MQTT_HOST"); mhost != "" {
		cfg := mqtt.Config{
			Host: mhost, ServerName: os.Getenv("MQTT_SERVER_NAME"), CAFile: os.Getenv("MQTT_CA_FILE"),
			CertFile: os.Getenv("MQTT_CERT_FILE"), KeyFile: os.Getenv("MQTT_KEY_FILE"),
			User: os.Getenv("MQTT_USER"), Password: os.Getenv("MQTT_PASSWORD"),
			ClientID: env("MQTT_CLIENT_ID", "tap-exporter"), Topic: env("MQTT_TOPIC", "tigo"),
			Discovery: env("DISCOVERY_PREFIX", "homeassistant"),
		}
		if cfg.Port, err = envInt("MQTT_PORT", "8883"); err != nil {
			return err
		}
		secs, err := envFloat("MQTT_INTERVAL", "15")
		if err != nil {
			return err
		}
		cfg.Interval = time.Duration(secs * float64(time.Second))
		if mq, err = mqtt.New(cfg, st); err != nil {
			return fmt.Errorf("mqtt: %w", err)
		}
	}

	var wg sync.WaitGroup
	tapDone := make(chan struct{})
	go func() {
		defer close(tapDone)
		taptap.Run(ctx, st, taptap.Config{
			Binary: env("TAPTAP_BIN", "taptap"), Host: host, Port: tapPort,
			StateFile: filepath.Join(stateDir, "taptap.json"),
		})
	}()
	if mq != nil {
		wg.Add(1)
		go func() { defer wg.Done(); mq.Run(ctx) }()
	}

	tick, save := time.NewTicker(10*time.Second), time.Now()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			st.Tick(state.Now())
			if time.Since(save) >= 60*time.Second {
				st.SaveEnergy()
				save = time.Now()
			}
		}
	}
	tick.Stop()

	slog.Info("shutting down")
	<-tapDone // SIGTERM to taptap, wait for it
	wg.Wait()
	if mq != nil {
		mq.Close() // publishes "offline" (retained)
	}
	st.SaveEnergy()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
