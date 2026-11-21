// Package taptap supervises the `taptap observe` subprocess.
package taptap

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jgulick48/tap-exporter/internal/state"
)

// Config says how to run taptap.
type Config struct {
	Binary    string
	Host      string
	Port      int
	StateFile string
}

func (c Config) args() []string {
	return []string{"observe", "--tcp", c.Host, "--port", strconv.Itoa(c.Port), "--state-file", c.StateFile,
		"--reconnect-timeout", "120", "--reconnect-delay", "10"}
}

// killGrace is how long taptap gets after SIGTERM before it is killed.
const killGrace = 5 * time.Second

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Run starts taptap and restarts it with backoff until ctx is done; on cancel it
// sends SIGTERM and returns once the process has exited.
func Run(ctx context.Context, st *state.State, cfg Config) {
	args := cfg.args()
	env := os.Environ()
	if os.Getenv("RUST_LOG") == "" {
		env = append(env, "RUST_LOG=info")
	}
	backoff := 5 * time.Second
	for ctx.Err() == nil {
		slog.Info("starting taptap", "cmd", cfg.Binary+" "+strings.Join(args, " "))
		cmd := exec.Command(cfg.Binary, args...)
		cmd.Env = env
		stdout, err1 := cmd.StdoutPipe()
		stderr, err2 := cmd.StderrPipe()
		if err1 == nil && err2 == nil {
			err1 = cmd.Start()
		}
		if err1 != nil || err2 != nil {
			if err1 == nil {
				err1 = err2
			}
			slog.Error("cannot start taptap", "err", err1)
			sleep(ctx, 30*time.Second)
			continue
		}
		st.SetTaptapRunning(true)
		started := time.Now()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); readStderr(st, stderr) }()

		exited := make(chan struct{})
		go func() { // stop: SIGTERM, then SIGKILL if taptap lingers
			select {
			case <-exited:
				return
			case <-ctx.Done():
			}
			cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-exited:
			case <-time.After(killGrace):
				cmd.Process.Kill()
			}
		}()

		readStdout(st, stdout)
		wg.Wait()
		waitErr := cmd.Wait()
		close(exited)
		st.TaptapStopped()
		if ctx.Err() != nil {
			return
		}
		st.IncRestarts()
		if time.Since(started) > 300*time.Second {
			backoff = 5 * time.Second
		} else {
			backoff = min(backoff*2, 300*time.Second)
		}
		slog.Warn("taptap exited, restarting", "err", waitErr, "in", backoff)
		sleep(ctx, backoff)
	}
}

func readLines(r io.Reader, fn func(string)) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			fn(line)
		}
		if err != nil {
			return
		}
	}
}

func readStdout(st *state.State, r io.Reader) {
	readLines(r, func(line string) {
		if line = strings.TrimSpace(line); line != "" {
			st.Handle(line, state.Now())
		}
	})
}

func readStderr(st *state.State, r io.Reader) {
	readLines(r, func(line string) { handleStderrLine(st, line) })
}

// handleStderrLine tracks the adapter-connected state from taptap's log output.
func handleStderrLine(st *state.State, line string) {
	line = strings.TrimRight(line, " \t\r\n")
	if line == "" {
		return
	}
	low := strings.ToLower(line)
	switch {
	case strings.Contains(low, "entering read loop") || strings.Contains(low, "source opened"):
		st.SetConnected(true)
	case strings.Contains(low, "error opening source") || strings.Contains(low, "reconnecting"):
		st.SetConnected(false)
	}
	slog.Info("taptap: " + line)
}
