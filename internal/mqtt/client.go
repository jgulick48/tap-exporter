package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/jgulick48/tap-exporter/internal/state"
)

// Config is the MQTT connection configuration.
type Config struct {
	Host       string
	Port       int
	ServerName string // TLS name to verify (default Host)
	CAFile     string
	CertFile   string
	KeyFile    string
	User       string
	Password   string
	ClientID   string
	Topic      string
	Discovery  string
	Interval   time.Duration
}

// UseTLS: TLS when a cert/CA is set or on port 8883.
func (c Config) UseTLS() bool { return c.CertFile != "" || c.CAFile != "" || c.Port == 8883 }

// TLSConfig builds the client TLS configuration.
func (c Config) TLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", c.CAFile)
		}
		cfg.RootCAs = pool
	}
	if c.CertFile != "" {
		key := c.KeyFile
		if key == "" {
			key = c.CertFile
		}
		cert, err := tls.LoadX509KeyPair(c.CertFile, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	// Connect to the in-cluster Service but verify the broker's certificate name.
	if c.ServerName != "" {
		cfg.ServerName = c.ServerName
	}
	return cfg, nil
}

type pahoPublisher struct{ c paho.Client }

func (p pahoPublisher) Publish(topic, payload string, qos byte, retain bool) {
	p.c.Publish(topic, qos, retain, payload)
}

// Client connects to the broker and keeps Home Assistant up to date.
type Client struct {
	cfg Config
	st  *state.State
	c   paho.Client
	a   *Announcer
}

// New builds the client (it does not connect until Run).
func New(cfg Config, st *state.State) (*Client, error) {
	m := &Client{cfg: cfg, st: st}
	m.a = NewAnnouncer(nil, cfg.Topic, cfg.Discovery)
	scheme := "tcp"
	opts := paho.NewClientOptions()
	if cfg.UseTLS() {
		tc, err := cfg.TLSConfig()
		if err != nil {
			return nil, err
		}
		scheme = "ssl"
		opts.SetTLSConfig(tc)
	}
	opts.AddBroker(fmt.Sprintf("%s://%s", scheme, hostPort(cfg.Host, cfg.Port)))
	opts.SetClientID(cfg.ClientID)
	if cfg.User != "" {
		opts.SetUsername(cfg.User).SetPassword(cfg.Password)
	}
	opts.SetWill(m.a.Avail, "offline", 1, true)
	opts.SetKeepAlive(60 * time.Second)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetMaxReconnectInterval(120 * time.Second)
	opts.SetOrderMatters(false)
	opts.SetOnConnectHandler(m.onConnect)
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) { slog.Warn("MQTT connection lost", "err", err) })
	m.c = paho.NewClient(opts)
	m.a.Pub = pahoPublisher{m.c}
	return m, nil
}

func hostPort(host string, port int) string {
	return host + ":" + strconv.Itoa(port)
}

func (m *Client) onConnect(c paho.Client) {
	slog.Info("MQTT connected")
	m.a.Reset()
	c.Publish(m.a.Avail, 1, true, "online")
	c.Subscribe(m.cfg.Discovery+"/status", 0, m.onMessage)
	m.st.MarkChanged()
}

func (m *Client) onMessage(_ paho.Client, msg paho.Message) {
	if msg.Topic() == m.cfg.Discovery+"/status" && string(msg.Payload()) == "online" {
		slog.Info("Home Assistant restarted: announcing again")
		m.a.Reset()
		m.st.MarkChanged()
	}
}

// Run connects (retrying in the background) and publishes state every Interval until ctx is done.
func (m *Client) Run(ctx context.Context) {
	m.c.Connect() // non-blocking; ConnectRetry keeps trying
	for {
		if m.c.IsConnectionOpen() {
			s := m.st.Snapshot(state.Now())
			if m.st.TakeChanged() {
				m.a.Announce(s)
			}
			m.a.Publish(s)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.cfg.Interval):
		}
	}
}

// Close publishes "offline" (retained) and disconnects.
func (m *Client) Close() {
	if m.c.IsConnectionOpen() {
		m.c.Publish(m.a.Avail, 1, true, "offline").WaitTimeout(3 * time.Second)
	}
	m.c.Disconnect(250)
}
