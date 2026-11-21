#!/usr/bin/env python3
"""tap-exporter: Tigo TS4 optimizer data from the TAP RS485 bus.

Runs `taptap observe` (github.com/litinoveweedle/taptap, a fork of
willglynn/taptap) against an RS485-to-TCP adapter wired in parallel with the
CCA <-> TAP bus. taptap only listens (its TCP mode is read-only), decodes the
optimizers' power reports and prints them as JSON lines, which this program
turns into:

  * Prometheus /metrics (per optimizer and array totals, energy counters),
  * MQTT with Home Assistant discovery (one device for the array, one per
    optimizer), over TLS with a client certificate,
  * /healthz for Kubernetes.

Optimizers stop reporting at night; after NODE_TIMEOUT seconds without a
report an optimizer counts as offline and its power as 0.

Configuration (environment):
  TAPTAP_HOST, TAPTAP_PORT (4196)   the adapter
  TAPTAP_BIN (taptap), STATE_DIR (/data): taptap's topology file and the
                                    energy counters are kept there
  LISTEN_ADDRESS (:9812)
  NODE_TIMEOUT (180)                seconds
  NODE_NAMES                        optional "BARCODE=Name,BARCODE=Name" or
                                    "gateway/node=Name"
  MQTT_HOST (empty = MQTT off), MQTT_PORT (8883), MQTT_SERVER_NAME (TLS name
  to verify, default MQTT_HOST), MQTT_CA_FILE, MQTT_CERT_FILE, MQTT_KEY_FILE,
  MQTT_CLIENT_ID (tap-exporter), MQTT_TOPIC (tigo), DISCOVERY_PREFIX
  (homeassistant), MQTT_INTERVAL (15) seconds between state updates
"""
import json
import logging
import os
import signal
import ssl
import subprocess
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("tap-exporter")

# Wh accumulated only across gaps shorter than this (longer = missing data).
MAX_INTEGRATION_GAP = 120.0


def env(name, default=None):
    v = os.environ.get(name)
    return default if v is None or v == "" else v


def parse_names(spec):
    out = {}
    for part in (spec or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip() and v.strip():
                out[k.strip()] = v.strip()
    return out


def parse_ts(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except (AttributeError, ValueError):
        return None


class Node:
    __slots__ = ("gateway", "node", "barcode", "address", "values", "ts", "reports", "energy_wh", "power")

    def __init__(self, gateway, node):
        self.gateway, self.node = gateway, node
        self.barcode = self.address = None
        self.values = {}
        self.ts = 0.0
        self.reports = 0
        self.energy_wh = 0.0
        self.power = 0.0

    @property
    def key(self):
        return f"{self.gateway}/{self.node}"

    @property
    def uid(self):
        return f"g{self.gateway}n{self.node}"


class State:
    """Everything known about the bus. All access under self.lock."""

    def __init__(self, state_dir, timeout, names):
        self.lock = threading.Lock()
        self.nodes = {}
        self.gateways = {}
        self.timeout = timeout
        self.names = names
        self.energy_file = os.path.join(state_dir, "energy.json")
        self.array_energy_wh = 0.0
        self.array_power = 0.0
        self.array_ts = 0.0
        self.events = {"power_report": 0, "infrastructure_report": 0, "other": 0, "bad": 0}
        self.last_event = 0.0
        self.taptap_running = False
        self.connected = False
        self.restarts = 0
        self.changed = threading.Event()  # set when the node list / names change (re-announce to HA)
        self._load_energy()

    # ---- persistence -------------------------------------------------------
    def _load_energy(self):
        try:
            with open(self.energy_file) as f:
                d = json.load(f)
        except (OSError, ValueError):
            return
        self.array_energy_wh = float(d.get("array_wh", 0.0))
        for key, wh in (d.get("nodes") or {}).items():
            try:
                gw, nd = (int(x) for x in key.split("/"))
            except ValueError:
                continue
            n = self.nodes.setdefault(key, Node(gw, nd))
            n.energy_wh = float(wh)

    def save_energy(self):
        with self.lock:
            d = {"array_wh": self.array_energy_wh, "nodes": {k: n.energy_wh for k, n in self.nodes.items()}}
        tmp = self.energy_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(d, f)
            os.replace(tmp, self.energy_file)
        except OSError as e:
            log.warning("saving energy counters failed: %s", e)

    # ---- events ------------------------------------------------------------
    def name_of(self, n):
        return self.names.get(n.barcode or "") or self.names.get(n.key) or (n.barcode or f"Node {n.key}")

    def handle(self, line, now=None):
        now = time.time() if now is None else now
        try:
            ev = json.loads(line)
        except ValueError:
            log.debug("not JSON: %s", line[:200])
            return
        if not isinstance(ev, dict):
            return
        kind = ev.get("event_type")
        with self.lock:
            self.last_event = now
            self.connected = True
            if kind == "power_report":
                if self._power(ev, now):
                    self.events["power_report"] += 1
                else:
                    self.events["bad"] += 1
            elif kind == "infrastructure_report":
                self.events["infrastructure_report"] += 1
                self._infrastructure(ev)
            else:
                self.events["other"] += 1

    def _power(self, ev, now):
        try:
            gw, nd = int(ev["gateway"]), int(ev["node"])
            vin, vout = float(ev["voltage_in"]), float(ev["voltage_out"])
            cur, duty = float(ev["current"]), float(ev["dc_dc_duty_cycle"])
            temp, rssi = float(ev["temperature"]), int(ev["rssi"])
        except (KeyError, TypeError, ValueError):
            log.warning("bad power report: %s", ev)
            return False
        ts = parse_ts(ev.get("timestamp")) or now
        if ts > now + 60 or ts < now - 600:  # taptap's slot clock is off; use arrival time
            ts = now
        key = f"{gw}/{nd}"
        n = self.nodes.get(key)
        if n is None:
            n = self.nodes[key] = Node(gw, nd)
            self.changed.set()
        power = vout * cur
        if n.ts and 0 < ts - n.ts <= MAX_INTEGRATION_GAP:
            n.energy_wh += (n.power + power) / 2 * (ts - n.ts) / 3600.0
        if ts >= n.ts:
            n.ts = ts
            n.power = power
            n.values = {
                "voltage_in": vin, "voltage_out": vout, "current_out": cur,
                "current_in": power / vin if vin > 0.5 else 0.0,
                "power": power, "duty_cycle": duty, "temperature": temp, "rssi": rssi,
            }
        n.reports += 1
        self._array_update(now)
        return True

    def _infrastructure(self, ev):
        for gw, g in (ev.get("gateways") or {}).items():
            if isinstance(g, dict):
                self.gateways[str(gw)] = {"address": g.get("address"), "version": g.get("version")}
        for gw, nodes in (ev.get("nodes") or {}).items():
            if not isinstance(nodes, dict):
                continue
            for nd, info in nodes.items():
                try:
                    key = f"{int(gw)}/{int(nd)}"
                except ValueError:
                    continue
                n = self.nodes.get(key) or self.nodes.setdefault(key, Node(int(gw), int(nd)))
                info = info if isinstance(info, dict) else {}
                if info.get("barcode") and info.get("barcode") != n.barcode:
                    n.barcode = info.get("barcode")
                    self.changed.set()
                n.address = info.get("address") or n.address

    def _array_update(self, now):
        """Array power = sum of online optimizers; integrate it for array energy."""
        p = sum(n.power for n in self.nodes.values() if n.ts and now - n.ts <= self.timeout)
        if self.array_ts and 0 < now - self.array_ts <= MAX_INTEGRATION_GAP:
            self.array_energy_wh += (self.array_power + p) / 2 * (now - self.array_ts) / 3600.0
        self.array_power, self.array_ts = p, now

    def tick(self, now=None):
        """Called periodically: lets the array power fall to 0 when reports stop."""
        now = time.time() if now is None else now
        with self.lock:
            self._array_update(now)

    def online(self, n, now):
        return bool(n.ts) and now - n.ts <= self.timeout

    # ---- outputs -----------------------------------------------------------
    def snapshot(self, now=None):
        now = time.time() if now is None else now
        with self.lock:
            nodes = []
            for n in sorted(self.nodes.values(), key=lambda x: (x.gateway, x.node)):
                on = self.online(n, now)
                nodes.append({
                    "key": n.key, "uid": n.uid, "gateway": n.gateway, "node": n.node,
                    "barcode": n.barcode, "name": self.name_of(n), "online": on,
                    "values": dict(n.values) if on else {}, "power": n.power if on else 0.0,
                    "energy_wh": n.energy_wh, "ts": n.ts, "reports": n.reports,
                })
            return {
                "now": now, "nodes": nodes,
                "array_power": sum(x["power"] for x in nodes),
                "array_energy_wh": self.array_energy_wh,
                "online": sum(1 for x in nodes if x["online"]),
                "events": dict(self.events), "last_event": self.last_event,
                "taptap_running": self.taptap_running, "connected": self.connected,
                "restarts": self.restarts, "gateways": dict(self.gateways),
            }


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------
METRICS = [  # (value key, metric, help)
    ("voltage_in", "tigo_voltage_in_volts", "Panel (optimizer input) voltage."),
    ("voltage_out", "tigo_voltage_out_volts", "Optimizer output voltage."),
    ("current_out", "tigo_current_out_amps", "Optimizer output (string) current."),
    ("current_in", "tigo_current_in_amps", "Panel current, from power / input voltage."),
    ("power", "tigo_power_watts", "Optimizer output power."),
    ("duty_cycle", "tigo_duty_cycle_ratio", "DC-DC converter duty cycle (1 = pass-through)."),
    ("temperature", "tigo_temperature_celsius", "Optimizer temperature."),
    ("rssi", "tigo_rssi", "Signal strength of the optimizer's last report as seen by the TAP."),
]


def esc(v):
    return str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_metrics(s):
    out = []

    def head(name, typ, help_):
        out.append(f"# HELP {name} {help_}")
        out.append(f"# TYPE {name} {typ}")

    def labels(x):
        return (f'gateway="{x["gateway"]}",node="{x["node"]}",'
                f'barcode="{esc(x["barcode"] or "")}",name="{esc(x["name"])}"')

    for key, name, help_ in METRICS:
        head(name, "gauge", help_)
        for x in s["nodes"]:
            if key in x["values"]:
                out.append(f'{name}{{{labels(x)}}} {x["values"][key]:.6g}')
    head("tigo_online", "gauge", "1 if the optimizer reported within NODE_TIMEOUT.")
    for x in s["nodes"]:
        out.append(f'tigo_online{{{labels(x)}}} {int(x["online"])}')
    head("tigo_last_report_timestamp_seconds", "gauge", "Time of the optimizer's last power report.")
    for x in s["nodes"]:
        if x["ts"]:
            out.append(f'tigo_last_report_timestamp_seconds{{{labels(x)}}} {x["ts"]:.3f}')
    head("tigo_reports_total", "counter", "Power reports received from the optimizer since start.")
    for x in s["nodes"]:
        out.append(f'tigo_reports_total{{{labels(x)}}} {x["reports"]}')
    head("tigo_energy_wh_total", "counter", "Energy from the optimizer, integrated from its reports.")
    for x in s["nodes"]:
        out.append(f'tigo_energy_wh_total{{{labels(x)}}} {x["energy_wh"]:.3f}')
    head("tigo_array_power_watts", "gauge", "Sum of the online optimizers' output power.")
    out.append(f'tigo_array_power_watts {s["array_power"]:.6g}')
    head("tigo_array_energy_wh_total", "counter", "Array energy, integrated from the array power.")
    out.append(f'tigo_array_energy_wh_total {s["array_energy_wh"]:.3f}')
    head("tigo_nodes", "gauge", "Optimizers known / online.")
    out.append(f'tigo_nodes{{state="known"}} {len(s["nodes"])}')
    out.append(f'tigo_nodes{{state="online"}} {s["online"]}')
    head("tigo_events_total", "counter", "taptap events by type (bad = power reports that failed to parse).")
    for k, v in s["events"].items():
        out.append(f'tigo_events_total{{type="{k}"}} {v}')
    head("tigo_last_event_timestamp_seconds", "gauge", "Time of the last event from taptap.")
    out.append(f'tigo_last_event_timestamp_seconds {s["last_event"]:.3f}')
    head("tigo_taptap_up", "gauge", "1 while the taptap process is running.")
    out.append(f'tigo_taptap_up {int(s["taptap_running"])}')
    head("tigo_adapter_connected", "gauge", "1 while taptap is connected to the RS485 adapter.")
    out.append(f'tigo_adapter_connected {int(s["connected"])}')
    head("tigo_taptap_restarts_total", "counter", "taptap restarts.")
    out.append(f'tigo_taptap_restarts_total {s["restarts"]}')
    return "\n".join(out) + "\n"


class Handler(BaseHTTPRequestHandler):
    state = None

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/metrics"):
            body, ctype, code = render_metrics(self.state.snapshot()).encode(), "text/plain; version=0.0.4", 200
        elif self.path.startswith("/healthz"):
            ok = self.state.taptap_running
            body, ctype, code = (b"ok\n" if ok else b"taptap not running\n"), "text/plain", 200 if ok else 503
        elif self.path.startswith("/state"):
            body, ctype, code = json.dumps(self.state.snapshot(), indent=1).encode(), "application/json", 200
        else:
            body, ctype, code = b"tap-exporter: /metrics /state /healthz\n", "text/plain", 200
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


# ---------------------------------------------------------------------------
# taptap
# ---------------------------------------------------------------------------
def run_taptap(state, stop, binary, host, port, state_file):
    cmd = [binary, "observe", "--tcp", host, "--port", str(port), "--state-file", state_file,
           "--reconnect-timeout", "120", "--reconnect-delay", "10"]
    envp = dict(os.environ, RUST_LOG=os.environ.get("RUST_LOG", "info"))
    backoff = 5
    while not stop.is_set():
        log.info("starting %s", " ".join(cmd))
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=envp, bufsize=1)
        except OSError as e:
            log.error("cannot start taptap: %s", e)
            stop.wait(30)
            continue
        state.taptap_running = True
        started = time.time()
        threading.Thread(target=_taptap_stderr, args=(state, p), daemon=True).start()
        for line in p.stdout:
            line = line.strip()
            if line:
                state.handle(line)
            if stop.is_set():
                break
        if stop.is_set():
            p.terminate()
        rc = p.wait()
        state.taptap_running = state.connected = False
        if stop.is_set():
            return
        state.restarts += 1
        backoff = 5 if time.time() - started > 300 else min(backoff * 2, 300)
        log.warning("taptap exited with %s, restarting in %ss", rc, backoff)
        stop.wait(backoff)


def _taptap_stderr(state, p):
    for line in p.stderr:
        line = line.rstrip()
        if not line:
            continue
        low = line.lower()
        if "entering read loop" in low or "source opened" in low:
            state.connected = True
        elif "error opening source" in low or "reconnecting" in low:
            state.connected = False
        log.info("taptap: %s", line)


# ---------------------------------------------------------------------------
# MQTT / Home Assistant
# ---------------------------------------------------------------------------
SENSORS = [  # key, name, unit, device_class, state_class, precision
    ("power", "Power", "W", "power", "measurement", 0),
    ("voltage_in", "Input voltage", "V", "voltage", "measurement", 1),
    ("voltage_out", "Output voltage", "V", "voltage", "measurement", 1),
    ("current_in", "Input current", "A", "current", "measurement", 2),
    ("current_out", "Output current", "A", "current", "measurement", 2),
    ("duty_cycle", "Duty cycle", "%", None, "measurement", 0),
    ("temperature", "Temperature", "°C", "temperature", "measurement", 1),
    ("rssi", "Signal", None, None, "measurement", 0),
]


class Mqtt:
    def __init__(self, state, stop):
        import paho.mqtt.client as mqtt  # only needed when MQTT is configured
        self.state, self.stop = state, stop
        self.topic = env("MQTT_TOPIC", "tigo")
        self.disc = env("DISCOVERY_PREFIX", "homeassistant")
        self.interval = float(env("MQTT_INTERVAL", "15"))
        self.avail = f"{self.topic}/status"
        self.announced = set()
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=env("MQTT_CLIENT_ID", "tap-exporter"))
        host = env("MQTT_HOST")
        port = int(env("MQTT_PORT", "8883"))
        if env("MQTT_CERT_FILE") or env("MQTT_CA_FILE") or port == 8883:
            ctx = ssl.create_default_context(cafile=env("MQTT_CA_FILE"))
            if env("MQTT_CERT_FILE"):
                ctx.load_cert_chain(env("MQTT_CERT_FILE"), env("MQTT_KEY_FILE"))
            self.c.tls_set_context(ctx)
            server_name = env("MQTT_SERVER_NAME")
            if server_name and server_name != host:
                # Connect to the in-cluster Service but verify the broker's certificate name.
                orig = ctx.wrap_socket
                ctx.wrap_socket = lambda sock, server_hostname=None, **kw: orig(sock, server_hostname=server_name, **kw)
        if env("MQTT_USER"):
            self.c.username_pw_set(env("MQTT_USER"), env("MQTT_PASSWORD"))
        self.c.will_set(self.avail, "offline", qos=1, retain=True)
        self.c.on_connect = self.on_connect
        self.c.on_message = self.on_message
        self.c.reconnect_delay_set(5, 120)
        self.host, self.port = host, port

    def on_connect(self, c, userdata, flags, rc, props=None):
        if rc != 0:
            log.warning("MQTT connect failed: %s", rc)
            return
        log.info("MQTT connected")
        self.announced.clear()
        c.publish(self.avail, "online", qos=1, retain=True)
        c.subscribe(f"{self.disc}/status")
        self.state.changed.set()

    def on_message(self, c, userdata, msg):
        if msg.topic == f"{self.disc}/status" and msg.payload == b"online":
            log.info("Home Assistant restarted: announcing again")
            self.announced.clear()
            self.state.changed.set()

    ARRAY_DEVICE = {"identifiers": ["tigo_tap"], "name": "Tigo array", "manufacturer": "Tigo",
                    "model": "TAP (read by taptap)"}

    def _sensor(self, uid, name, topic, tmpl, device, unit=None, dclass=None, sclass=None, prec=None, extra=None):
        cfg = {"name": name, "unique_id": uid, "object_id": uid, "state_topic": topic,
               "value_template": tmpl, "availability_topic": self.avail, "device": device}
        if unit:
            cfg["unit_of_measurement"] = unit
        if dclass:
            cfg["device_class"] = dclass
        if sclass:
            cfg["state_class"] = sclass
        if prec is not None:
            cfg["suggested_display_precision"] = prec
        cfg.update(extra or {})
        self.c.publish(f"{self.disc}/sensor/{uid}/config", json.dumps(cfg), qos=1, retain=True)

    def announce(self, s):
        dev = self.ARRAY_DEVICE
        if "array" not in self.announced:
            t = f"{self.topic}/array"
            self._sensor("tigo_array_power", "Power", t, "{{ value_json.power }}", dev, "W", "power", "measurement", 0)
            self._sensor("tigo_array_energy", "Energy", t, "{{ value_json.energy_kwh }}", dev, "kWh", "energy",
                         "total_increasing", 2)
            self._sensor("tigo_array_online", "Optimizers online", t, "{{ value_json.online }}", dev, None, None,
                         "measurement", 0)
            self._sensor("tigo_array_last_report", "Last report", t,
                         "{{ value_json.last_report }}", dev,
                         None, "timestamp", None, None, {"entity_category": "diagnostic"})
            self.announced.add("array")
        for x in s["nodes"]:
            sig = (x["uid"], x["name"])
            if sig in self.announced:
                continue
            ndev = {"identifiers": [f"tigo_{x['uid']}"], "name": f"Tigo {x['name']}", "manufacturer": "Tigo",
                    "model": "TS4 optimizer", "via_device": "tigo_tap"}
            if x["barcode"]:
                ndev["serial_number"] = x["barcode"]
            t = f"{self.topic}/node/{x['uid']}"
            for key, name, unit, dclass, sclass, prec in SENSORS:
                extra = {"entity_category": "diagnostic"} if key in ("rssi", "duty_cycle") else None
                tmpl = "{{ value_json.%s if value_json.%s is not none else None }}" % (key, key)
                if key == "power":
                    tmpl = "{{ value_json.power }}"
                if key == "duty_cycle":
                    tmpl = "{{ (value_json.duty_cycle * 100) | round(0) if value_json.duty_cycle is not none else None }}"
                self._sensor(f"tigo_{x['uid']}_{key}", name, t, tmpl, ndev, unit, dclass, sclass, prec, extra)
            self._sensor(f"tigo_{x['uid']}_energy", "Energy", t, "{{ value_json.energy_kwh }}", ndev, "kWh", "energy",
                         "total_increasing", 3)
            self.announced.add(sig)

    def publish(self, s):
        last = max((x["ts"] for x in s["nodes"]), default=0)
        self.c.publish(f"{self.topic}/array", json.dumps({
            "power": round(s["array_power"], 1), "energy_kwh": round(s["array_energy_wh"] / 1000, 4),
            "online": s["online"], "known": len(s["nodes"]),
            "last_report": datetime.fromtimestamp(last).astimezone().isoformat() if last else None,
        }), qos=0, retain=True)
        for x in s["nodes"]:
            v = x["values"]
            payload = {k: (round(v[k], 3) if k in v else None) for k, *_ in SENSORS}
            payload["power"] = round(x["power"], 1)
            payload["energy_kwh"] = round(x["energy_wh"] / 1000, 5)
            payload["online"] = x["online"]
            payload["barcode"] = x["barcode"]
            self.c.publish(f"{self.topic}/node/{x['uid']}", json.dumps(payload), qos=0, retain=True)

    def run(self):
        self.c.connect_async(self.host, self.port, keepalive=60)
        self.c.loop_start()
        while not self.stop.is_set():
            if self.c.is_connected():
                s = self.state.snapshot()
                if self.state.changed.is_set():
                    self.state.changed.clear()
                    self.announce(s)
                self.publish(s)
            self.stop.wait(self.interval)
        if self.c.is_connected():
            self.c.publish(self.avail, "offline", qos=1, retain=True).wait_for_publish(3)
        self.c.loop_stop()
        self.c.disconnect()


# ---------------------------------------------------------------------------
def main():
    logging.basicConfig(level=env("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s",
                        stream=sys.stdout)
    host = env("TAPTAP_HOST")
    if not host:
        sys.exit("TAPTAP_HOST is required")
    state_dir = env("STATE_DIR", "/data")
    os.makedirs(state_dir, exist_ok=True)
    state = State(state_dir, float(env("NODE_TIMEOUT", "180")), parse_names(env("NODE_NAMES")))
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *a: stop.set())

    listen = env("LISTEN_ADDRESS", ":9812")
    lhost, lport = listen.rsplit(":", 1)
    Handler.state = state
    httpd = ThreadingHTTPServer((lhost, int(lport)), Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    log.info("listening on %s", listen)

    threading.Thread(target=run_taptap, daemon=True, args=(
        state, stop, env("TAPTAP_BIN", "taptap"), host, int(env("TAPTAP_PORT", "4196")),
        os.path.join(state_dir, "taptap.json"))).start()
    if env("MQTT_HOST"):
        threading.Thread(target=Mqtt(state, stop).run, daemon=True).start()

    last_save = time.time()
    while not stop.wait(10):
        state.tick()
        if time.time() - last_save >= 60:
            state.save_energy()
            last_save = time.time()
    state.save_energy()
    httpd.shutdown()
    time.sleep(1)


if __name__ == "__main__":
    main()
