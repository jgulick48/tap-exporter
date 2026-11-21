# tap-exporter

Tigo TS4 optimizer data from the CCA <-> TAP RS485 bus, read by an RS485-to-TCP
adapter wired in parallel with the bus. It runs
[taptap](https://github.com/litinoveweedle/taptap) (built from source into the
image; read-only on TCP) and publishes what it decodes:

- `/metrics` (Prometheus): per optimizer `tigo_power_watts`, `tigo_voltage_in_volts`,
  `tigo_voltage_out_volts`, `tigo_current_in_amps`, `tigo_current_out_amps`,
  `tigo_temperature_celsius`, `tigo_duty_cycle_ratio`, `tigo_rssi`, `tigo_online`,
  `tigo_energy_wh_total`; array `tigo_array_power_watts`, `tigo_array_energy_wh_total`;
  estimated gain (see below) `tigo_optimizer_gain_watts`, `tigo_optimizer_gain_wh_total`,
  `tigo_array_unoptimized_power_watts`, `tigo_array_optimizer_gain_watts`,
  `tigo_array_optimizer_gain_wh_total`, and per `string` label `tigo_string_power_watts`,
  `tigo_string_unoptimized_power_watts`, `tigo_string_optimizer_gain_watts`,
  `tigo_string_current_amps`;
  health `tigo_adapter_connected`, `tigo_taptap_up`, `tigo_events_total{type}`.
- MQTT with Home Assistant discovery: a "Tigo array" device (power, energy, optimizers
  online, last report) and one device per optimizer. TLS client certificates supported.
- `/state` (JSON) and `/healthz`.

Optimizers stop reporting at night; after `NODE_TIMEOUT` (180 s) without a report one
counts as offline and its power as 0. Energy is integrated from the reports (gaps over
2 minutes are skipped) and kept in `STATE_DIR` with taptap's topology file, which is
where optimizer barcodes are learned (slowly, mostly at night).

## Configuration

| Variable | Default | |
|---|---|---|
| `TAPTAP_HOST`, `TAPTAP_PORT` | -, 4196 | the RS485-to-TCP adapter (raw TCP server, 38400 8N1) |
| `STATE_DIR` | `/data` | taptap.json (topology) and energy.json |
| `LISTEN_ADDRESS` | `:9812` | |
| `NODE_TIMEOUT` | 180 | seconds |
| `NODE_NAMES` | | `BARCODE=Name,...` or `gateway/node=Name` |
| `STRINGS` | (one string "all") | series strings for the gain estimate: `Name=ID,ID;Name2=ID,...`, ID = barcode or `gateway/node`; unlisted optimizers go in "unassigned" |
| `MQTT_HOST` | (off) | |
| `MQTT_PORT` | 8883 | TLS on 8883, or when a cert/CA is set |
| `MQTT_SERVER_NAME` | `MQTT_HOST` | certificate name to verify |
| `MQTT_CA_FILE`, `MQTT_CERT_FILE`, `MQTT_KEY_FILE` | | |
| `MQTT_USER`, `MQTT_PASSWORD` | | |
| `MQTT_CLIENT_ID`, `MQTT_TOPIC`, `DISCOVERY_PREFIX` | tap-exporter, tigo, homeassistant | |
| `MQTT_INTERVAL` | 15 | seconds between state updates |

## Optimizer gain (estimate)

The exporter estimates the power the optimizers recover compared with a plain string with
no optimizers. Per series string it takes each online optimizer's latest panel operating
point (input voltage `V_i`, input current `I_i`; each TS4 tracks its panel's MPP). Without
optimizers every panel carries the same string current `I`: a panel with `I_i >= I`
contributes `V_i * I` (voltage held at its MPP voltage, which is conservative), a panel
with less is bypassed and contributes 0. The inverter is assumed to pick the best `I` among
the panels' currents, giving the unoptimized power `P_unopt` and string current `I*`. An
optimizer's gain is `max(0, output power - unoptimized contribution)`; string and array
gain are sums, and gain energy is integrated like the array energy (kept in `energy.json`).
Offline optimizers are ignored.

Assumptions: panels are in series strings; parallel strings on one MPPT and inverter MPPT
voltage/current limits are not modelled. Set `STRINGS` when you have more than one string
(e.g. `East=4-A1B2C3D,4609/3;West=...`), otherwise all optimizers are treated as one string
and the gain is overstated.

Home Assistant: the array device gets "Optimizer gain", "Optimizer gain energy" and
"Unoptimized power estimate"; each optimizer gets "Optimizer gain", "Optimizer gain energy"
and a diagnostic "Online" binary sensor (connectivity). `/state` includes `strings`,
`array_gain_power`, `array_unoptimized_power`, `array_gain_wh` and per node `string`,
`gain_power`, `gain_wh`.

## Build and test

tap-exporter is written in Go (the `taptap` binary it supervises is built from Rust source
in the image).

```sh
make test          # go test ./...
make docker.push   # build and push the multi-arch image, then tag the commit
```

`make docker.push` pushes `docker.io/jgulick48/tap-exporter` with a UTC date tag
(`YYYY.MM.DD-HHMM`, also `latest`) and then pushes a matching git tag to `origin`. It
refuses to run with uncommitted changes to tracked files, so the image matches the tagged
commit.

CI (`.github/workflows/ci.yml`) runs the tests and builds the image on amd64 and arm64;
it doesn't push.
