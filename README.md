# tap-exporter

Tigo TS4 optimizer data from the CCA <-> TAP RS485 bus, read by an RS485-to-TCP
adapter wired in parallel with the bus. It runs
[taptap](https://github.com/litinoveweedle/taptap) (built from source into the
image; read-only on TCP) and publishes what it decodes:

- `/metrics` (Prometheus): per optimizer `tigo_power_watts`, `tigo_voltage_in_volts`,
  `tigo_voltage_out_volts`, `tigo_current_in_amps`, `tigo_current_out_amps`,
  `tigo_temperature_celsius`, `tigo_duty_cycle_ratio`, `tigo_rssi`, `tigo_online`,
  `tigo_energy_wh_total`; array `tigo_array_power_watts`, `tigo_array_energy_wh_total`;
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
| `MQTT_HOST` | (off) | |
| `MQTT_PORT` | 8883 | TLS on 8883, or when a cert/CA is set |
| `MQTT_SERVER_NAME` | `MQTT_HOST` | certificate name to verify |
| `MQTT_CA_FILE`, `MQTT_CERT_FILE`, `MQTT_KEY_FILE` | | |
| `MQTT_USER`, `MQTT_PASSWORD` | | |
| `MQTT_CLIENT_ID`, `MQTT_TOPIC`, `DISCOVERY_PREFIX` | tap-exporter, tigo, homeassistant | |
| `MQTT_INTERVAL` | 15 | seconds between state updates |

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
