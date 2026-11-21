import json
import os
import tempfile
import unittest

import tap_exporter as te

T0 = 1_800_000_000.0


def pr(node, vin=35.0, vout=34.5, cur=8.0, ts=T0, gw=4609):
    from datetime import datetime, timezone
    return json.dumps({"event_type": "power_report", "gateway": gw, "node": node,
                       "timestamp": datetime.fromtimestamp(ts, timezone.utc).isoformat(),
                       "voltage_in": vin, "voltage_out": vout, "current": cur,
                       "dc_dc_duty_cycle": 1.0, "temperature": 12.3, "rssi": 150})


class T(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.s = te.State(self.d, 180, {"4-A1B2C3D": "East 1", "4609/3": "West 3"})

    def test_power_and_energy(self):
        self.s.handle(pr(2, ts=T0), now=T0)
        self.s.handle(pr(2, cur=10.0, ts=T0 + 60), now=T0 + 60)
        snap = self.s.snapshot(now=T0 + 61)
        n = snap["nodes"][0]
        self.assertTrue(n["online"])
        self.assertAlmostEqual(n["power"], 345.0)
        self.assertAlmostEqual(n["values"]["current_in"], 345.0 / 35.0)
        # trapezoid: (276 + 345)/2 W for 60 s
        self.assertAlmostEqual(n["energy_wh"], (276 + 345) / 2 / 60, places=6)
        self.assertAlmostEqual(snap["array_power"], 345.0)
        self.assertEqual(n["name"], "Node 4609/2")

    def test_gap_not_integrated_and_offline(self):
        self.s.handle(pr(2, ts=T0), now=T0)
        self.s.handle(pr(2, ts=T0 + 3600), now=T0 + 3600)
        self.assertEqual(self.s.snapshot(now=T0 + 3600)["nodes"][0]["energy_wh"], 0.0)
        snap = self.s.snapshot(now=T0 + 3600 + 181)
        self.assertFalse(snap["nodes"][0]["online"])
        self.assertEqual(snap["array_power"], 0.0)
        self.assertEqual(snap["nodes"][0]["values"], {})

    def test_clock_skew_uses_arrival(self):
        self.s.handle(pr(5, ts=T0 + 86400), now=T0)
        self.assertEqual(self.s.snapshot(now=T0)["nodes"][0]["ts"], T0)

    def test_infrastructure_names_and_bad(self):
        self.s.handle(pr(3), now=T0)
        self.s.handle(json.dumps({"event_type": "infrastructure_report",
                                  "gateways": {"4609": {"address": "04:C0:5B:30:00:01:02:03", "version": "x"}},
                                  "nodes": {"4609": {"2": {"address": "04:C0:5B:40:00:A1:B2:C3", "barcode": "4-A1B2C3D"}}}}),
                      now=T0)
        self.s.handle('{"event_type":"power_report","gateway":1}', now=T0)
        self.s.handle("garbage", now=T0)
        snap = self.s.snapshot(now=T0)
        names = {x["key"]: x["name"] for x in snap["nodes"]}
        self.assertEqual(names, {"4609/2": "East 1", "4609/3": "West 3"})
        self.assertEqual(snap["events"], {"power_report": 1, "infrastructure_report": 1, "other": 0, "bad": 1})

    def test_array_energy_falls_to_zero(self):
        self.s.handle(pr(2, ts=T0), now=T0)
        self.s.tick(now=T0 + 100)
        self.s.tick(now=T0 + 200)   # node stale at +181: array power -> 0
        self.assertAlmostEqual(self.s.array_energy_wh, 276 * 100 / 3600 + 276 * 100 / 3600 / 2 * 1, places=6)

    def test_energy_persist(self):
        self.s.handle(pr(2, ts=T0), now=T0)
        self.s.handle(pr(2, ts=T0 + 60), now=T0 + 60)
        self.s.save_energy()
        s2 = te.State(self.d, 180, {})
        self.assertAlmostEqual(s2.nodes["4609/2"].energy_wh, self.s.nodes["4609/2"].energy_wh)
        s2.handle(pr(2, ts=T0 + 120), now=T0 + 120)  # first report after restart: no integration
        self.assertAlmostEqual(s2.nodes["4609/2"].energy_wh, self.s.nodes["4609/2"].energy_wh)

    def test_metrics_render(self):
        self.s.handle(pr(2), now=T0)
        txt = te.render_metrics(self.s.snapshot(now=T0))
        self.assertIn('tigo_power_watts{gateway="4609",node="2",barcode="",name="Node 4609/2"} 276', txt)
        self.assertIn("tigo_array_power_watts 276", txt)
        self.assertIn('tigo_nodes{state="online"} 1', txt)
        for line in txt.splitlines():
            if not line.startswith("#"):
                self.assertEqual(len(line.rsplit(" ", 1)), 2, line)


class FakeClient:
    def __init__(self):
        self.pub = {}

    def publish(self, topic, payload, qos=0, retain=False):
        self.pub[topic] = payload


class TM(unittest.TestCase):
    def test_announce_and_publish(self):
        s = te.State(tempfile.mkdtemp(), 180, {})
        s.handle(pr(2), now=T0)
        m = te.Mqtt.__new__(te.Mqtt)
        m.c, m.topic, m.disc, m.avail, m.announced = FakeClient(), "tigo", "homeassistant", "tigo/status", set()
        snap = s.snapshot(now=T0)
        m.announce(snap)
        m.publish(snap)
        cfg = json.loads(m.c.pub["homeassistant/sensor/tigo_g4609n2_power/config"])
        self.assertEqual(cfg["state_topic"], "tigo/node/g4609n2")
        self.assertEqual(cfg["device"]["via_device"], "tigo_tap")
        st = json.loads(m.c.pub["tigo/node/g4609n2"])
        self.assertEqual(st["power"], 276.0)
        self.assertEqual(st["rssi"], 150)
        arr = json.loads(m.c.pub["tigo/array"])
        self.assertEqual(arr["online"], 1)
        self.assertIn("homeassistant/sensor/tigo_array_energy/config", m.c.pub)


if __name__ == "__main__":
    unittest.main()
