"""End-to-end recovery test: real serial code over a pty, no EMU hardware.

Covers the two failure modes seen in practice: the EMU going quiet, and the EMU
coming back as a different device node after being replugged.
"""

import os
import threading
import time
import unittest

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import emu2influx

DEMAND_TEMPLATE = """<InstantaneousDemand>
  <DeviceMacId>0xd8d5b9000000f8bc</DeviceMacId>
  <MeterMacId>0x00135003005dba2b</MeterMacId>
  <TimeStamp>{timestamp}</TimeStamp>
  <Demand>0x00014d</Demand>
  <Multiplier>0x00000001</Multiplier>
  <Divisor>0x000003e8</Divisor>
  <DigitsRight>0x03</DigitsRight>
  <DigitsLeft>0x0f</DigitsLeft>
  <SuppressLeadingZero>Y</SuppressLeadingZero>
</InstantaneousDemand>
"""

FIRST_TIMESTAMP = 0x234E5AF3

POLL_INTERVAL = 0.2
NUDGE_AFTER = 0.5
RECONNECT_AFTER = 1
EXIT_AFTER = 8


class RecordingInflux:
    def __init__(self):
        self.points = []
        self.lock = threading.Lock()

    def write_points(self, points, **kwargs):
        with self.lock:
            self.points.extend(points)

    def count(self):
        with self.lock:
            return len(self.points)


def wait_until(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.port_spec = str(Path(self.directory.name, "ttyACM*"))
        self.timestamp = FIRST_TIMESTAMP
        self.db = RecordingInflux()
        self.result = []
        self.open_descriptors = []
        self.addCleanup(self.close_descriptors)

        patches = [
            mock.patch.object(emu2influx, "POLL_INTERVAL", POLL_INTERVAL),
            mock.patch.object(emu2influx.emu.emu, "timeout", 0.2),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)

    def plug_in(self, name):
        """Create a pty and expose it under the device name the app will find."""
        master, slave = os.openpty()
        link = Path(self.directory.name, name)
        link.symlink_to(os.ttyname(slave))
        self.open_descriptors += [master, slave]
        return master, link

    def close_descriptors(self):
        for descriptor in self.open_descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass

    def push_reading(self, master):
        self.timestamp += 1
        os.write(master, DEMAND_TEMPLATE.format(timestamp=hex(self.timestamp)).encode())

    def push_until_recorded(self, master, previous_count, timeout=20):
        """Push readings until at least one lands in InfluxDB."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.push_reading(master)
            if wait_until(lambda: self.db.count() > previous_count, timeout=1):
                return True
        return False

    def start_main(self):
        def run():
            self.result.append(
                emu2influx.main(
                    port_spec=self.port_spec,
                    db=self.db,
                    nudge_after=NUDGE_AFTER,
                    reconnect_after=RECONNECT_AFTER,
                    exit_after=EXIT_AFTER,
                )
            )

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        return thread

    def test_recovers_from_replug_then_exits_when_device_stays_quiet(self):
        first_master, first_link = self.plug_in("ttyACM0")
        thread = self.start_main()

        self.assertTrue(self.push_until_recorded(first_master, 0),
                        "no readings recorded from the original device")

        # Replug: the old node disappears and the EMU comes back as ttyACM1.
        recorded = self.db.count()
        first_link.unlink()
        os.close(first_master)
        second_master, _ = self.plug_in("ttyACM1")

        self.assertTrue(self.push_until_recorded(second_master, recorded),
                        "did not recover after the device was replugged")

        # Once the EMU stops reporting entirely, exit nonzero so the container
        # runtime restarts the service.
        thread.join(timeout=EXIT_AFTER * 4)
        self.assertFalse(thread.is_alive())
        self.assertEqual([1], self.result)


if __name__ == "__main__":
    unittest.main()
