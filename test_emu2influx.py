import unittest

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from xml.etree.ElementTree import fromstring

import emu2influx

from api_classes import CurrentSummationDelivered, InstantaneousDemand, PriceCluster

PRICE_CLUSTER = """
<PriceCluster>
  <DeviceMacId>0xd8d5b9000000f8bc</DeviceMacId>
  <MeterMacId>0x00135003005dba2b</MeterMacId>
  <TimeStamp>0x234e56bd</TimeStamp>
  <Price>0x0000025b</Price>
  <Currency>0x0348</Currency>
  <TrailingDigits>0x04</TrailingDigits>
  <Tier>0x00</Tier>
  <StartTime>0x234da0d0</StartTime>
  <Duration>0x05a0</Duration>
  <RateLabel></RateLabel>
</PriceCluster>"""

INSTANTANEOUS_DEMAND = """
<InstantaneousDemand>
  <DeviceMacId>0xd8d5b9000000f8bc</DeviceMacId>
  <MeterMacId>0x00135003005dba2b</MeterMacId>
  <TimeStamp>0x234e5af3</TimeStamp>
  <Demand>0x00014d</Demand>
  <Multiplier>0x00000001</Multiplier>
  <Divisor>0x000003e8</Divisor>
  <DigitsRight>0x03</DigitsRight>
  <DigitsLeft>0x0f</DigitsLeft>
  <SuppressLeadingZero>Y</SuppressLeadingZero>
</InstantaneousDemand>"""

CURRENT_SUMMATION_DELIVERED = """
<CurrentSummationDelivered>
  <DeviceMacId>0xd8d5b9000000f8bc</DeviceMacId>
  <MeterMacId>0x00135003005dba2b</MeterMacId>
  <TimeStamp>0x234e5b55</TimeStamp>
  <SummationDelivered>0x0000000002cb3ad2</SummationDelivered>
  <SummationReceived>0x0000000000000000</SummationReceived>
  <Multiplier>0x00000001</Multiplier>
  <Divisor>0x000003e8</Divisor>
  <DigitsRight>0x03</DigitsRight>
  <DigitsLeft>0x0f</DigitsLeft>
  <SuppressLeadingZero>Y</SuppressLeadingZero>
</CurrentSummationDelivered>"""


def parse(block_string, class_):
    return class_(fromstring(block_string), block_string)


class FakeClock:
    """Stand-in for the time module that advances only when asked to sleep."""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeThread:
    def __init__(self, alive=True):
        self.alive = alive

    def is_alive(self):
        return self.alive

    def join(self, timeout=None):
        self.alive = False


class FakeEmu:
    """EMU client that reports whatever messages the test hands it."""

    def __init__(self):
        self.thread_handle = FakeThread()
        self.serial_error = None
        self.update_requests = 0
        self.stopped = False

    def get_instantaneous_demand(self, refresh):
        self.update_requests += 1

    def get_current_summation_delivered(self):
        pass

    def get_price_blocks(self):
        pass

    def stop_serial(self):
        self.stopped = True


class FakeInflux:
    def __init__(self):
        self.points = []

    def write_points(self, points, **kwargs):
        self.points.extend(points)


class ParsingTest(unittest.TestCase):
    def test_price(self):
        self.assertEqual(0.0603, emu2influx.get_price(parse(PRICE_CLUSTER, PriceCluster)))

    def test_demand(self):
        demand = parse(INSTANTANEOUS_DEMAND, InstantaneousDemand)
        self.assertEqual(0.333, emu2influx.get_reading(demand.Demand, demand))

    def test_timestamp(self):
        summation = parse(CURRENT_SUMMATION_DELIVERED, CurrentSummationDelivered)
        self.assertEqual(
            datetime(2018, 10, 8, 18, 15, 49), emu2influx.get_timestamp(summation)
        )


class WaitForInfluxTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        clock_patch = mock.patch.object(emu2influx, "time", self.clock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)

    def test_retries_until_influx_is_up(self):
        db = FakeInflux()
        create = mock.Mock(side_effect=[IOError("refused"), IOError("refused"), None])
        db.create_database = create
        self.assertTrue(emu2influx.wait_for_influx(db, "rainforest", attempts=5))
        self.assertEqual(3, create.call_count)

    def test_gives_up_eventually(self):
        db = FakeInflux()
        db.create_database = mock.Mock(side_effect=IOError("refused"))
        with self.assertRaises(IOError):
            emu2influx.wait_for_influx(db, "rainforest", attempts=3)
        self.assertEqual(3, db.create_database.call_count)


class FindSerialPortTest(unittest.TestCase):
    def test_prefers_earlier_glob(self):
        with TemporaryDirectory() as directory:
            by_id = Path(directory, "by-id-Rainforest")
            by_id.touch()
            Path(directory, "ttyACM0").touch()
            globs = (str(Path(directory, "by-id-*")), str(Path(directory, "ttyACM*")))
            with mock.patch.object(emu2influx, "DEFAULT_PORT_GLOBS", globs):
                self.assertEqual(str(by_id), emu2influx.find_serial_port("auto"))

    def test_discovers_renumbered_device(self):
        with TemporaryDirectory() as directory:
            Path(directory, "ttyACM3").touch()
            globs = (str(Path(directory, "ttyACM*")),)
            with mock.patch.object(emu2influx, "DEFAULT_PORT_GLOBS", globs):
                self.assertEqual(
                    str(Path(directory, "ttyACM3")), emu2influx.find_serial_port("auto")
                )

    def test_no_match(self):
        with TemporaryDirectory() as directory:
            globs = (str(Path(directory, "ttyACM*")),)
            with mock.patch.object(emu2influx, "DEFAULT_PORT_GLOBS", globs):
                self.assertIsNone(emu2influx.find_serial_port("auto"))

    def test_explicit_port_is_qualified(self):
        self.assertIsNone(emu2influx.find_serial_port("definitely_not_a_device"))


class WriteNewPointsTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeEmu()
        self.client.InstantaneousDemand = parse(
            INSTANTANEOUS_DEMAND, InstantaneousDemand
        )
        self.db = FakeInflux()

    def test_writes_new_reading(self):
        last_timestamps = {}
        self.assertTrue(
            emu2influx.write_new_points(self.client, self.db, last_timestamps)
        )
        self.assertEqual(
            [{"measurement": "demand", "time": "2018-10-08T18:14:11",
              "fields": {"demand": 0.333}}],
            self.db.points,
        )
        self.assertEqual({"demand"}, set(last_timestamps))

    def test_skips_repeated_timestamp(self):
        last_timestamps = {}
        emu2influx.write_new_points(self.client, self.db, last_timestamps)
        self.assertFalse(
            emu2influx.write_new_points(self.client, self.db, last_timestamps)
        )
        self.assertEqual(1, len(self.db.points))

    def test_influx_failure_is_retried(self):
        last_timestamps = {}
        with mock.patch.object(self.db, "write_points", side_effect=IOError("down")):
            self.assertTrue(
                emu2influx.write_new_points(self.client, self.db, last_timestamps)
            )
        self.assertEqual({}, last_timestamps)

        self.assertTrue(
            emu2influx.write_new_points(self.client, self.db, last_timestamps)
        )
        self.assertEqual(1, len(self.db.points))

    def test_malformed_message_is_skipped(self):
        self.client.InstantaneousDemand = parse(
            INSTANTANEOUS_DEMAND.replace("0x00000001", "bogus"), InstantaneousDemand
        )
        self.assertFalse(
            emu2influx.write_new_points(self.client, self.db, {})
        )
        self.assertEqual([], self.db.points)


class MainTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.clients = []
        clock_patch = mock.patch.object(emu2influx, "time", self.clock)
        clock_patch.start()
        self.addCleanup(clock_patch.stop)

    def fake_connect(self, port_spec, discover_timeout=60, connect_timeout=30):
        client = FakeEmu()
        self.clients.append(client)
        return client

    def run_main(self, **kwargs):
        options = dict(nudge_after=120, reconnect_after=300, exit_after=900)
        options.update(kwargs)
        with mock.patch.object(emu2influx, "connect", self.fake_connect):
            return emu2influx.main(port_spec="auto", db=FakeInflux(), **options)

    def test_silent_emu_escalates_to_exit(self):
        self.assertEqual(1, self.run_main())
        # Re-requested updates before giving up, then reopened the port at least
        # once, and exited so the container restarts.
        self.assertGreaterEqual(self.clients[0].update_requests, 2)
        self.assertGreaterEqual(len(self.clients), 2)
        # Reconnects are throttled rather than attempted every poll.
        self.assertLessEqual(len(self.clients), 4)
        self.assertTrue(self.clients[0].stopped)

    def test_dead_serial_thread_reconnects_immediately(self):
        connects = []

        def connect(port_spec, discover_timeout=60, connect_timeout=30):
            client = self.fake_connect(port_spec)
            client.thread_handle.alive = False
            connects.append(self.clock.now)
            return client

        with mock.patch.object(emu2influx, "connect", connect):
            self.assertEqual(
                1,
                emu2influx.main(
                    port_spec="auto",
                    db=FakeInflux(),
                    nudge_after=120,
                    reconnect_after=300,
                    exit_after=60,
                ),
            )
        # One reconnect per poll interval, rather than waiting for the timeout.
        self.assertGreaterEqual(len(connects), 6)

    def test_healthy_emu_keeps_running(self):
        demand = parse(INSTANTANEOUS_DEMAND, InstantaneousDemand)

        def connect(port_spec, discover_timeout=60, connect_timeout=30):
            client = self.fake_connect(port_spec)
            client.InstantaneousDemand = demand
            return client

        # Stop the loop once the EMU has reported often enough to prove no
        # reconnect or exit happened in the meantime.
        writes = []
        real_write_new_points = emu2influx.write_new_points

        def write_new_points(client, db, last_timestamps):
            writes.append(self.clock.now)
            if len(writes) > 200:
                raise KeyboardInterrupt
            # Every poll returns a reading with a newer device timestamp.
            client.InstantaneousDemand.TimeStamp = hex(0x234E5AF3 + len(writes))
            return real_write_new_points(client, db, last_timestamps)

        with mock.patch.object(emu2influx, "connect", connect), mock.patch.object(
            emu2influx, "write_new_points", write_new_points
        ):
            with self.assertRaises(KeyboardInterrupt):
                emu2influx.main(
                    port_spec="auto",
                    db=FakeInflux(),
                    nudge_after=120,
                    reconnect_after=300,
                    exit_after=900,
                )
        self.assertEqual(1, len(self.clients))


if __name__ == "__main__":
    unittest.main()
