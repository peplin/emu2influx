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


if __name__ == "__main__":
    unittest.main()
