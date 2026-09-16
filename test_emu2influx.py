import unittest

from datetime import datetime
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


if __name__ == "__main__":
    unittest.main()
