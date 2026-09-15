"""Tests for the EMU serial client, driven through a pty instead of hardware."""

import os
import select
import time
import unittest

import emu

INSTANTANEOUS_DEMAND = b"""<InstantaneousDemand>
  <DeviceMacId>0xd8d5b9000000f8bc</DeviceMacId>
  <MeterMacId>0x00135003005dba2b</MeterMacId>
  <TimeStamp>0x234e5af3</TimeStamp>
  <Demand>0x00014d</Demand>
  <Multiplier>0x00000001</Multiplier>
  <Divisor>0x000003e8</Divisor>
  <DigitsRight>0x03</DigitsRight>
  <DigitsLeft>0x0f</DigitsLeft>
  <SuppressLeadingZero>Y</SuppressLeadingZero>
</InstantaneousDemand>
"""


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return False


def read_available(fd, timeout=5):
    """Read from fd until it stays quiet for a moment."""
    data = b""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        readable, _, _ = select.select([fd], [], [], 0.5)
        if readable:
            data += os.read(fd, 4096)
        elif data:
            return data
    return data


class EmuSerialTest(unittest.TestCase):
    def setUp(self):
        self.master, slave = os.openpty()
        self.addCleanup(os.close, self.master)
        self.client = emu.emu(os.ttyname(slave))
        self.client.start_serial()
        self.addCleanup(self.stop)
        self.assertTrue(wait_until(lambda: self.client.serial_connected))
        # The client has its own handle on the port now.
        os.close(slave)

    def stop(self):
        self.client.stop_serial()
        self.client.thread_handle.join(timeout=10)

    def test_parses_pushed_message(self):
        os.write(self.master, INSTANTANEOUS_DEMAND)
        self.assertTrue(
            wait_until(lambda: getattr(self.client, "InstantaneousDemand", None))
        )
        self.assertEqual("0x00014d", self.client.InstantaneousDemand.Demand)
        self.assertIsNotNone(self.client.last_message_time)

    def test_sends_every_queued_command(self):
        # Commands issued back to back must all reach the device, not just the
        # last one.
        self.client.get_instantaneous_demand("Y")
        self.client.get_current_summation_delivered()
        self.client.get_price_blocks()

        written = read_available(self.master).decode()
        self.assertIn("get_instantaneous_demand", written)
        self.assertIn("get_current_summation_delivered", written)
        self.assertIn("get_price_blocks", written)

    def test_thread_exits_when_device_disappears(self):
        os.close(self.master)
        self.addCleanup(setattr, self, "master", os.open(os.devnull, os.O_RDONLY))
        self.assertTrue(wait_until(lambda: not self.client.thread_handle.is_alive()))
        self.assertFalse(self.client.serial_connected)

    def test_open_failure_is_reported(self):
        client = emu.emu("/dev/definitely_not_a_serial_port")
        client.start_serial()
        client.thread_handle.join(timeout=10)
        self.assertFalse(client.thread_handle.is_alive())
        self.assertFalse(client.serial_connected)
        self.assertIsNotNone(client.serial_error)


if __name__ == "__main__":
    unittest.main()
