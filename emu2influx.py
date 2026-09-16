import argparse
import glob
import logging
import os
import sys
import time

from datetime import datetime

from influxdb import InfluxDBClient

import emu


Y2K = 946684800
int_max = 2**31 - 1
uint_max = 2**32 - 1

# Not every host has udev's stable /dev/serial/by-id symlinks, so fall back to
# globbing the CDC ACM devices. /host/dev is where the container bind-mounts the
# host's /dev, which is what makes a re-enumerated device visible without a
# container restart.
DEFAULT_PORT_GLOBS = (
    "/dev/serial/by-id/*Rainforest*",
    "/host/dev/serial/by-id/*Rainforest*",
    "/dev/ttyACM*",
    "/host/dev/ttyACM*",
)

POLL_INTERVAL = 10


def get_timestamp(obj):
    # print obj.TimeStamp
    if obj.TimeStamp is None:
        obj.TimeStamp = "0x0"
    return datetime.utcfromtimestamp(Y2K + int(obj.TimeStamp, 16))


def get_reading(reading, obj):
    reading = int(reading, 16) * int(obj.Multiplier, 16)
    if reading > int_max:
        reading = -1 * (uint_max - reading)
    return reading / float(int(obj.Divisor, 16))


def get_price(obj):
    return int(obj.Price, 16) / float(10 ** int(obj.TrailingDigits, 16))


def price_fields(obj):
    return {"price": get_price(obj)}


def demand_fields(obj):
    return {"demand": get_reading(obj.Demand, obj)}


def reading_fields(obj):
    return {
        "reading": get_reading(obj.SummationDelivered, obj),
        "reading_received": get_reading(obj.SummationReceived, obj),
    }


# (InfluxDB measurement, attribute set on the emu client, field builder)
MEASUREMENTS = (
    ("price", "PriceCluster", price_fields),
    ("demand", "InstantaneousDemand", demand_fields),
    ("reading", "CurrentSummationDelivered", reading_fields),
)


def find_serial_port(port_spec):
    """Return a serial device path, or None if nothing matches.

    A spec of "auto" searches DEFAULT_PORT_GLOBS. Anything else is used
    directly, and may itself be a glob.
    """
    if port_spec == "auto":
        patterns = DEFAULT_PORT_GLOBS
    else:
        if not port_spec.startswith("/"):
            port_spec = "/dev/" + port_spec
        patterns = (port_spec,)

    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        if matches:
            if len(matches) > 1:
                logging.warning(
                    "Multiple serial ports match %s: %s, using %s",
                    pattern,
                    matches,
                    matches[0],
                )
            return matches[0]
    return None


def wait_for_serial_port(port_spec, timeout, interval=POLL_INTERVAL):
    """Return a matching serial device path, or None if none appears in time."""
    deadline = time.monotonic() + timeout
    logged = False
    while True:
        port = find_serial_port(port_spec)
        if port is not None:
            return port
        if not logged:
            logging.warning("No serial port matches %s, waiting for it to appear",
                            port_spec)
            logged = True
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)


def request_updates(client):
    client.get_instantaneous_demand("Y")
    client.get_current_summation_delivered()
    client.get_price_blocks()


def connect(port_spec, discover_timeout=60, connect_timeout=30):
    """Open the EMU serial port, returning a started client.

    Returns None if the port did not come up, so the caller can retry.
    """
    port = wait_for_serial_port(port_spec, discover_timeout)
    if port is None:
        return None
    client = emu.emu(port)
    client.start_serial()

    deadline = time.monotonic() + connect_timeout
    while time.monotonic() < deadline:
        if client.serial_connected:
            logging.info("Connected to the EMU on %s", port)
            request_updates(client)
            return client
        if not client.thread_handle.is_alive():
            logging.error("Could not open %s: %s", port, client.serial_error)
            return None
        time.sleep(1)

    logging.error("Timed out opening %s", port)
    disconnect(client)
    return None


def disconnect(client):
    if client is None:
        return
    client.stop_serial()
    client.thread_handle.join(timeout=30)


def write_new_points(client, db, last_timestamps):
    """Write any readings newer than the last ones seen.

    Returns True if the EMU reported anything new.
    """
    fresh = False
    for measurement, attribute, build_fields in MEASUREMENTS:
        obj = getattr(client, attribute, None)
        if obj is None:
            continue

        try:
            timestamp = get_timestamp(obj)
            fields = build_fields(obj)
        except (AttributeError, TypeError, ValueError):
            logging.warning("Skipping malformed %s message", attribute, exc_info=True)
            continue

        last_timestamp = last_timestamps.get(measurement)
        if last_timestamp is not None and timestamp <= last_timestamp:
            continue
        fresh = True

        point = {
            "measurement": measurement,
            "time": timestamp.isoformat(),
            "fields": fields,
        }
        logging.debug(point)
        db.write_points([point], time_precision="s")
        last_timestamps[measurement] = timestamp
    return fresh


def main(port_spec, db, nudge_after, reconnect_after, exit_after):
    """Poll the EMU forever, escalating through recovery steps when it goes quiet.

    The EMU sometimes stops pushing messages, and sometimes re-enumerates under
    a different device node. So when no new readings arrive, first re-send the
    requests, then reopen the (re-discovered) serial port, and finally exit
    nonzero so the container is restarted.
    """
    last_timestamps = {}
    client = None
    last_fresh_data = time.monotonic()
    last_nudge = last_fresh_data
    last_reconnect = last_fresh_data

    while True:
        if client is None:
            client = connect(port_spec, discover_timeout=reconnect_after)
            last_reconnect = time.monotonic()
            if client is None:
                time.sleep(POLL_INTERVAL)
                continue
            last_nudge = last_reconnect

        time.sleep(POLL_INTERVAL)

        now = time.monotonic()
        if write_new_points(client, db, last_timestamps):
            last_fresh_data = now
            continue

        quiet_for = now - last_fresh_data
        if quiet_for > exit_after:
            logging.error(
                "No data from the EMU for %.0fs, exiting so the service restarts",
                quiet_for,
            )
            disconnect(client)
            return 1

        serial_dead = not client.thread_handle.is_alive()
        reconnect_due = (
            quiet_for > reconnect_after and now - last_reconnect > reconnect_after
        )
        if serial_dead or reconnect_due:
            logging.warning(
                "Reconnecting to the EMU (quiet for %.0fs, serial error: %s)",
                quiet_for,
                client.serial_error,
            )
            disconnect(client)
            client = None
            continue

        if quiet_for > nudge_after and now - last_nudge > nudge_after:
            logging.warning("No new data for %.0fs, re-requesting updates",
                            quiet_for)
            request_updates(client)
            last_nudge = now


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", help="enable debug logging", required=False
    )
    parser.add_argument(
        "--host", help="influx host", required=False, default="localhost"
    )
    parser.add_argument("--port", help="influx port", required=False, default=8086)
    parser.add_argument(
        "--username", help="influx username", required=False, default="root"
    )
    parser.add_argument(
        "--password", help="influx password", required=False, default="root"
    )
    parser.add_argument(
        "--db", help="influx database name", required=False, default="rainforest"
    )
    parser.add_argument("--retries", help="influx retries", required=False, default=3)
    parser.add_argument(
        "--nudge-after",
        type=float,
        default=120,
        help="seconds without new data before re-requesting updates",
    )
    parser.add_argument(
        "--reconnect-after",
        type=float,
        default=300,
        help="seconds without new data before reopening the serial port",
    )
    parser.add_argument(
        "--exit-after",
        type=float,
        default=900,
        help="seconds without new data before exiting nonzero",
    )
    parser.add_argument(
        "serial_port",
        nargs="?",
        default="auto",
        help="Rainforest serial port, e.g. 'ttyACM0', or 'auto' to discover it",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(
        level=("DEBUG" if args.debug else "INFO"),
        format="%(asctime)s:%(levelname)s:%(name)s: %(message)s",
    )
    influx = InfluxDBClient(
        database=args.db,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        retries=args.retries,
    )
    influx.create_database(args.db)

    try:
        sys.exit(
            main(
                port_spec=args.serial_port,
                db=influx,
                nudge_after=args.nudge_after,
                reconnect_after=args.reconnect_after,
                exit_after=args.exit_after,
            )
        )
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
