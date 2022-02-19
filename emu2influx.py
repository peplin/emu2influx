import argparse
import logging
import sys
import time
import os

from datetime import datetime

from influxdb import InfluxDBClient

import emu


Y2K = 946684800
int_max = 2**31 - 1
uint_max = 2**32 - 1


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


def main(client, db):
    client.start_serial()
    client.get_instantaneous_demand("Y")
    client.get_current_summation_delivered()
    client.get_price_blocks()

    last_demand_timestamp = None
    last_price_timestamp = None
    last_reading_timestamp = None

    while True:
        time.sleep(10)

        try:
            price_cluster = client.PriceCluster
            timestamp = get_timestamp(price_cluster)
            if last_price_timestamp is None or timestamp > last_price_timestamp:
                measurement = [
                    {
                        "measurement": "price",
                        "time": timestamp,
                        "fields": {"price": get_price(price_cluster)},
                    }
                ]
                logging.debug(price_cluster)
                logging.debug(measurement)
                db.write_points(measurement, time_precision="s")
                last_price_timestamp = timestamp
        except AttributeError:
            pass

        try:
            instantaneous_demand = client.InstantaneousDemand
            timestamp = get_timestamp(instantaneous_demand)
            if last_demand_timestamp is None or timestamp > last_demand_timestamp:
                measurement = [
                    {
                        "measurement": "demand",
                        "time": timestamp.isoformat(),
                        "fields": {
                            "demand": get_reading(
                                instantaneous_demand.Demand, instantaneous_demand
                            )
                        },
                    }
                ]
                logging.debug(instantaneous_demand)
                logging.debug(measurement)
                db.write_points(measurement, time_precision="s")
                last_demand_timestamp = timestamp
        except AttributeError:
            pass

        try:
            current_summation_delivered = client.CurrentSummationDelivered
            timestamp = get_timestamp(current_summation_delivered)
            if last_reading_timestamp is None or timestamp > last_reading_timestamp:
                measurement = [
                    {
                        "measurement": "reading",
                        "time": timestamp.isoformat(),
                        "fields": {
                            "reading": get_reading(
                                current_summation_delivered.SummationDelivered,
                                current_summation_delivered,
                            )
                        },
                    }
                ]
                logging.debug(current_summation_delivered)
                logging.debug(measurement)
                db.write_points(measurement, time_precision="s")
                last_reading_timestamp = timestamp
        except AttributeError:
            pass


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
    parser.add_argument("serial_port", help="Rainforest serial port, e.g. 'ttyACM0'")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(
        level=("DEBUG" if args.debug else "WARN"),
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
        main(client=emu.emu(args.serial_port), db=influx)
    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
