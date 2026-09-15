`emu2influx` uses Rainforest Automation's [Emu-Serial-API](https://github.com/rainforestautomation/Emu-Serial-API) to insert EMU-2 ([Amazon.com](https://www.amazon.com/Rainforest-EMU-2-Energy-Monitoring-Unit/dp/B00BGDPRAI)) energy monitoring data into InfluxDB

`armhf` and `amd64` images are available on Docker Hub at [bakerba/emu2influx](https://hub.docker.com/r/bakerba/emu2influx)

Your EMU-2 must be provisioned with your utility company and connected to your PC

<p align="center">
  <img src="screenshot.png" width="50%" height="50%"/>
</p>

### Prerequisites

* macOS with [Homebrew](https://brew.sh): 
```
$ brew install python2 influxdb
$ brew services start influxdb
```
* Debian/Ubuntu: 
```
sudo apt install python-pip libxslt1-dev influxdb
```

### Setup

```
$ git clone --recursive https://github.com/abaker/emu2influx.git
$ cd emu2influx
$ pip install -r requirements.txt 
```

### Run

`$ python emu2influx.py [emu2_serial_port]`

The serial port is optional. Without it, or with `auto`, `emu2influx` finds the
EMU by looking for `/dev/serial/by-id/*Rainforest*` and then `/dev/ttyACM*`
(also under `/host/dev`, see Docker below). The argument may also be a path or a
glob, e.g. `/dev/ttyACM1` or `'/dev/serial/by-id/*EMU*'`.

By default `emu2influx` will connect to a local InfluxDB install, use the default credentials, and store data in a table named `rainforest`

Run `emu2influx.py --help` for the full argument list.

### Tests

```
$ python -m unittest discover -p 'test_*.py'
```

`test_emu.py` drives the serial client through a pty, so it needs no hardware.

### Recovering from a stalled EMU

The EMU sometimes stops pushing messages, and it comes back on a different
`/dev/ttyACM*` node when it is replugged. When no new readings arrive,
`emu2influx` escalates:

* after `--nudge-after` seconds (120 by default), re-send the data requests
* after `--reconnect-after` seconds (300), close the port, look the device up
  again, and reopen it; this also happens immediately if the serial reader
  fails, for example because the device was unplugged
* after `--exit-after` seconds (900), exit nonzero so the supervisor or
  container runtime restarts the process

### Docker

` $ docker run --device=/dev/ttyACM0 bakerba/emu2influx --host <influx_ip> ttyACM0`

A `devices` mapping is resolved once, when the container starts, so an EMU that
comes back on a different `/dev/ttyACM*` node stays invisible to the container
until it is recreated. To let the container find the device wherever it lands,
bind-mount the host's `/dev` at `/host/dev` and allow the USB ACM character
major number (166) instead:

```
version: '3.3'

services:
  emu2influx:
    image: bakerba/emu2influx
    container_name: emu2influx
    network_mode: host
    volumes:
      - /dev:/host/dev
    device_cgroup_rules:
      - 'c 166:* rmw'
    restart: unless-stopped
    command: '--host <influx_ip>'
```

`device_cgroup_rules` needs docker-compose 1.28 or newer; on older versions use
`privileged: true` with the same bind mount.

### What next?

* Import the [sample dashboard](grafana.json) into Grafana (pictured above)
* Set up a Home Assistant [InfluxDB sensor](https://www.home-assistant.io/components/sensor.influxdb/)
