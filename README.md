# tm
semi-real time telemetry data sharing

## environment
- libev
- unix like os
- every node on the same network

## overview
The purpose of the system is to share current data in the network like temperature, music volume and other relatively small sensor data. All of the data in the system can be found in a directory in the filesystem (TM_DATADIR). The system keep this directory up to date. Every sensor data is a file in the directory. The filename contains information regarding the sensor type, sensor id and source node. The file modification time set to the data creation time. After a certain time (TM_MAXAGE) all not updated data files are removed.

## details


### data filename structure
- Sensor type (2char)
- Sensor instance (2 hex digits, MSBs: priority)
- Source node id or "xxx..." if the data is global (8char)

### sensor priority
Encoded in sensor instance number in the two most significant bits.

| MSB | hex  | priority | frequency
| --- | ---- | -------- | ---------
| 00  | 0x0x | urgent   | 1 sec
| 01  | 0x4x | normal   | 5 sec
| 10  | 0x8x | low      | 61 sec
| 11  | 0xcx | sporadic | once<sup>1</sup>

<sup>1</sup> appears in every cycle for 1 minute after generated

### sensor data content
One line of ascii7 text without space. Binary data can be base64 encoded. Maximum length is 256 bytes.

### sensor input
Local sensors can feed data to the system in one of two ways:
- Writing a file to the "input" directory, where the filename should be the sensor type and instance and the content should be the measurements
  
  ```echo -n "00:02" >/var/run/tm_data/in/TI40```

- Sending the measurement to the tcp input port to localhost (name, content)
  
  ```echo "#bTI4000:02" | socat - TCP:127.0.0.1:7699```

- Quit local instance with a "quit" message to the local input port
  
  ```echo "quit" | socat - TCP:127.0.0.1:7699```

### misc

| name             | value               | config
| ---------------- | ------------------- | ---------
| data directory   | /tmp/tm_data        | TM_DATADIR
| input directory  | /tmp/tm_data/in     | -
| local input port | 7699                | -
| buffer size      | 4096                | TM_BUFSIZE
| max data age     | 3600                | TM_MAXAGE
| lockfile         | /var/lock/tm.lock   | TM_LOCKFILE
| max sensor data  | 256                 | -
| sensor heartbeat | 5000 ms             | -
| system heartbeat | 1000 ms             | -
| global prefix    | GL                  | -
| heartbeat prefix | HB                  | -
| UDP bus port     | 7697                | -
| TCP fwd port     | 7698                | -
| log ident string | tmd                 | TM_LOG_IDENT
| error limit      | 30                  | TM_MAXERR


### Disclaimer
_This is not a hard real time system and shouldn't be used for critical and urgent data. The typical delivery time is ~0.5-1sec but there are no guarantees of data consistency and delivery times in this implementation nor in the supported underlying operating system._
