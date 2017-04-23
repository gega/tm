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
- Sensor instance (2char)
- Source node id or "xxx..." if the data is global (8char)

### Sensor Data Content
One line of ascii text.

### Sensor Input
Local sensors can feed data to the system in one of two ways:
- Writing a file to the "input" directory, where the filename should be the sensor type and instance and the content should be the measurements
- Sending the measurement to the tcp input port to localhost (name, content)

### misc

| name             | value
| ---------------- | ------------------- |
| data directory   | /var/run/tm_data    |
| input directory  | /var/run/tm_data/in |
| local input port | 7699                |

### Disclaimer
_This is not a hard real time system and shouldn't be used for critical and urgent data. The typical delivery time is ~0.5-1sec but there are no guarantees of data consistency and delivery times in this implementation nor in the supported underlying operating system._
