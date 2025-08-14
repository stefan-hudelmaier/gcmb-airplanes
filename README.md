# Airplanes

This is a real-time stream of airplane positions based on SBS1 data. The adapter connects to a TCP socket that provides SBS1 data, extracts airplane positions, and publishes them via MQTT.

## Topics

The following topics are available:

- `${GCMB_ORG}/${GCMB_PROJECT}/flights/${callsign}/location`: The current location of the airplane with the given callsign, in the format "latitude,longitude".
- `${GCMB_ORG}/${GCMB_PROJECT}/stats/flights_seen_in_last_15m`: The number of flights seen in the last 15 minutes.
- `${GCMB_ORG}/${GCMB_PROJECT}/stats/queue_size`: The current size of the location queue.
- `${GCMB_ORG}/${GCMB_PROJECT}/stats/messages_per_minute`: The number of messages published per minute.

Here is an example MQTT message for a flight location:

```
51.5074,-0.1278
```

This represents a latitude of 51.5074 and a longitude of -0.1278.

## Technical integration

The adapter ([GitHub project](https://github.com/stefan-hudelmaier/gcmb-airplanes)) connects to a TCP socket that provides SBS1 data, extracts airplane positions, and publishes them via MQTT. The adapter uses the following environment variables:

- `MQTT_USERNAME`: The username for MQTT authentication.
- `MQTT_CLIENT_ID`: The client ID for MQTT connection.
- `MQTT_PASSWORD`: The password for MQTT authentication.
- `GCMB_ORG`: The organization name for MQTT topic prefix.
- `GCMB_PROJECT`: The project name for MQTT topic prefix.
- `SBS1_HOST`: The host to connect to for SBS1 data.
- `SBS1_PORT`: The port to connect to for SBS1 data.
- `LOG_LEVEL`: The log level to use (DEBUG, INFO, WARNING, ERROR, CRITICAL).

The MQTT messages are published with the retain flag set to true, but with a limited retention time of 10 minutes.

## Acknowledgements

This project uses the SBS1 parser from the [ADS-B-funhouse](https://github.com/kanflo/ADS-B-funhouse) project by John Wiseman.
