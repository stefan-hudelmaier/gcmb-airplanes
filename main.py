#!/usr/bin/env python3
from dotenv import load_dotenv
load_dotenv()

import os
import logging
import sys
import time
import socket
import queue
from threading import Thread
from cachetools import TTLCache
from gcmb_publisher import MqttPublisher

from sbs1 import parse
from utils import sanitize_topic

# Initialize environment variables
MQTT_USERNAME = os.environ.get('MQTT_USERNAME')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'airplanes/data-generator/pub')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD')
GCMB_ORG = os.environ.get('GCMB_ORG', 'stefan')
GCMB_PROJECT = os.environ.get('GCMB_PROJECT', 'airplanes')
SBS1_HOST = os.environ.get('SBS1_HOST', 'localhost')
SBS1_PORT = int(os.environ.get('SBS1_PORT', '5002'))
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')

# Set up logging
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class Adapter:
    """
    Adapter class that handles the main fetching and publishing loop.

    Gets passed in the values of the env variables and contains the main
    fetching and publishing loop with exception handling.
    """
    def __init__(self, mqtt_publisher, sbs1_host, sbs1_port, gcmb_org, gcmb_project):
        """
        Initialize the adapter with the given parameters.

        Args:
            mqtt_publisher: The MQTT publisher to use
            sbs1_host: The host to connect to for SBS1 data
            sbs1_port: The port to connect to for SBS1 data
            gcmb_org: The organization name for MQTT topic prefix
            gcmb_project: The project name for MQTT topic prefix
        """
        self.mqtt_publisher = mqtt_publisher
        self.sbs1_host = sbs1_host
        self.sbs1_port = sbs1_port
        self.gcmb_org = gcmb_org
        self.gcmb_project = gcmb_project

        # Base MQTT topic
        self.base_topic = f"{self.gcmb_org}/{self.gcmb_project}"

        # Queue for location data
        self.location_queue = queue.Queue(maxsize=100000)

        # Cache for flights (TTL of 15 minutes)
        self.flights_cache = TTLCache(maxsize=100_000, ttl=60 * 15)

        # Cache for callsigns
        self.callsigns = {}

        # For measuring throughput
        self.messages_cache = TTLCache(maxsize=100_000, ttl=60)

        # Flag to control the running of threads
        self.running = True

    def start(self):
        """
        Start the adapter by creating and starting the necessary threads.
        """
        logger.info("Starting adapter")

        # Start the thread to consume data from SBS1
        sbs1_thread = Thread(target=self.consume_from_sbs1, args=())
        sbs1_thread.daemon = True
        sbs1_thread.start()

        # Start the thread to publish location data
        publish_thread = Thread(target=self.publish_locations, args=())
        publish_thread.daemon = True
        publish_thread.start()

        # Start the thread to publish stats
        stats_thread = Thread(target=self.publish_stats, args=())
        stats_thread.daemon = True
        stats_thread.start()

        # Keep the main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping adapter")
            self.running = False

    def consume_from_sbs1(self):
        """
        Consume data from the SBS1 socket and put it in the location queue.
        """
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                logger.info(f"Connecting to {self.sbs1_host}:{self.sbs1_port}")
                s.connect((self.sbs1_host, self.sbs1_port))
                f = s.makefile("rb")

                while self.running:
                    try:
                        data = f.readline()

                        if data == b'':
                            break

                        parsed_data = parse(data.decode('utf-8'))
                        if parsed_data is None:
                            continue

                        icao24 = parsed_data['icao24']
                        callsign = parsed_data['callsign']

                        if callsign is not None:
                            self.callsigns[icao24] = callsign

                        lat = parsed_data['lat']
                        lon = parsed_data['lon']

                        callsign = self.callsigns[icao24] if icao24 in self.callsigns else None

                        if callsign is not None:
                            self.flights_cache[icao24] = None

                        if lat is not None and lon is not None and callsign is not None:
                            self.location_queue.put({'icao24': icao24, 'callsign': callsign, 'lat': lat, 'lon': lon})
                            logger.debug(f"Added location for {callsign} to queue")

                    except Exception as e:
                        logger.error(f"Error processing SBS1 data: {e}")

                logger.info("Socket closed, reconnecting")
                time.sleep(5)

            except Exception as e:
                logger.error(f"Error connecting to SBS1 socket: {e}")
                time.sleep(5)

    def publish_locations(self):
        """
        Publish location data from the queue to MQTT.
        """
        while self.running:
            try:
                # Get location data from the queue with a timeout
                try:
                    location = self.location_queue.get(timeout=1)
                except queue.Empty:
                    continue

                callsign = location['callsign']
                lat = location['lat']
                lon = location['lon']

                # Sanitize the callsign for use in the topic
                sanitized_callsign = sanitize_topic(callsign)

                topic = f"{self.base_topic}/flights/{sanitized_callsign}/location"

                # Publish the location
                self.mqtt_publisher.send_msg(f"{lat},{lon}", topic, retain=True, message_expiry_interval=10)
                logger.debug(f"Published location for {callsign} to {topic}")

                # Add to messages cache for stats
                self.messages_cache[time.time()] = None

            except Exception as e:
                logger.error(f"Error publishing location: {e}")

    def publish_stats(self):
        """
        Publish statistics about the adapter.
        """
        while self.running:
            try:
                time.sleep(60)  # Publish stats every minute

                # Calculate stats
                flights_seen = len(self.flights_cache)
                queue_size = self.location_queue.qsize()
                messages_per_minute = len(self.messages_cache)

                # Publish stats
                self.mqtt_publisher.send_msg(f"{flights_seen}", f"{self.base_topic}/stats/flights_seen_in_last_15m", retain=True)
                self.mqtt_publisher.send_msg(f"{queue_size}", f"{self.base_topic}/stats/queue_size", retain=True)
                self.mqtt_publisher.send_msg(f"{messages_per_minute}", f"{self.base_topic}/stats/messages_per_minute", retain=True)

                logger.info(f"Stats: Flights seen: {flights_seen}, Queue size: {queue_size}, Messages per minute: {messages_per_minute}")

            except Exception as e:
                logger.error(f"Error publishing stats: {e}")


def main():
    """
    Main function to create and start the adapter.
    """
    try:
        # Create MQTT publisher with 10-minute message retention
        mqtt_publisher = MqttPublisher(enable_watchdog=True)

        # Create and start the adapter
        adapter = Adapter(
            mqtt_publisher=mqtt_publisher,
            sbs1_host=SBS1_HOST,
            sbs1_port=SBS1_PORT,
            gcmb_org=GCMB_ORG,
            gcmb_project=GCMB_PROJECT
        )

        adapter.start()

    except Exception as e:
        logger.error(f"Error in main: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
