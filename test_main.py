#!/usr/bin/env python3
import unittest
from unittest.mock import patch, MagicMock
import socket
import queue
from io import BytesIO

from main import Adapter
from utils.mock_mqtt_publisher import MockMqttPublisher


class TestAdapter(unittest.TestCase):
    def setUp(self):
        self.mqtt_publisher = MockMqttPublisher()
        self.adapter = Adapter(
            mqtt_publisher=self.mqtt_publisher,
            sbs1_host="localhost",
            sbs1_port=5002,
            gcmb_org="test-org",
            gcmb_project="test-project"
        )
        # Set running to False to prevent threads from starting
        self.adapter.running = False

    def test_publish_locations(self):
        """Test that locations are published correctly."""
        # Add a location to the queue
        self.adapter.location_queue.put({
            'icao24': 'ABC123',
            'callsign': 'TEST123',
            'lat': 51.5074,
            'lon': -0.1278
        })

        # Run the publish_locations method once
        with patch.object(self.adapter, 'running', side_effect=[True, False]):
            self.adapter.publish_locations()

        # Check that the message was published to the correct topic with the correct payload
        expected_topic = "test-org/test-project/flights/TEST123/location"
        published_messages = self.mqtt_publisher.get_messages_by_topic(expected_topic)

        self.assertEqual(len(published_messages), 1)
        self.assertEqual(published_messages[0]['payload'], "51.5074,-0.1278")
        self.assertTrue(published_messages[0]['retain'])

    def test_publish_stats(self):
        """Test that stats are published correctly."""
        # Add some data to the caches
        self.adapter.flights_cache['ABC123'] = None
        self.adapter.messages_cache[1] = None

        # Run the publish_stats method once
        with patch.object(self.adapter, 'running', side_effect=[True, False]):
            self.adapter.publish_stats()

        # Check that the stats were published to the correct topics with the correct payloads
        base_topic = "test-org/test-project"

        flights_seen_topic = f"{base_topic}/stats/flights_seen_in_last_15m"
        flights_seen_messages = self.mqtt_publisher.get_messages_by_topic(flights_seen_topic)
        self.assertEqual(len(flights_seen_messages), 1)
        self.assertEqual(flights_seen_messages[0]['payload'], "1")
        self.assertTrue(flights_seen_messages[0]['retain'])

        queue_size_topic = f"{base_topic}/stats/queue_size"
        queue_size_messages = self.mqtt_publisher.get_messages_by_topic(queue_size_topic)
        self.assertEqual(len(queue_size_messages), 1)
        self.assertEqual(queue_size_messages[0]['payload'], "0")
        self.assertTrue(queue_size_messages[0]['retain'])

        messages_per_minute_topic = f"{base_topic}/stats/messages_per_minute"
        messages_per_minute_messages = self.mqtt_publisher.get_messages_by_topic(messages_per_minute_topic)
        self.assertEqual(len(messages_per_minute_messages), 1)
        self.assertEqual(messages_per_minute_messages[0]['payload'], "1")
        self.assertTrue(messages_per_minute_messages[0]['retain'])

    def test_consume_from_sbs1(self):
        """Test that SBS1 data is consumed and processed correctly."""
        # Mock socket and file
        mock_socket = MagicMock()
        mock_file = BytesIO(b'MSG,3,1,1,ABC123,1,2023/04/01,12:34:56.789,2023/04/01,12:34:56.789,TEST123,37000,265.2,95.3,51.5074,-0.1278,,-1234,,,0,0\n')

        # Mock socket.socket to return our mock socket
        with patch('socket.socket', return_value=mock_socket):
            # Mock socket.makefile to return our mock file
            mock_socket.makefile.return_value = mock_file

            # Run the consume_from_sbs1 method once
            with patch.object(self.adapter, 'running', side_effect=[True, True, False]):
                self.adapter.consume_from_sbs1()

        # Check that the location was added to the queue
        self.assertEqual(self.adapter.location_queue.qsize(), 1)

        location = self.adapter.location_queue.get()
        self.assertEqual(location['icao24'], 'ABC123')
        self.assertEqual(location['callsign'], 'TEST123')
        self.assertEqual(location['lat'], 51.5074)
        self.assertEqual(location['lon'], -0.1278)

        # Check that the callsign was stored
        self.assertEqual(self.adapter.callsigns['ABC123'], 'TEST123')

        # Check that the flight was added to the cache
        self.assertIn('ABC123', self.adapter.flights_cache)

    def test_topic_sanitization(self):
        """Test that topics are sanitized correctly."""
        # Add a location with a callsign that needs sanitization
        self.adapter.location_queue.put({
            'icao24': 'ABC123',
            'callsign': 'TEST+123 #ÄÖÜ',
            'lat': 51.5074,
            'lon': -0.1278
        })

        # Run the publish_locations method once
        with patch.object(self.adapter, 'running', side_effect=[True, False]):
            self.adapter.publish_locations()

        # Check that the message was published to the sanitized topic
        expected_topic = "test-org/test-project/flights/TEST-123--AeOeUe/location"
        published_messages = self.mqtt_publisher.get_messages_by_topic(expected_topic)

        self.assertEqual(len(published_messages), 1)
        self.assertEqual(published_messages[0]['payload'], "51.5074,-0.1278")

    def test_debouncing(self):
        """Test that debouncing works correctly (only one message per flight every 5 seconds)."""
        # Reset the MQTT publisher to clear any previous messages
        self.mqtt_publisher = MockMqttPublisher()
        self.adapter.mqtt_publisher = self.mqtt_publisher

        # Mock time.time to return a fixed value for the first call
        initial_time = 1000.0

        with patch('time.time', return_value=initial_time):
            # Add a location to the queue
            self.adapter.location_queue.put({
                'icao24': 'ABC123',
                'callsign': 'TEST123',
                'lat': 51.5074,
                'lon': -0.1278
            })

            # Run the publish_locations method once
            with patch.object(self.adapter, 'running', side_effect=[True, False]):
                self.adapter.publish_locations()

            # Check that the message was published
            expected_topic = "test-org/test-project/flights/TEST123/location"
            published_messages = self.mqtt_publisher.get_messages_by_topic(expected_topic)
            self.assertEqual(len(published_messages), 1)
            self.assertEqual(published_messages[0]['payload'], "51.5074,-0.1278")

        # Now try to publish again immediately (still at the same time)
        with patch('time.time', return_value=initial_time + 1):  # Only 1 second later
            # Add the same location to the queue again
            self.adapter.location_queue.put({
                'icao24': 'ABC123',
                'callsign': 'TEST123',
                'lat': 51.5074,
                'lon': -0.1278
            })

            # Run the publish_locations method again
            with patch.object(self.adapter, 'running', side_effect=[True, False]):
                self.adapter.publish_locations()

            # Check that no new message was published (still only 1 message)
            published_messages = self.mqtt_publisher.get_messages_by_topic(expected_topic)
            self.assertEqual(len(published_messages), 1)  # Still only 1 message

        # Now try to publish after 5 seconds have passed
        with patch('time.time', return_value=initial_time + 6):  # 6 seconds later (> 5 seconds)
            # Add the same location to the queue again
            self.adapter.location_queue.put({
                'icao24': 'ABC123',
                'callsign': 'TEST123',
                'lat': 51.6000,  # Slightly different location
                'lon': -0.1300
            })

            # Run the publish_locations method again
            with patch.object(self.adapter, 'running', side_effect=[True, False]):
                self.adapter.publish_locations()

            # Check that a new message was published (now 2 messages)
            published_messages = self.mqtt_publisher.get_messages_by_topic(expected_topic)
            self.assertEqual(len(published_messages), 2)
            self.assertEqual(published_messages[1]['payload'], "51.6,-0.13")


if __name__ == '__main__':
    unittest.main()
