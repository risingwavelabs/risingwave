# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from rabbitmq import RabbitMq


def message(value):
    return {
        "exchange": "events",
        "routing_key": "selected",
        "properties": {"content_type": "application/json", "delivery_mode": 2},
        "payload_encoding": "base64",
        "payload": base64.b64encode(json.dumps(value).encode()).decode(),
    }


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        # The verifier and fault selection need no real network for these tests.
        self.broker = RabbitMq.__new__(RabbitMq)
        self.args = SimpleNamespace(
            ids=[3, 4], allow_previous_ids=[1, 2], encoding="json",
            exchange="events", routing_key="selected", queue="test", timeout=1,
        )

    @patch("rabbitmq.time.sleep")
    def test_accepts_duplicates_and_previously_verified_rows(self, _sleep):
        self.broker.get_messages = Mock(side_effect=[
            [message({"id": i}) for i in [1, 3, 3, 2, 4]], [],
        ])
        self.broker.verify(self.args)

    @patch("rabbitmq.time.sleep")
    @patch("rabbitmq.time.monotonic", side_effect=[0, 0, 2])
    def test_previous_rows_cannot_satisfy_missing_new_rows(self, _clock, _sleep):
        self.broker.get_messages = Mock(return_value=[
            message({"id": i}) for i in [1, 2, 3, 3]
        ])
        with self.assertRaisesRegex(AssertionError, 'Missing messages.*"id": 4'):
            self.broker.verify(self.args)

    def test_rejects_unexpected_payload_even_with_an_allowed_previous_id(self):
        self.broker.get_messages = Mock(return_value=[
            message({"id": 1, "unexpected": True}),
        ])
        with self.assertRaisesRegex(AssertionError, "Unexpected message"):
            self.broker.verify(self.args)

    @patch.dict("os.environ", {"RABBITMQ_TEST_RESTART_CONTAINER": ""})
    @patch("rabbitmq.time.sleep")
    def test_disconnect_waits_for_stats_and_is_scoped_to_test_user(self, _sleep):
        self.broker.vhost = "/"
        self.broker.request = Mock(side_effect=[[], [
            {"name": "target", "user": "fixture", "vhost": "/"},
            {"name": "other-user", "user": "other", "vhost": "/"},
            {"name": "other-vhost", "user": "fixture", "vhost": "other"},
        ], None])
        self.broker.interrupt_recovery("fixture")
        self.assertEqual(self.broker.request.call_count, 3)
        self.broker.request.assert_called_with(
            "DELETE", "connections", "target", allow_missing=True
        )


if __name__ == "__main__":
    unittest.main()
