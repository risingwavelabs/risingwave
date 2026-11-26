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

# name, staged (id, payload, row_id, operation), initial target, expected target
FIXTURES = [
    (
        "legacy_counter_9_to_10",
        [(1, "old", "100_9", 1), (1, "new", "100_10", 1)],
        [],
        [(1, "new")],
    ),
    (
        "new_counter_9_to_10",
        [(1, "old", "100_9_1", 1), (1, "new", "100_10_1", 1)],
        [],
        [(1, "new")],
    ),
    (
        "epoch_99_to_100",
        [(1, "old", "99_999_1", 1), (1, "new", "100_0_1", 1)],
        [],
        [(1, "new")],
    ),
    (
        "parallel_actor_9_to_10",
        [(1, "old", "100_10_9", 1), (1, "new", "100_10_10", 1)],
        [],
        [(1, "new")],
    ),
    (
        "legacy_new_padded_mix",
        [
            (1, "old", "100_9", 1),
            (1, "new", "100_10_1", 1),
            (2, "old", "00000000000000000100_00000000000000000009_0000000001", 1),
            (2, "new", "100_10", 1),
            (3, "old", "100_10", 1),
            (3, "new", "100_10_1", 1),
        ],
        [],
        [(1, "new"), (2, "new"), (3, "new")],
    ),
    (
        "unsigned_limits",
        [
            (1, "old", "18446744073709551614_18446744073709551615_4294967295", 1),
            (1, "new", "18446744073709551615_0_0", 1),
            (2, "old", "18446744073709551615_18446744073709551614_4294967295", 1),
            (2, "new", "18446744073709551615_18446744073709551615_0", 1),
        ],
        [],
        [(1, "new"), (2, "new")],
    ),
    (
        "update_delete_reinsert",
        [
            (1, "old", "100_9_1", 1),
            (1, "deleted", "100_10_1", 2),
            (2, "old", "100_9_1", 4),
            (2, "new", "100_10_1", 3),
            (3, "deleted", "100_9_1", 2),
            (3, "new", "100_10_1", 1),
        ],
        [(1, "old"), (2, "old"), (3, "old")],
        [(2, "new"), (3, "new")],
    ),
    ("empty_batch", [], [(1, "unchanged")], [(1, "unchanged")]),
]
