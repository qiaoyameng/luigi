# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from helpers import unittest

from luigi.freezing import FrozenOrderedDict, recursively_freeze


class FrozenOrderedDictHashTest(unittest.TestCase):
    def test_equal_content_same_hash_regardless_of_order(self):
        first = FrozenOrderedDict([("a", 1), ("b", 2)])
        shuffled = FrozenOrderedDict([("b", 2), ("a", 1)])
        self.assertEqual(first, shuffled)
        self.assertEqual(hash(first), hash(shuffled))
        self.assertIn(first, {shuffled})

    def test_swapped_values_do_not_collide(self):
        # The legacy XOR implementation hashed these two different mappings
        # identically, corrupting the task instance cache.
        first = FrozenOrderedDict([("a", 1), ("b", 2)])
        swapped = FrozenOrderedDict([("a", 2), ("b", 1)])
        self.assertNotEqual(hash(first), hash(swapped))

    def test_different_key_sets_do_not_collide(self):
        first = FrozenOrderedDict([("a", 1), ("b", 2)])
        extended = FrozenOrderedDict([("a", 1), ("b", 2), ("c", 3)])
        self.assertNotEqual(hash(first), hash(extended))

    def test_nested_frozen_structures_hash_consistently(self):
        first = recursively_freeze({"a": [1, {"k": 1}], "b": 2})
        shuffled = recursively_freeze({"b": 2, "a": (1, {"k": 1})})
        self.assertEqual(first, shuffled)
        self.assertEqual(hash(first), hash(shuffled))


if __name__ == "__main__":
    unittest.main()
