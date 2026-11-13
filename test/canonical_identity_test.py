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
"""Tests for the canonical task identity mode (:mod:`luigi.canonical`)."""

import datetime
import enum
import math
import re
from decimal import Decimal

from helpers import unittest

import luigi
from luigi import canonical, configuration
from luigi.canonical import CanonicalEncodingError, canonical_encoding, canonical_identity_hash
from luigi.parameter import ParameterVisibility


class CanonicalModeTestCase(unittest.TestCase):
    def setUp(self):
        self._config = configuration.get_config()
        self._config.set("core", "canonical_task_identity", "true")
        luigi.task_register.Register.clear_instance_cache()

    def tearDown(self):
        if self._config.has_option("core", "canonical_task_identity"):
            self._config.remove_option("core", "canonical_task_identity")
        luigi.task_register.Register.clear_instance_cache()


class CanonicalEncodingTest(CanonicalModeTestCase):
    def test_scalar_type_isolation(self):
        # The whole point of the type tags.
        self.assertNotEqual(canonical_encoding(1), canonical_encoding(True))
        self.assertNotEqual(canonical_encoding(1), canonical_encoding("1"))
        self.assertNotEqual(canonical_encoding(True), canonical_encoding("1"))
        self.assertNotEqual(canonical_encoding(1), canonical_encoding(1.0))
        self.assertNotEqual(canonical_encoding(Decimal("1")), canonical_encoding(1))
        self.assertNotEqual(canonical_encoding(Decimal("1.0")), canonical_encoding(1.0))

    def test_scalar_equality_is_stable(self):
        self.assertEqual(canonical_encoding(1), canonical_encoding(1))
        self.assertEqual(canonical_encoding("abc"), canonical_encoding("abc"))
        self.assertEqual(canonical_encoding(None), canonical_encoding(None))
        self.assertEqual(canonical_encoding(True), canonical_encoding(True))
        self.assertEqual(canonical_encoding(Decimal("1.50")), canonical_encoding(Decimal("1.50")))

    def test_decimal_is_lexical_not_float(self):
        self.assertTrue(canonical_encoding(Decimal("1.50")).startswith("decimal:"))
        self.assertIn("1.50", canonical_encoding(Decimal("1.50")))

    def test_float_special_values_have_fixed_forms(self):
        self.assertIn("NaN", canonical_encoding(float("nan")))
        self.assertIn("Infinity", canonical_encoding(float("inf")))
        self.assertIn("-Infinity", canonical_encoding(float("-inf")))
        self.assertTrue(math.isnan(float("nan")))  # sanity, the encoding must not raise

    def test_datetime_timezone_convention(self):
        naive = datetime.datetime(2020, 1, 1, 12, 0, 0)
        utc = datetime.datetime(2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        tokyo = datetime.datetime(2020, 1, 1, 21, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
        # Naive wall-clock and absolute instants never share an identity.
        self.assertNotEqual(canonical_encoding(naive), canonical_encoding(utc))
        # Two aware datetimes representing the same instant encode equally.
        self.assertEqual(canonical_encoding(utc), canonical_encoding(tokyo))
        self.assertTrue(canonical_encoding(utc).startswith("datetime_utc:"))
        self.assertTrue(canonical_encoding(utc).endswith("Z"))
        self.assertTrue(canonical_encoding(naive).startswith("datetime:"))

    def test_date_and_timedelta(self):
        self.assertTrue(canonical_encoding(datetime.date(2020, 1, 1)).startswith("date:"))
        encoded_delta = canonical_encoding(datetime.timedelta(days=1, seconds=2, microseconds=3))
        self.assertTrue(encoded_delta.startswith("timedelta:"))
        self.assertEqual(encoded_delta, canonical_encoding(datetime.timedelta(days=1, seconds=2, microseconds=3)))

    def test_list_and_tuple_are_distinct(self):
        self.assertNotEqual(canonical_encoding([1, 2]), canonical_encoding((1, 2)))
        self.assertEqual(canonical_encoding([1, [2, 3]]), canonical_encoding([1, [2, 3]]))

    def test_mapping_order_independence(self):
        left = {"a": 1, "b": 2, "c": [1, {"x": True}]}
        right = {"c": [1, {"x": True}], "b": 2, "a": 1}
        self.assertEqual(canonical_encoding(left), canonical_encoding(right))

    def test_mapping_content_differences_still_distinguish(self):
        self.assertNotEqual(canonical_encoding({"a": 1}), canonical_encoding({"a": 2}))
        self.assertNotEqual(canonical_encoding({"a": 1}), canonical_encoding({"b": 1}))

    def test_non_string_mapping_key_rejected(self):
        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding({1: "x"})

    def test_mixed_type_mapping_keys_rejected_without_raw_type_error(self):
        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding(luigi.freezing.FrozenOrderedDict([(1, "x"), ("a", 1)]))
        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding({True: "x"})

    def test_unregistered_type_rejected_not_repr_coerced(self):
        class NotRegistered:
            def __repr__(self):
                return "surprise"

        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding(NotRegistered())

    def test_circular_mapping_rejected(self):
        cyclic = {}
        cyclic["self"] = cyclic
        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding(cyclic)

    def test_circular_sequence_rejected(self):
        cyclic = []
        cyclic.append(cyclic)
        with self.assertRaises(CanonicalEncodingError):
            canonical_encoding(cyclic)

    def test_enum_encoding(self):
        class Color(enum.Enum):
            RED = 1

        self.assertEqual(canonical_encoding(Color.RED), canonical_encoding(Color.RED))
        self.assertIn("Color.RED", canonical_encoding(Color.RED))

    def test_hash_independent_of_part_order(self):
        parts = [("a", 1), ("b", {"x": [1, 2]}), ("c", "s")]
        self.assertEqual(
            canonical_identity_hash("Family", parts),
            canonical_identity_hash("Family", list(reversed(parts))),
        )

    def test_hash_includes_family_and_version(self):
        parts = [("a", 1)]
        self.assertNotEqual(
            canonical_identity_hash("FamilyA", parts),
            canonical_identity_hash("FamilyB", parts),
        )
        digest = canonical_identity_hash("Family", parts)
        self.assertEqual(len(digest), 64)


class CanonicalTaskIdTest(CanonicalModeTestCase):
    class DictTask(luigi.Task):
        payload = luigi.DictParameter()

    class PlainTask(luigi.Task):
        value = luigi.Parameter()

    def test_shuffled_mapping_insertion_order_same_id(self):
        first = self.DictTask(payload={"a": 1, "b": 2})
        second = self.DictTask(payload={"b": 2, "a": 1})
        self.assertEqual(first.task_id, second.task_id)
        self.assertTrue(first.task_id.endswith("-" + canonical.CANONICAL_IDENTITY_VERSION))

    def test_nested_shuffled_mapping_same_id(self):
        first = self.DictTask(payload={"z": [1, {"y": 2, "x": [True, False]}], "a": None})
        second = self.DictTask(payload={"a": None, "z": (1, luigi.freezing.FrozenOrderedDict([("x", (True, False)), ("y", 2)]))})
        self.assertEqual(first.task_id, second.task_id)

    def test_int_bool_and_string_are_isolated(self):
        int_task = self.PlainTask(value=1)
        str_task = self.PlainTask(value="1")
        bool_task = self.PlainTask(value=True)
        self.assertEqual(len({int_task.task_id, str_task.task_id, bool_task.task_id}), 3)
        self.assertEqual(int_task, int_task)
        self.assertNotEqual(int_task, str_task)
        self.assertNotEqual(int_task, bool_task)

    def test_decimal_parameter_value_supported(self):
        task = self.DictTask(payload={"price": Decimal("10.25")})
        self.assertTrue(task.task_id.endswith("-v2"))

    def test_insignificant_parameter_excluded(self):
        class SigTask(luigi.Task):
            important = luigi.Parameter()
            noise = luigi.Parameter(default="n", significant=False)

        self.assertEqual(SigTask(important="x", noise="a").task_id, SigTask(important="x", noise="b").task_id)

    def test_private_parameter_excluded_but_hidden_included(self):
        class VisTask(luigi.Task):
            important = luigi.Parameter()
            secret = luigi.Parameter(default="s", visibility=ParameterVisibility.PRIVATE)

        self.assertEqual(VisTask(important="x", secret="a").task_id, VisTask(important="x", secret="b").task_id)

        class HiddenTask(luigi.Task):
            important = luigi.Parameter()
            hidden = luigi.Parameter(default="h", visibility=ParameterVisibility.HIDDEN)

        self.assertNotEqual(HiddenTask(important="x", hidden="a").task_id, HiddenTask(important="x", hidden="b").task_id)

    def test_unsupported_value_raises_at_instantiation(self):
        class BrokenTask(luigi.Task):
            value = luigi.Parameter()

        with self.assertRaises(CanonicalEncodingError):
            BrokenTask(value=object())

    def test_from_str_params_roundtrip_preserves_id(self):
        original = self.DictTask(payload={"a": 1, "b": [True, "x", None]})
        rebuilt = self.DictTask.from_str_params(original.to_str_params(only_significant=True, only_public=True))
        self.assertEqual(rebuilt.task_id, original.task_id)

    def test_from_str_params_keeps_type_isolation(self):
        str_task = self.PlainTask(value="1")
        rebuilt = self.PlainTask.from_str_params(str_task.to_str_params(only_significant=True, only_public=True))
        self.assertEqual(rebuilt.task_id, str_task.task_id)
        self.assertNotEqual(rebuilt.task_id, self.PlainTask(value=1).task_id)

    def test_typed_parameter_roundtrips(self):
        class DateTask(luigi.Task):
            moment = luigi.DateSecondParameter()

        original = DateTask(moment=datetime.datetime(2020, 1, 1, 12, 30, 45))
        rebuilt = DateTask.from_str_params(original.to_str_params(only_significant=True, only_public=True))
        self.assertEqual(rebuilt.task_id, original.task_id)

    def test_list_parameter_roundtrips(self):
        class ListTask(luigi.Task):
            items = luigi.ListParameter()

        original = ListTask(items=[1, "a", True, None, 2.5])
        rebuilt = ListTask.from_str_params(original.to_str_params(only_significant=True, only_public=True))
        self.assertEqual(rebuilt.task_id, original.task_id)
        self.assertEqual(rebuilt.items, original.items)

    def test_optional_parameter_none_roundtrips(self):
        class OptionalTask(luigi.Task):
            maybe = luigi.OptionalParameter()

        original = OptionalTask(maybe=None)
        rebuilt = OptionalTask.from_str_params(original.to_str_params(only_significant=True, only_public=True))
        self.assertEqual(rebuilt.task_id, original.task_id)
        self.assertIsNone(rebuilt.maybe)

    def test_batched_parse_list_roundtrip(self):
        class BatchedTask(luigi.Task):
            total = luigi.IntParameter(batch_method=sum)

        rebuilt = BatchedTask.from_str_params({"total": ["1", "2"]})
        self.assertEqual(rebuilt.task_id, BatchedTask(total=3).task_id)
        self.assertEqual(rebuilt.total, 3)

    def test_instance_cache_follows_canonical_identity(self):
        first = self.DictTask(payload={"a": 1, "b": 2})
        shuffled = self.DictTask(payload={"b": 2, "a": 1})
        different = self.DictTask(payload={"a": 1, "b": 3})
        self.assertIs(first, shuffled)
        self.assertIsNot(first, different)

        int_task = self.PlainTask(value=1)
        self.assertIsNot(int_task, self.PlainTask(value="1"))
        self.assertIsNot(int_task, self.PlainTask(value=True))

    def test_equality_and_hash_follow_task_id(self):
        first = self.DictTask(payload={"a": 1, "b": 2})
        second = self.DictTask(payload={"b": 2, "a": 1})
        self.assertEqual(first, second)
        self.assertEqual(hash(first), hash(second))


class LegacyCompatibilityTest(unittest.TestCase):
    class LegacyDictTask(luigi.Task):
        payload = luigi.DictParameter()

    class LegacyPlainTask(luigi.Task):
        value = luigi.Parameter()

    def setUp(self):
        luigi.task_register.Register.clear_instance_cache()

    def tearDown(self):
        luigi.task_register.Register.clear_instance_cache()

    def test_legacy_id_unchanged_by_default(self):
        task = self.LegacyDictTask(payload={"a": 1})
        self.assertNotIn("-v2", task.task_id)
        self.assertEqual(task.task_id, luigi.task.task_id_str(task.get_task_family(), task.to_str_params(only_significant=True, only_public=True)))

    def test_legacy_and_canonical_ids_never_merge(self):
        legacy_task = self.LegacyDictTask(payload={"a": 1, "b": 2})
        legacy_id = legacy_task.task_id

        configuration.get_config().set("core", "canonical_task_identity", "true")
        try:
            canonical_task = self.LegacyDictTask(payload={"a": 1, "b": 2})
            canonical_id = canonical_task.task_id
        finally:
            configuration.get_config().remove_option("core", "canonical_task_identity")

        self.assertNotEqual(legacy_id, canonical_id)
        self.assertTrue(canonical_id.endswith("-v2"))

        scheduler = luigi.scheduler.Scheduler()
        scheduler.add_task(
            task_id=legacy_id,
            worker="worker",
            status="PENDING",
            family=legacy_task.task_family,
            params=legacy_task.to_str_params(only_significant=True),
        )
        scheduler.add_task(
            task_id=canonical_id,
            worker="worker",
            status="PENDING",
            family=canonical_task.task_family,
            params=canonical_task.to_str_params(only_significant=True),
        )
        self.assertTrue(scheduler._state.has_task(legacy_id))
        self.assertTrue(scheduler._state.has_task(canonical_id))
        self.assertEqual(len(scheduler._state._tasks), 2)

    def test_parameterless_task_ids_still_distinguished_by_version(self):
        class NoParams(luigi.Task):
            pass

        legacy_id = NoParams().task_id
        configuration.get_config().set("core", "canonical_task_identity", "true")
        try:
            canonical_id = NoParams().task_id
        finally:
            configuration.get_config().remove_option("core", "canonical_task_identity")

        self.assertNotEqual(legacy_id, canonical_id)
        self.assertTrue(canonical_id.endswith("-v2"))
        self.assertIsNotNone(re.fullmatch(r"[A-Za-z0-9_-]+", canonical_id))


if __name__ == "__main__":
    unittest.main()
