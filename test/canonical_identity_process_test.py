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
"""Cross-process stability tests for the canonical task identity mode.

Task ids are sent between worker processes and the scheduler, so the canonical
encoding must produce byte-identical ids in independently started Python
processes regardless of dictionary insertion order or hash randomization.
These tests spawn real subprocesses rather than reusing the in-process
registry.
"""

import json
import os
import subprocess
import sys

from helpers import unittest

import luigi


_CHILD_SCRIPT = r"""
import json
import sys
import warnings

warnings.simplefilter("ignore")

import luigi
from luigi import configuration

configuration.get_config().set("core", "canonical_task_identity", "true")


class CrossProcessDictTask(luigi.Task):
    payload = luigi.DictParameter()


class CrossProcessScalarTask(luigi.Task):
    value = luigi.Parameter()


content = {"a": 1, "b": 2, "c": [1, {"x": True, "y": None}]}
ordered = {key: content[key] for key in json.loads(sys.argv[1])}

result = {
    "dict_task_id": CrossProcessDictTask(payload=ordered).task_id,
    "int_task_id": CrossProcessScalarTask(value=1).task_id,
    "bool_task_id": CrossProcessScalarTask(value=True).task_id,
    "str_task_id": CrossProcessScalarTask(value="1").task_id,
    "float_task_id": CrossProcessScalarTask(value=1.0).task_id,
}
print(json.dumps(result, sort_keys=True))
"""


def _run_child(key_order):
    env = dict(os.environ)
    env["PYTHONHASHSEED"] = str(hash(tuple(key_order)) & 0xFFFF)
    completed = subprocess.run(
        [sys.executable, "-c", _CHILD_SCRIPT, json.dumps(key_order)],
        capture_output=True,
        text=True,
        env=env,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(luigi.__file__))),
    )
    if completed.returncode != 0:
        raise AssertionError("canonical identity subprocess failed:\n" + completed.stderr)
    return json.loads(completed.stdout)


class CanonicalIdentityCrossProcessTest(unittest.TestCase):
    def test_shuffled_mapping_order_identical_across_processes(self):
        first = _run_child(["a", "b", "c"])
        second = _run_child(["c", "b", "a"])
        third = _run_child(["b", "a", "c"])

        self.assertEqual(first["dict_task_id"], second["dict_task_id"])
        self.assertEqual(first["dict_task_id"], third["dict_task_id"])

    def test_scalar_type_isolation_holds_across_processes(self):
        result = _run_child(["a", "b", "c"])
        ids = [result["int_task_id"], result["bool_task_id"], result["str_task_id"], result["float_task_id"]]
        self.assertEqual(len(set(ids)), len(ids))

    def test_ids_match_in_process_task_instances(self):
        from luigi import configuration

        configuration.get_config().set("core", "canonical_task_identity", "true")
        try:

            class CrossProcessDictTask(luigi.Task):
                payload = luigi.DictParameter()

            in_process = CrossProcessDictTask(payload={"a": 1, "b": 2, "c": [1, {"x": True, "y": None}]}).task_id
        finally:
            configuration.get_config().remove_option("core", "canonical_task_identity")

        self.assertEqual(in_process, _run_child(["a", "b", "c"])["dict_task_id"])


if __name__ == "__main__":
    unittest.main()
