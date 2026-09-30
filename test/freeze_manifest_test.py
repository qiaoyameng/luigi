#
# Copyright (c) 2026 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

import unittest
from unittest import mock

import luigi
from luigi.freeze_manifest import (
    DependencyDriftError,
    FreezeManifest,
    IncompleteManifestError,
    TaskRegistry,
    UnknownTaskFamilyError,
    freeze_graph,
    restore_graph,
)


class LeafTask(luigi.Task):
    val = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("/tmp/freeze-manifest-test/leaf-%s" % self.val)


class ConfigTask(luigi.Task):
    config = luigi.DictParameter()
    name = luigi.Parameter(default="default")

    def requires(self):
        return LeafTask(val=self.name)

    def output(self):
        return luigi.LocalTarget("/tmp/freeze-manifest-test/config-%s" % self.name)


class SharedDep(luigi.Task):
    def output(self):
        return luigi.LocalTarget("/tmp/freeze-manifest-test/shared")


class RootA(luigi.Task):
    def requires(self):
        return SharedDep()


class RootB(luigi.Task):
    def requires(self):
        return SharedDep()


class DriftTask(luigi.Task):
    def requires(self):
        return LeafTask(val="a")


class DynamicTask(luigi.Task):
    def requires(self):
        return LeafTask(val="static")

    def run(self):
        yield LeafTask(val="dynamic")


class FreezeManifestTest(unittest.TestCase):
    def test_param_dict_order_change_keeps_digest_stable(self):
        manifest_1 = freeze_graph(ConfigTask(config={"a": 1, "b": 2}, name="x"))
        manifest_2 = freeze_graph(ConfigTask(config={"b": 2, "a": 1}, name="x"))
        self.assertEqual(manifest_1.digest(), manifest_2.digest())

    def test_param_kwarg_order_change_keeps_digest_stable(self):
        manifest_1 = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        manifest_2 = freeze_graph(ConfigTask(name="x", config={"a": 1}))
        self.assertEqual(manifest_1.digest(), manifest_2.digest())

    def test_digest_stable_across_json_roundtrip(self):
        manifest = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        reloaded = FreezeManifest.from_json(manifest.to_json())
        self.assertEqual(manifest, reloaded)
        self.assertEqual(manifest.digest(), reloaded.digest())

    def test_shared_dependency_recorded_once(self):
        manifest = freeze_graph([RootA(), RootB()])
        families = [entry["family"] for entry in manifest.tasks]
        self.assertEqual(sorted(families), ["RootA", "RootB", "SharedDep"])
        shared_id = next(
            entry["id"] for entry in manifest.tasks if entry["family"] == "SharedDep"
        )
        dependents = sorted(
            task_id for task_id, dep_id in manifest.edges if dep_id == shared_id
        )
        self.assertEqual(len(dependents), 2)

    def test_root_order_keeps_digest_stable(self):
        manifest_1 = freeze_graph([RootA(), RootB()])
        manifest_2 = freeze_graph([RootB(), RootA()])
        self.assertEqual(manifest_1.digest(), manifest_2.digest())

    def test_manifest_records_outputs_and_edges(self):
        manifest = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        entries = {entry["family"]: entry for entry in manifest.tasks}
        self.assertEqual(
            entries["ConfigTask"]["outputs"],
            ["LocalTarget:/tmp/freeze-manifest-test/config-x"],
        )
        self.assertEqual(
            entries["LeafTask"]["outputs"],
            ["LocalTarget:/tmp/freeze-manifest-test/leaf-x"],
        )
        self.assertEqual(len(manifest.edges), 1)
        self.assertTrue(manifest.is_complete)

    def test_restore_roundtrip_from_registry(self):
        manifest = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        registry = TaskRegistry()
        registry.register(ConfigTask)
        registry.register(LeafTask)
        roots = restore_graph(manifest, registry)
        self.assertEqual(len(roots), 1)
        self.assertIsInstance(roots[0], ConfigTask)
        self.assertEqual(roots[0].name, "x")
        self.assertEqual(dict(roots[0].config), {"a": 1})

    def test_restore_rejects_family_outside_whitelist(self):
        manifest = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        registry = TaskRegistry()
        registry.register(ConfigTask)  # LeafTask deliberately not whitelisted
        with self.assertRaises(UnknownTaskFamilyError):
            restore_graph(manifest, registry)

    def test_manifest_carries_no_importable_module_references(self):
        manifest = freeze_graph(ConfigTask(config={"a": 1}, name="x"))
        payload = manifest.to_json()
        self.assertNotIn("__module__", payload)
        self.assertNotIn("freeze_manifest_test", payload)

    def test_new_dependency_in_implementation_is_rejected_as_drift(self):
        manifest = freeze_graph(DriftTask())
        # Simulate the task implementation growing a new dependency after
        # the manifest was reviewed: same family, same params, more deps.
        registry = {"DriftTask": DriftTask, "LeafTask": LeafTask}
        with mock.patch.object(
            DriftTask,
            "requires",
            lambda self: [LeafTask(val="a"), LeafTask(val="b")],
        ):
            with self.assertRaises(DependencyDriftError) as ctx:
                restore_graph(manifest, registry)
        self.assertIn("added_tasks", str(ctx.exception))
        self.assertIn("added_edges", str(ctx.exception))

    def test_dynamic_requirements_marked_not_fully_frozen(self):
        manifest = freeze_graph(DynamicTask())
        self.assertFalse(manifest.is_complete)
        entries = {entry["family"]: entry for entry in manifest.tasks}
        self.assertFalse(entries["DynamicTask"]["fully_frozen"])
        self.assertTrue(entries["LeafTask"]["fully_frozen"])
        self.assertEqual(
            manifest.incomplete_tasks(), (entries["DynamicTask"]["id"],)
        )

    def test_incomplete_manifest_refused_at_restore(self):
        manifest = freeze_graph(DynamicTask())
        registry = {"DynamicTask": DynamicTask, "LeafTask": LeafTask}
        with self.assertRaises(IncompleteManifestError):
            restore_graph(manifest, registry)

    def test_incomplete_manifest_restorable_when_explicitly_allowed(self):
        manifest = freeze_graph(DynamicTask())
        registry = {"DynamicTask": DynamicTask, "LeafTask": LeafTask}
        roots = restore_graph(manifest, registry, allow_incomplete=True)
        self.assertEqual(len(roots), 1)
        self.assertIsInstance(roots[0], DynamicTask)


if __name__ == "__main__":
    unittest.main()
