# -*- coding: utf-8 -*-
import mock
import pytest

import luigi
from luigi.freeze_manifest import (
    DependencyDriftError,
    GraphNotFullyFrozenError,
    TaskGraphManifest,
    UnknownTaskFamilyError,
    freeze_graph,
    restore_graph,
    verify_graph,
)


class FreezeLeaf(luigi.Task):
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("/tmp/freeze/leaf-{}.txt".format(self.name))


class FreezeMid(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return FreezeLeaf(name="shared")

    def output(self):
        return luigi.LocalTarget("/tmp/freeze/mid-{}.txt".format(self.name))


class FreezeRoot(luigi.Task):
    def requires(self):
        return [FreezeMid(name="left"), FreezeMid(name="right")]

    def output(self):
        return luigi.LocalTarget("/tmp/freeze/root.txt")


class FreezeDictTask(luigi.Task):
    config = luigi.DictParameter()

    def output(self):
        return luigi.LocalTarget("/tmp/freeze/dict.txt")


class FreezeDynamicTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget("/tmp/freeze/dynamic.txt")


class TestFreezeDictOrder:
    def test_dict_param_order_produces_stable_digest(self):
        # Disable the instance cache: otherwise logically equal dict
        # parameters collapse to a single task instance before we can
        # observe the ordering effect at all.
        FreezeDictTask.disable_instance_cache()
        try:
            first = FreezeDictTask(config={"alpha": 1, "beta": {"x": 1, "y": 2}, "gamma": [1, 2]})
            second = FreezeDictTask(config={"gamma": [1, 2], "beta": {"y": 2, "x": 1}, "alpha": 1})

            # Luigi's own task ids differ because DictParameter
            # serialization preserves insertion order ...
            assert first.task_id != second.task_id

            # ... but the frozen manifest normalizes parameters, so the
            # approved digest is stable.
            first_manifest = freeze_graph(first)
            second_manifest = freeze_graph(second)
            assert first_manifest == second_manifest
            assert first_manifest.digest() == second_manifest.digest()
        finally:
            FreezeDictTask.clear_instance_cache()

    def test_digest_is_stable_across_serializations(self):
        manifest = freeze_graph(FreezeRoot())
        assert TaskGraphManifest.from_json(manifest.to_json()).digest() == manifest.digest()


class TestFreezeSharedDependency:
    def test_shared_dependency_is_a_single_node(self):
        manifest = freeze_graph(FreezeRoot())

        # root + 2 mid tasks + 1 shared leaf
        assert len(manifest.tasks) == 4
        leaf_ids = [node_id for node_id, record in manifest.tasks.items() if record["family"] == "FreezeLeaf"]
        assert len(leaf_ids) == 1

        leaf_id = leaf_ids[0]
        mid_ids = sorted(node_id for node_id, record in manifest.tasks.items() if record["family"] == "FreezeMid")
        root_id = next(node_id for node_id, record in manifest.tasks.items() if record["family"] == "FreezeRoot")
        assert set(manifest.edges) == {
            (mid_ids[0], leaf_id),
            (mid_ids[1], leaf_id),
            (root_id, mid_ids[0]),
            (root_id, mid_ids[1]),
        }

    def test_shared_dependency_digest_independent_of_requires_order(self):
        with mock.patch.object(FreezeRoot, "requires", lambda self: [FreezeMid(name="left"), FreezeMid(name="right")]):
            forward = freeze_graph(FreezeRoot())
        with mock.patch.object(FreezeRoot, "requires", lambda self: [FreezeMid(name="right"), FreezeMid(name="left")]):
            backward = freeze_graph(FreezeRoot())
        assert forward.digest() == backward.digest()

    def test_restore_and_verify_shared_graph(self):
        manifest = freeze_graph(FreezeRoot())
        restored = verify_graph(manifest, allowed_families={"FreezeRoot", "FreezeMid", "FreezeLeaf"})
        assert len(restored) == 4
        leaves = [task for task in restored.values() if isinstance(task, FreezeLeaf)]
        assert len(leaves) == 1
        assert leaves[0].name == "shared"


class TestFreezeDrift:
    def test_newly_added_dependency_is_rejected(self):
        manifest = freeze_graph(FreezeMid(name="mid"))

        def drifted_requires(self):
            return [FreezeLeaf(name="shared"), FreezeLeaf(name="sneaky-new-dependency")]

        with mock.patch.object(FreezeMid, "requires", drifted_requires):
            with pytest.raises(DependencyDriftError, match="new dependency edge"):
                verify_graph(manifest)

    def test_removed_dependency_is_rejected(self):
        manifest = freeze_graph(FreezeMid(name="mid"))
        with mock.patch.object(FreezeMid, "requires", lambda self: []):
            with pytest.raises(DependencyDriftError, match="missing dependency edge"):
                verify_graph(manifest)

    def test_output_drift_is_rejected(self):
        manifest = freeze_graph(FreezeLeaf(name="leaf"))

        def drifted_output(self):
            return luigi.LocalTarget("/tmp/freeze/somewhere-else.txt")

        with mock.patch.object(FreezeLeaf, "output", drifted_output):
            with pytest.raises(DependencyDriftError, match="outputs of"):
                verify_graph(manifest)


class TestFreezeDynamicTask:
    def test_dynamic_task_is_marked_not_fully_frozen(self):
        manifest = freeze_graph(FreezeDynamicTask(), dynamic_tasks=[FreezeDynamicTask])

        assert not manifest.is_fully_frozen
        assert len(manifest.incomplete) == 1
        assert manifest.incomplete[0] in manifest.tasks
        with pytest.raises(GraphNotFullyFrozenError):
            manifest.assert_fully_frozen()

    def test_verify_refuses_to_vouch_for_dynamic_graph_by_default(self):
        manifest = freeze_graph(FreezeDynamicTask(), dynamic_tasks=["FreezeDynamicTask"])
        with pytest.raises(GraphNotFullyFrozenError):
            verify_graph(manifest)

        # Static structure can still be checked when explicitly requested.
        restored = verify_graph(manifest, require_fully_frozen=False)
        assert len(restored) == 1

    def test_unmarked_dynamic_task_defaults_to_fully_frozen(self):
        manifest = freeze_graph(FreezeDynamicTask())
        assert manifest.is_fully_frozen


class TestFreezeWhitelist:
    def test_restore_rejects_family_outside_whitelist(self):
        manifest = freeze_graph(FreezeRoot())
        with pytest.raises(UnknownTaskFamilyError, match="whitelist"):
            restore_graph(manifest, allowed_families={"FreezeRoot"})

    def test_restore_rejects_unregistered_family_without_importing(self):
        manifest = freeze_graph(FreezeLeaf(name="leaf"))
        data = manifest.to_dict()
        record = next(iter(data["tasks"].values()))
        record["family"] = "totally.unregistered.Module"
        evil = TaskGraphManifest.from_dict(data)

        with pytest.raises(UnknownTaskFamilyError, match="not registered"):
            restore_graph(evil)
        with pytest.raises(UnknownTaskFamilyError):
            verify_graph(evil)

    def test_restore_roundtrip_params(self):
        manifest = freeze_graph(FreezeDictTask(config={"alpha": 1, "beta": {"x": 1, "y": 2}}))
        restored = restore_graph(manifest)
        (task,) = restored.values()
        assert dict(task.config) == {"alpha": 1, "beta": {"x": 1, "y": 2}}
