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

import tempfile

from helpers import unittest

import luigi
from luigi.scheduler import DONE, RUNNING, Scheduler


def make_scheduler(resources):
    return Scheduler(
        resources=resources,
        retry_delay=-1,
        remove_delay=1000,
        worker_disconnect_delay=10,
        disable_persist=10,
        disable_window=10,
        retry_count=3,
        disable_hard_timeout=60 * 60,
        stable_done_cooldown_secs=0,
    )


class AlternativeResourcesSchedulerTest(unittest.TestCase):
    def setUp(self):
        super(AlternativeResourcesSchedulerTest, self).setUp()
        # 1 GPU and 4 CPU cores available
        self.sch = make_scheduler({"gpu": 1, "cpu": 4})

    def add_task(self, task_id="T", worker="W", **kwargs):
        kwargs.setdefault("resources", {"gpu": 1})
        kwargs.setdefault("alternative_resources", [{"cpu": 4}])
        self.sch.add_task(worker=worker, task_id=task_id, **kwargs)

    def test_primary_plan_chosen_when_available(self):
        self.add_task()
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T")
        self.assertEqual(reply["assigned_resources"], {"gpu": 1})
        self.assertEqual(reply["assigned_resources_index"], 0)

    def test_alternative_plan_chosen_when_primary_unavailable(self):
        # another running task already holds the only GPU
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})
        self.assertEqual(reply["assigned_resources_index"], 1)

    def test_waits_when_no_complete_plan_fits(self):
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")
        # 3 of 4 CPU cores are taken, so 4 CPU cores do not fit either
        self.sch.add_task(worker="X", task_id="C", resources={"cpu": 3})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        reply = self.sch.get_work(worker="W")
        self.assertIsNone(reply["task_id"])

    def test_never_mixes_resources_across_plans(self):
        # 0 GPU available and only 3 of 4 CPU cores free. It must not be able
        # to run by taking half the GPU plan and half the CPU plan.
        self.sch.update_resources(gpu=0, cpu=4)
        self.sch.add_task(worker="X", task_id="C", resources={"cpu": 1})
        self.sch.get_work(worker="X")
        self.sch.add_task(worker="X", task_id="C2", resources={"cpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        reply = self.sch.get_work(worker="W")
        self.assertIsNone(reply["task_id"])

    def test_resources_are_accounted_per_assigned_plan(self):
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        self.sch.get_work(worker="W")

        used = self.sch._used_resources()
        self.assertEqual(used["gpu"], 1)
        self.assertEqual(used["cpu"], 4)

        # another task still cannot start on either plan
        self.add_task(task_id="T2", worker="W2")
        self.sch.add_worker("W2", [])
        self.assertIsNone(self.sch.get_work(worker="W2")["task_id"])

    def test_finishing_releases_the_plan_actually_assigned(self):
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        self.sch.get_work(worker="W")  # T runs on {"cpu": 4}

        task = self.sch._state.get_task("T")
        self.assertEqual(task.resources_running, {"cpu": 4})

        self.sch.add_task(worker="W", task_id="T", status=DONE)
        self.assertIsNone(task.assigned_resources)
        self.assertIsNone(task.assigned_resources_index)
        # the last run's footprint is kept as a record but not counted
        self.assertEqual(task.resources_running, {"cpu": 4})
        self.assertEqual(dict(self.sch._used_resources()), {"gpu": 1})

        # 4 CPU cores are available again, a new task can take that plan
        self.add_task(task_id="T2", worker="W")
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T2")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})

    def test_reduction_stays_within_assigned_plan(self):
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        self.sch.get_work(worker="W")  # assigned {"cpu": 4}

        # reducing a resource of the assigned plan is allowed
        self.sch.decrease_running_task_resources("T", {"cpu": 4})
        self.assertEqual(self.sch.get_running_task_resources("T")["resources"], {"cpu": 0})

        # freed cores let another CPU-plan task through
        self.add_task(task_id="T2", worker="W2")
        self.sch.add_worker("W2", [])
        reply = self.sch.get_work(worker="W2")
        self.assertEqual(reply["task_id"], "T2")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})

    def test_reduction_cannot_switch_to_another_plan(self):
        # T is running on {"gpu": 1}. Asking to reduce a resource that only
        # exists in the alternative plan must be ignored.
        self.add_task()
        self.sch.get_work(worker="W")
        self.assertEqual(
            self.sch.get_running_task_resources("T")["assigned_resources"],
            {"gpu": 1},
        )

        self.sch.decrease_running_task_resources("T", {"cpu": 4})
        running = self.sch.get_running_task_resources("T")["resources"]
        self.assertEqual(running, {"gpu": 1})
        self.assertNotIn("cpu", running)

    def test_priority_still_decides_order(self):
        # high priority task that can only use the GPU plan
        self.sch.add_task(worker="W", task_id="HIGH", resources={"gpu": 1}, priority=10)
        # low priority task that could also fall back to CPU
        self.sch.add_task(worker="W", task_id="LOW", resources={"gpu": 1}, alternative_resources=[{"cpu": 4}], priority=1)

        # LOW is scheduled first (lower priority ... worker picks HIGH first)
        self.assertEqual(self.sch.get_work(worker="W")["task_id"], "HIGH")

        # HIGH holds the GPU, LOW must fall back to its CPU plan
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "LOW")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})

    def test_plan_prefers_primary_then_alternatives_in_order(self):
        self.sch.update_resources(gpu=0, cpu=2, ram=8)
        self.sch.add_task(
            worker="W",
            task_id="T",
            resources={"gpu": 1},
            alternative_resources=[{"cpu": 4}, {"ram": 4}],
        )
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T")
        # cpu plan (needs 4) doesn't fit with cpu=2, ram plan does
        self.assertEqual(reply["assigned_resources"], {"ram": 4})
        self.assertEqual(reply["assigned_resources_index"], 2)

    def test_empty_alternative_list_behaves_like_primary_only(self):
        self.sch.add_task(worker="W", task_id="T", resources={"gpu": 1}, alternative_resources=[])
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T")
        self.assertEqual(reply["assigned_resources_index"], 0)

    def test_task_without_any_resources_still_runs(self):
        self.sch.add_task(worker="W", task_id="T", resources={}, alternative_resources=[])
        reply = self.sch.get_work(worker="W")
        self.assertEqual(reply["task_id"], "T")
        self.assertEqual(reply["assigned_resources"], {})

    def test_assigned_plan_survives_scheduler_reload(self):
        self.sch.add_task(worker="X", task_id="G", resources={"gpu": 1})
        self.sch.get_work(worker="X")

        self.add_task(worker="W")
        self.sch.get_work(worker="W")  # runs on the CPU plan

        with tempfile.NamedTemporaryFile(delete=True) as fn:
            self.sch._state._state_path = fn.name
            self.sch.dump()

            reloaded = make_scheduler({"gpu": 1, "cpu": 4})
            reloaded._state._state_path = fn.name
            reloaded.load()

            task = reloaded._state.get_task("T")
            self.assertEqual(task.status, RUNNING)
            self.assertEqual(task.assigned_resources, {"cpu": 4})
            self.assertEqual(task.assigned_resources_index, 1)
            self.assertEqual(task.resources_running, {"cpu": 4})

            # the reloaded scheduler still counts the assigned plan as used
            used = reloaded._used_resources()
            self.assertEqual(used["cpu"], 4)
            self.assertEqual(used["gpu"], 1)

    def test_running_resources_do_not_change_when_declaration_is_rescheduled(self):
        self.add_task()
        self.sch.get_work(worker="W")  # running on {"gpu": 1}

        # a worker re-reports the task while it is running
        self.sch.add_task(
            worker="W",
            task_id="T",
            resources={"gpu": 1},
            alternative_resources=[{"cpu": 4}],
        )
        task = self.sch._state.get_task("T")
        self.assertEqual(task.assigned_resources, {"gpu": 1})
        self.assertEqual(task.resources_running, {"gpu": 1})


class AssignedResourcesTask(luigi.Task):
    seen = []

    def complete(self):
        return self.task_id in self.seen

    def run(self):
        self.seen.append(self.task_id)
        # the scheduler tells the task which complete plan it received
        assert self.assigned_resources == {"cpu": 4}, self.assigned_resources
        assert self.assigned_resources_index == 1, self.assigned_resources_index


class AssignedResourcesWorkerTest(unittest.TestCase):
    def test_task_instance_receives_selected_plan(self):
        AssignedResourcesTask.seen = []
        sch = make_scheduler({"gpu": 1, "cpu": 4})

        # occupy the GPU with an unrelated running task
        sch.add_task(worker="X", task_id="G_holder", resources={"gpu": 1})
        sch.get_work(worker="X")

        with luigi.worker.Worker(scheduler=sch, worker_processes=1) as w:
            w._config.check_unfulfilled_deps = False
            task = AssignedResourcesTask()
            task.alternative_resources = [{"cpu": 4}]
            task.resources = {"gpu": 1}
            w.add(task)
            w.run()

        self.assertIn(task.task_id, AssignedResourcesTask.seen)


class MultiWorkerAlternativeResourcesTest(unittest.TestCase):
    """Drive a shared scheduler with several real Worker requests."""

    def test_selection_waiting_and_release_across_workers(self):
        sch = make_scheduler({"gpu": 1, "cpu": 4})

        # Worker X holds the single GPU for the duration of the test.
        sch.add_task(worker="X", task_id="GPU_HOLDER", resources={"gpu": 1})
        self.assertEqual(sch.get_work(worker="X")["assigned_resources"], {"gpu": 1})

        # T1 can run on GPU or 4 CPU cores, but neither is free yet (X holds
        # the GPU and the CPU cores will be occupied one by one below).
        sch.add_task(worker="W1", task_id="T1", resources={"gpu": 1}, alternative_resources=[{"cpu": 4}])
        sch.add_task(worker="W2", task_id="C1", resources={"cpu": 2})
        sch.add_task(worker="W2", task_id="C2", resources={"cpu": 2})

        self.assertEqual(sch.get_work(worker="W2")["assigned_resources"], {"cpu": 2})
        self.assertEqual(sch.get_work(worker="W2")["assigned_resources"], {"cpu": 2})

        # no complete plan fits: 0 GPU and 0 CPU cores
        self.assertIsNone(sch.get_work(worker="W1")["task_id"])

        # releasing one of the 2-core tasks still isn't enough for 4 cores
        sch.add_task(worker="W2", task_id="C1", status=DONE)
        self.assertIsNone(sch.get_work(worker="W1")["task_id"])

        # releasing the second 2-core task frees a full CPU plan
        sch.add_task(worker="W2", task_id="C2", status=DONE)
        reply = sch.get_work(worker="W1")
        self.assertEqual(reply["task_id"], "T1")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})
        self.assertEqual(reply["assigned_resources_index"], 1)

        used = sch._used_resources()
        self.assertEqual(dict(used), {"gpu": 1, "cpu": 4})

        # once T1 finishes, its CPU plan is returned; GPU stays held by X
        sch.add_task(worker="W1", task_id="T1", status=DONE)
        self.assertEqual(dict(sch._used_resources()), {"gpu": 1})

        # a freshly scheduled GPU/CPU task can still only get the CPU plan
        sch.add_task(worker="W1", task_id="T2", resources={"gpu": 1}, alternative_resources=[{"cpu": 4}])
        reply = sch.get_work(worker="W1")
        self.assertEqual(reply["task_id"], "T2")
        self.assertEqual(reply["assigned_resources"], {"cpu": 4})
