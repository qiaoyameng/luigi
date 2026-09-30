# -*- coding: utf-8 -*-
#
# Copyright 2026 Spotify AB
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

import pytest
from helpers import RunOnceTask, unittest

import luigi
import luigi.scheduler
import luigi.worker
from luigi.scheduler import DONE, Scheduler

SCHEDULER_CONFIG = {
    "retry_delay": 100,
    "remove_delay": 1000,
    "worker_disconnect_delay": 10,
    "disable_persist": 10,
    "disable_window": 10,
    "retry_count": 3,
    "disable_hard_timeout": 60 * 60,
    "stable_done_cooldown_secs": 0,
}

GPU_OR_CPU = [{"gpu": 1}, {"cpu": 4}]


def make_scheduler(**resources):
    return Scheduler(resources=resources, **SCHEDULER_CONFIG)


@pytest.mark.scheduler
class AlternativeResourcesSelectionTest(unittest.TestCase):
    def test_first_fitting_scheme_is_selected(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)

        reply = sch.get_work(worker="W1")

        self.assertEqual("A", reply["task_id"])
        self.assertEqual({"gpu": 1}, reply["resources"])
        self.assertEqual({"gpu": 1}, sch.get_running_task_resources("A")["resources"])

    def test_second_scheme_selected_when_first_unavailable(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="G", resources={"gpu": 1})
        self.assertEqual("G", sch.get_work(worker="W1")["task_id"])

        sch.add_task(worker="W2", task_id="A", resource_alternatives=GPU_OR_CPU)
        reply = sch.get_work(worker="W2")

        self.assertEqual("A", reply["task_id"])
        self.assertEqual({"cpu": 4}, reply["resources"])

    def test_task_waits_when_no_complete_scheme_fits(self):
        # gpu is taken and only 2 of the 4 required cpus are available:
        # the task must wait instead of mixing half of each scheme
        sch = make_scheduler(gpu=1, cpu=2)
        sch.add_task(worker="W1", task_id="G", resources={"gpu": 1})
        self.assertEqual("G", sch.get_work(worker="W1")["task_id"])

        sch.add_task(worker="W2", task_id="A", resource_alternatives=GPU_OR_CPU)
        reply = sch.get_work(worker="W2")

        self.assertIsNone(reply["task_id"])
        self.assertEqual({"gpu": 1, "cpu": 0}, {r: v["used"] for r, v in sch.resources().items() if "used" in v})

    def test_schemes_are_never_combined(self):
        # 1 gpu and 2 cpus are free, but the task needs a whole {gpu: 1}
        # or a whole {cpu: 4}; partial bits of both must not schedule it
        sch = make_scheduler(gpu=1, cpu=2)
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)

        self.assertEqual("A", sch.get_work(worker="W1")["task_id"])

        sch2 = make_scheduler(gpu=0, cpu=2)
        sch2.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)
        self.assertIsNone(sch2.get_work(worker="W1")["task_id"])

    def test_priority_still_applies_with_alternatives(self):
        sch = make_scheduler(gpu=1, cpu=2)
        both_schemes = [{"gpu": 1}, {"cpu": 2}]
        sch.get_work(worker="W_HIGH")  # indicate to the scheduler that W_HIGH is active
        sch.add_task(worker="W_HIGH", task_id="HIGH", resource_alternatives=both_schemes, priority=10)
        sch.add_task(worker="W_LOW", task_id="LOW", resource_alternatives=both_schemes, priority=1)
        sch.add_worker("W_HIGH", [("workers", 2)])
        sch.add_worker("W_LOW", [])

        # the greedy check reserves the gpu scheme for the higher priority task,
        # so the low priority worker only gets the cpu scheme
        reply_low = sch.get_work(worker="W_LOW")
        self.assertEqual("LOW", reply_low["task_id"])
        self.assertEqual({"cpu": 2}, reply_low["resources"])

        reply_high = sch.get_work(worker="W_HIGH")
        self.assertEqual("HIGH", reply_high["task_id"])
        self.assertEqual({"gpu": 1}, reply_high["resources"])

    def test_fixed_resources_still_work(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="A", resources={"gpu": 1})
        reply = sch.get_work(worker="W1")
        self.assertEqual("A", reply["task_id"])
        self.assertEqual({"gpu": 1}, reply["resources"])


@pytest.mark.scheduler
class AlternativeResourcesMultiWorkerTest(unittest.TestCase):
    def test_waiting_and_release_across_workers(self):
        sch = make_scheduler(gpu=1, cpu=4)

        # worker 1 grabs the gpu scheme
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)
        self.assertEqual({"gpu": 1}, sch.get_work(worker="W1")["resources"])

        # worker 2 grabs the cpu scheme
        sch.add_task(worker="W2", task_id="B", resource_alternatives=GPU_OR_CPU)
        self.assertEqual({"cpu": 4}, sch.get_work(worker="W2")["resources"])

        # worker 3 has to wait: no complete scheme is left
        sch.add_task(worker="W3", task_id="C", resource_alternatives=GPU_OR_CPU)
        self.assertIsNone(sch.get_work(worker="W3")["task_id"])

        # worker 1 finishes: exactly the gpu scheme is handed back
        sch.add_task(worker="W1", task_id="A", status=DONE)
        reply = sch.get_work(worker="W3")
        self.assertEqual("C", reply["task_id"])
        self.assertEqual({"gpu": 1}, reply["resources"])

        # worker 2 finishes: the cpu scheme is handed back
        sch.add_task(worker="W3", task_id="C", status=DONE)
        sch.add_task(worker="W2", task_id="B", status=DONE)
        sch.add_task(worker="W4", task_id="D", resource_alternatives=GPU_OR_CPU)
        self.assertEqual({"gpu": 1}, sch.get_work(worker="W4")["resources"])

    def test_used_resources_reflect_actual_allocation(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)
        sch.get_work(worker="W1")

        used = sch.resources()
        self.assertEqual(1, used["gpu"]["used"])
        self.assertEqual(0, used["cpu"]["used"])

    def test_decrease_cannot_turn_into_another_scheme(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)
        self.assertEqual({"gpu": 1}, sch.get_work(worker="W1")["resources"])

        # decreasing resources of the *other* scheme is a no-op
        sch.decrease_running_task_resources("A", {"cpu": 4})
        self.assertEqual({"gpu": 1}, sch.get_running_task_resources("A")["resources"])

        # decreasing the allocated scheme shrinks it, but never morphs it
        # into the {"cpu": 4} scheme
        sch.decrease_running_task_resources("A", {"gpu": 1})
        self.assertEqual({"gpu": 0}, sch.get_running_task_resources("A")["resources"])

        # the freed gpu is available again, cpus were never touched
        sch.add_task(worker="W2", task_id="B", resources={"gpu": 1})
        self.assertEqual("B", sch.get_work(worker="W2")["task_id"])
        self.assertEqual(0, sch.resources()["cpu"]["used"])

    def test_reload_remembers_actual_allocation(self):
        sch = make_scheduler(gpu=1, cpu=4)
        sch.add_task(worker="W1", task_id="A", resource_alternatives=GPU_OR_CPU)
        self.assertEqual({"gpu": 1}, sch.get_work(worker="W1")["resources"])

        with tempfile.NamedTemporaryFile(delete=True) as fn:
            sch._state._state_path = fn.name
            sch.dump()

            sch2 = make_scheduler(gpu=1, cpu=4)
            sch2._state._state_path = fn.name
            sch2.load()

        # the gpu is still occupied by A after the reload
        self.assertEqual(1, sch2.resources()["gpu"]["used"])
        self.assertEqual({"gpu": 1}, sch2.get_running_task_resources("A")["resources"])

        sch2.add_task(worker="W2", task_id="B", resources={"gpu": 1})
        self.assertIsNone(sch2.get_work(worker="W2")["task_id"])

        # but the untouched cpus are still available
        sch2.add_task(worker="W3", task_id="C", resources={"cpu": 4})
        self.assertEqual("C", sch2.get_work(worker="W3")["task_id"])


class AlternativeSchemeTask(RunOnceTask):
    resource_alternatives = GPU_OR_CPU
    seen_resources_running = None

    def run(self):
        AlternativeSchemeTask.seen_resources_running = self.resources_running
        super(AlternativeSchemeTask, self).run()


@pytest.mark.scheduler
class AlternativeResourcesWorkerTest(unittest.TestCase):
    def test_task_learns_selected_scheme(self):
        AlternativeSchemeTask.seen_resources_running = None
        sch = make_scheduler(gpu=1, cpu=4)

        with luigi.worker.Worker(scheduler=sch) as w:
            task = AlternativeSchemeTask()
            w.add(task)
            self.assertTrue(w.run())

        self.assertEqual({"gpu": 1}, AlternativeSchemeTask.seen_resources_running)

    def test_task_learns_fallback_scheme(self):
        AlternativeSchemeTask.seen_resources_running = None
        sch = make_scheduler(gpu=0, cpu=4)

        with luigi.worker.Worker(scheduler=sch) as w:
            task = AlternativeSchemeTask()
            w.add(task)
            self.assertTrue(w.run())

        self.assertEqual({"cpu": 4}, AlternativeSchemeTask.seen_resources_running)
