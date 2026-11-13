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

import pickle
import tempfile
import time

import pytest
from helpers import unittest

from luigi.scheduler import DONE, FAILED, RUNNING, Scheduler, Task

WORKER = "worker-1"
SECOND_WORKER = "worker-2"

# A fixed virtual clock. The tests never sleep and never talk to a real cluster.
FROZEN_NOW = 1000.0


@pytest.mark.scheduler
class SchedulerWaitAgingTest(unittest.TestCase):
    def setUp(self):
        self._real_time = time.time
        time.time = lambda: FROZEN_NOW

    def tearDown(self):
        time.time = self._real_time

    def set_time(self, current_time):
        time.time = lambda: current_time

    def make_scheduler(self, **overrides):
        config = {
            "retry_delay": 0,
            "remove_delay": 100000,
            "worker_disconnect_delay": 100000,
            "disable_persist": 100000,
            "disable_window": 100000,
            "retry_count": 999999999,
            "disable_hard_timeout": 100000,
            "stable_done_cooldown_secs": 0,
            "wait_aging_enabled": True,
            "wait_aging_step": 5,
            "wait_aging_max_boost": 100,
        }
        config.update(overrides)
        return Scheduler(resources={"r1": 2, "r2": 1}, **config)

    def add_for_both_workers(self, sch, task_id, resources=None, priority=0, deps=None):
        kwargs = {"resources": resources or {}, "priority": priority}
        if deps is not None:
            kwargs["deps"] = deps
        sch.add_task(worker=WORKER, task_id=task_id, **kwargs)
        sch.add_task(worker=SECOND_WORKER, task_id=task_id, **kwargs)

    def usage(self, sch):
        return dict(sch._used_resources())

    def assert_within_limits(self, sch):
        used = self.usage(sch)
        limits = sch._resources
        assert all(used.get(name, 0) <= limits[name] for name in limits)
        assert all(amount >= 0 for amount in used.values())

    def test_disabled_aging_keeps_original_priority_order(self):
        sch = self.make_scheduler(wait_aging_enabled=False)
        sch.add_task(worker=WORKER, task_id="old-low", priority=0)
        self.set_time(FROZEN_NOW + 100)
        sch.add_task(worker=WORKER, task_id="new-high", priority=5)

        assert sch.get_work(worker=WORKER)["task_id"] == "new-high"
        assert sch.get_work(worker=WORKER)["task_id"] == "old-low"
        task = sch._state.get_task("old-low")
        assert task.runnable_wait_since is None
        assert task.runnable_wait_id is None

    def test_dependency_waiting_time_does_not_count(self):
        sch = self.make_scheduler()
        sch.add_task(worker=WORKER, task_id="parent", deps=["child"])
        sch.add_task(worker=WORKER, task_id="child")
        parent = sch._state.get_task("parent")

        self.set_time(FROZEN_NOW + 5)
        sch._refresh_wait_episode(parent)
        assert parent.runnable_wait_since is None
        assert sch._wait_boost(parent) == 0

        self.set_time(FROZEN_NOW + 10)
        sch.add_task(worker=WORKER, task_id="child", status=DONE)
        sch._refresh_wait_episode(parent)
        assert parent.runnable_wait_since == FROZEN_NOW + 10
        assert sch._wait_boost(parent) == 0

        self.set_time(FROZEN_NOW + 20)
        assert sch._wait_boost(parent) == 2

    def test_boost_is_capped_and_equal_rank_keeps_fifo_tiebreak(self):
        sch = self.make_scheduler(wait_aging_step=10, wait_aging_max_boost=3)
        sch.add_task(worker=WORKER, task_id="old", priority=0)
        sch._refresh_wait_episode(sch._state.get_task("old"))
        assert sch._state.get_task("old").runnable_wait_since == FROZEN_NOW
        self.set_time(FROZEN_NOW + 1000)
        # Far beyond the cap: the boost never exceeds wait_aging_max_boost.
        assert sch._wait_boost(sch._state.get_task("old")) == 3
        assert sch._rank(sch._state.get_task("old")) == (3, -FROZEN_NOW)

        # A fresh task at the capped effective rank still loses the tie-break
        # to the older task, but a genuinely higher-priority task wins.
        sch.add_task(worker=WORKER, task_id="fresh-same", priority=3)
        sch.add_task(worker=WORKER, task_id="fresh-higher", priority=4)
        assert sch.get_work(worker=WORKER)["task_id"] == "fresh-higher"
        assert sch.get_work(worker=WORKER)["task_id"] == "old"
        assert sch.get_work(worker=WORKER)["task_id"] == "fresh-same"

    def test_aging_boost_is_bounded_by_configured_priority_gap(self):
        sch = self.make_scheduler(wait_aging_step=10, wait_aging_max_boost=2)
        sch.add_task(worker=WORKER, task_id="old", priority=0)
        sch._refresh_wait_episode(sch._state.get_task("old"))
        self.set_time(FROZEN_NOW + 1000)
        # New arrivals are bounded at priority 5; the old task cannot overtake.
        sch.add_task(worker=WORKER, task_id="new", priority=5)
        assert sch.get_work(worker=WORKER)["task_id"] == "new"

    def test_wait_identity_resets_and_persists_consistently(self):
        with tempfile.NamedTemporaryFile(delete=False) as state_file:
            state_path = state_file.name
        sch = self.make_scheduler(state_path=state_path)
        sch.add_task(worker=WORKER, task_id="parent", deps=["child"])
        sch.add_task(worker=WORKER, task_id="child")
        parent = sch._state.get_task("parent")

        self.set_time(FROZEN_NOW + 10)
        sch.get_work(worker=WORKER)  # child starts; parent stays blocked
        sch.get_work(worker=WORKER)  # parent is not runnable yet
        assert parent.runnable_wait_id is None

        self.set_time(FROZEN_NOW + 20)
        sch.add_task(worker=WORKER, task_id="child", status=DONE)
        sch._refresh_wait_episode(parent)
        first_id = parent.runnable_wait_id
        first_since = parent.runnable_wait_since
        assert first_id is not None

        sch.dump()
        reloaded = self.make_scheduler(state_path=state_path)
        reloaded.load()
        reloaded_parent = reloaded._state.get_task("parent")
        assert reloaded_parent.runnable_wait_id == first_id
        assert reloaded_parent.runnable_wait_since == first_since

        # A new wait episode (e.g. after failure/retry) gets a fresh identity.
        self.set_time(FROZEN_NOW + 30)
        reloaded._clear_wait_episode(reloaded_parent)
        reloaded._refresh_wait_episode(reloaded_parent)
        assert reloaded_parent.runnable_wait_id != first_id
        assert reloaded_parent.runnable_wait_since == FROZEN_NOW + 30

    def test_state_pickled_before_aging_feature_still_loads(self):
        legacy_task = Task(task_id="legacy", status="PENDING", deps=set())
        del legacy_task.runnable_wait_since
        del legacy_task.runnable_wait_id
        with tempfile.NamedTemporaryFile(delete=False) as state_file:
            state_path = state_file.name
            pickle.dump(({"legacy": legacy_task}, {}, {}), state_file)

        sch = self.make_scheduler(state_path=state_path)
        sch.load()
        loaded = sch._state.get_task("legacy")
        assert sch._wait_boost(loaded) == 0

        sch.add_task(worker=WORKER, task_id="legacy")
        assert sch.get_work(worker=WORKER)["task_id"] == "legacy"

    def test_releases_and_retries_do_not_return_resources_twice(self):
        sch = self.make_scheduler()
        sch.add_task(worker=WORKER, task_id="task", resources={"r1": 2})
        assert sch.get_work(worker=WORKER)["task_id"] == "task"
        assert self.usage(sch) == {"r1": 2}

        # Repeated completion transitions for the same run are idempotent.
        sch.add_task(worker=WORKER, task_id="task", status=DONE)
        sch.add_task(worker=WORKER, task_id="task", status=DONE)
        assert self.usage(sch) == {}

        # Failure followed by retry keeps accounting at zero until relaunched.
        sch.add_task(worker=WORKER, task_id="task2", resources={"r1": 1})
        assert sch.get_work(worker=WORKER)["task_id"] == "task2"
        sch.add_task(worker=WORKER, task_id="task2", status=FAILED)
        sch.add_task(worker=WORKER, task_id="task2", status=FAILED)
        assert self.usage(sch) == {}
        self.set_time(FROZEN_NOW + 1)
        sch.prune()
        # With retry_delay=0 the task becomes eligible again after one virtual second.
        assert sch._state.get_task("task2").status == "PENDING"
        assert self.usage(sch) == {}
        sch.add_task(worker=WORKER, task_id="task2", resources={"r1": 1})
        assert sch.get_work(worker=WORKER)["task_id"] == "task2"
        assert self.usage(sch) == {"r1": 1}

    def test_dead_worker_releases_resources_once(self):
        sch = self.make_scheduler(worker_disconnect_delay=10)
        sch.add_task(worker=WORKER, task_id="task", resources={"r1": 1, "r2": 1})
        assert sch.get_work(worker=WORKER)["task_id"] == "task"
        assert self.usage(sch) == {"r1": 1, "r2": 1}

        self.set_time(FROZEN_NOW + 20)
        sch.prune()
        task = sch._state.get_task("task")
        assert task.status == FAILED
        assert self.usage(sch) == {}
        sch.prune()
        assert self.usage(sch) == {}

    def test_allocation_is_atomic_across_two_resources(self):
        sch = self.make_scheduler()
        sch.add_task(worker=WORKER, task_id="holder", resources={"r1": 2})
        assert sch.get_work(worker=WORKER)["task_id"] == "holder"

        # The big task needs both resources; it must not take r2 while waiting for r1.
        sch.add_task(worker=WORKER, task_id="big", resources={"r1": 2, "r2": 1})
        assert sch.get_work(worker=WORKER)["task_id"] is None
        assert self.usage(sch) == {"r1": 2}

        sch.add_task(worker=WORKER, task_id="holder", status=DONE)
        assert sch.get_work(worker=WORKER)["task_id"] == "big"
        assert self.usage(sch) == {"r1": 2, "r2": 1}
        self.assert_within_limits(sch)

    def test_continuous_short_tasks_eventually_yield_to_aged_big_task(self):
        allocations = self.run_starvation_scenario(wait_aging_enabled=True)

        assert allocations["max_used"].get("r1", 0) <= 2
        assert allocations["max_used"].get("r2", 0) <= 1
        # The big task starts exactly once and before the short-task stream ends.
        assert allocations["big_start_tick"] == 16
        assert allocations["big_starts"] == 1
        assert allocations["short_started"] > 0

    def test_without_aging_big_task_starves_while_short_tasks_arrive(self):
        allocations = self.run_starvation_scenario(wait_aging_enabled=False)
        assert allocations["big_start_tick"] is None
        assert allocations["max_used"].get("r1", 0) <= 2
        assert allocations["max_used"].get("r2", 0) <= 1

    def run_starvation_scenario(self, wait_aging_enabled):
        sch = self.make_scheduler(wait_aging_enabled=wait_aging_enabled)
        sch.update_resources(r1=1, r2=1)
        start = FROZEN_NOW
        # The big ETL needs the r1 slot plus the single r2 slot atomically.
        self.add_for_both_workers(sch, "big", resources={"r1": 1, "r2": 1}, priority=0)
        # A small ETL initially occupies the only r1 slot.
        self.add_for_both_workers(sch, "seed-1", resources={"r1": 1}, priority=1)

        finish_at = {}
        big_started = False
        big_start_tick = None
        short_started = 0
        max_used = {}
        log = []

        def record_assignment(tick, task_id):
            nonlocal big_started, big_start_tick, short_started
            assert sch._state.get_task(task_id).status == RUNNING
            used = self.usage(sch)
            log.append((tick, task_id, used))
            for name, amount in used.items():
                max_used[name] = max(max_used.get(name, 0), amount)
            self.assert_within_limits(sch)
            if task_id == "big":
                assert not big_started
                big_started = True
                big_start_tick = tick
            elif task_id.startswith("seed-"):
                finish_at.setdefault(tick + 4, []).append(task_id)
            else:
                short_started += 1
                finish_at.setdefault(tick + 2, []).append(task_id)

        for tick in range(40):
            self.set_time(start + tick)
            for task_id in finish_at.pop(tick, []):
                sch.add_task(worker=WORKER, task_id=task_id, status=DONE)

            # Bounded-priority short ETLs keep arriving. The arrival/finish
            # cadence periodically frees the whole r1 slot, which is the only
            # moment the big task can atomically claim r1 + r2.
            if 0 < tick <= 30:
                self.add_for_both_workers(sch, "short-%02d" % tick, resources={"r1": 1}, priority=2)

            for worker_id in (WORKER, SECOND_WORKER):
                task_id = sch.get_work(worker=worker_id)["task_id"]
                if task_id is not None:
                    record_assignment(tick, task_id)

        print("allocations (%d):" % len(log))
        for tick, task_id, used in log:
            print("  tick=%02d -> %-9s used=%s" % (tick, task_id, used))

        return {
            "big_start_tick": big_start_tick,
            "big_starts": int(big_started),
            "short_started": short_started,
            "max_used": max_used,
            "log": log,
        }
