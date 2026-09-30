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

import os
import shutil
import signal
import tempfile
import time
from multiprocessing import Process

from helpers import LuigiTestCase

import luigi
from luigi.scheduler import Scheduler
from luigi.worker import Worker


class StageMarkerTest(LuigiTestCase):
    def test_no_completed_stages_by_default(self):
        task = luigi.Task()
        self.assertEqual(task.completed_stages, {})
        self.assertIsNone(task.last_completed_stage)
        self.assertFalse(task.stage_complete("anything"))

    def test_last_completed_stage_uses_recorded_time(self):
        task = luigi.Task()
        task._resume_stages = {
            "stage-a": {"deps": ("A",), "time": 1.0},
            "stage-b": {"deps": ("B", "C"), "time": 2.0},
        }
        self.assertTrue(task.stage_complete("stage-a"))
        self.assertEqual(task.last_completed_stage, "stage-b")


class _SubmitExternalJob(luigi.Task):
    dir_ = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_, "job_submitted"))

    def run(self):
        # Simulates an external system accepting a job submission.
        with self.output().open("w") as out:
            out.write("job-123")
        # Tell the parent task that the worker may now disappear.
        with open(os.path.join(self.dir_, "ready"), "w"):
            pass


class _FinalizeExternalJob(luigi.Task):
    dir_ = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_, "job_result"))

    def run(self):
        with open(os.path.join(self.dir_, "job_submitted")) as src, self.output().open("w") as dst:
            dst.write("result of " + src.read())


class _TwoStageTask(luigi.Task):
    dir_ = luigi.Parameter()
    kill_after_first_stage = luigi.BoolParameter(default=False, significant=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_, "final"))

    def _submit_external_job_once(self):
        # The "external work" that must never be submitted twice on resume.
        counter = os.path.join(self.dir_, "submit_count")
        if not os.path.exists(counter):
            with open(counter, "w") as out:
                out.write("1")
        return _SubmitExternalJob(dir_=self.dir_)

    def run(self):
        submitted = self._submit_external_job_once()
        yield luigi.DynamicRequirements(submitted, stage="submit_job")

        finalize = _FinalizeExternalJob(dir_=self.dir_)
        yield luigi.DynamicRequirements(finalize, stage="finalize")

        if self.kill_after_first_stage:
            # Wait until the first stage's external work is durably done, then
            # make this worker disappear. SIGKILL mirrors a mid-run worker
            # crash: the worker purge logic reports the task as FAILED.
            deadline = time.time() + 30
            while not os.path.exists(os.path.join(self.dir_, "ready")) and time.time() < deadline:
                time.sleep(0.01)
            os.kill(os.getpid(), signal.SIGKILL)

        with self.output().open("w") as out:
            out.write("done")


class _SharedSchedulerFactory:
    def __init__(self, scheduler):
        self._scheduler = scheduler

    def create_local_scheduler(self, *args, **kwargs):
        return self._scheduler

    def create_worker(self, scheduler, worker_processes, assistant=False):
        return Worker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)


class StagedTaskRecoveryTest(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.dir_ = tempfile.mkdtemp()
        self.state_path = os.path.join(self.dir_, "scheduler.pickle")

    def tearDown(self):
        shutil.rmtree(self.dir_, ignore_errors=True)
        super().tearDown()

    def _read(self, name):
        with open(os.path.join(self.dir_, name)) as inp:
            return inp.read()

    def _build(self, kill):
        # Each worker process gets its own Scheduler instance, but they share
        # the same persisted state file, exactly like a luigid restart.
        scheduler = Scheduler(
            state_path=self.state_path,
            prune_on_get_work=True,
            record_task_history=False,
            worker_disconnect_delay=0.05,
            retry_delay=3600,
        )
        scheduler.load()
        try:
            return luigi.build(
                [_TwoStageTask(dir_=self.dir_, kill_after_first_stage=kill)],
                local_scheduler=True,
                workers=2,
                worker_scheduler_factory=_SharedSchedulerFactory(scheduler),
            )
        finally:
            scheduler.dump()

    def test_worker_exit_between_stages_does_not_resubmit(self):
        # First worker crashes after the first stage's external work is done and
        # its stage marker has been persisted by the scheduler.
        crashed_worker = Process(
            target=self._build,
            kwargs=dict(kill=True),
        )
        crashed_worker.start()
        crashed_worker.join(60)
        self.assertEqual(crashed_worker.exitcode, 0)

        # A fresh worker takes over and finishes the task.
        ok = self._build(kill=False)
        self.assertTrue(ok)

        self.assertEqual(self._read("submit_count"), "1")
        self.assertEqual(self._read("job_result"), "result of job-123")
        self.assertEqual(self._read("final"), "done")

    def test_completed_stage_does_not_mean_task_complete(self):
        crashed_worker = Process(
            target=self._build,
            kwargs=dict(kill=True),
        )
        crashed_worker.start()
        crashed_worker.join(60)
        self.assertEqual(crashed_worker.exitcode, 0)

        # Stage "submit_job" is complete, but the task output was never written.
        self.assertFalse(os.path.exists(os.path.join(self.dir_, "final")))


class _DependencyIdentityBase(luigi.Task):
    dir_ = luigi.Parameter()
    name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_, self.name))

    def run(self):
        with self.output().open("w") as out:
            out.write(self.name)


class _ReplayedStageTask(luigi.Task):
    dir_ = luigi.Parameter()
    changed = luigi.BoolParameter(default=False, significant=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.dir_, "replayed_final"))

    def run(self):
        first_deps = [_DependencyIdentityBase(dir_=self.dir_, name="dep-1")]
        yield luigi.DynamicRequirements(first_deps, stage="only-stage")

        if self.changed:
            # Same stage name, but the code now yields a different dependency.
            second_deps = [_DependencyIdentityBase(dir_=self.dir_, name="dep-2")]
        else:
            second_deps = first_deps
        yield luigi.DynamicRequirements(second_deps, stage="only-stage")

        with self.output().open("w") as out:
            out.write("done")


class StageReplayIdentityTest(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.dir_ = tempfile.mkdtemp()

    def test_same_dependency_identity_reuses_marker(self):
        task = _ReplayedStageTask(dir_=self.dir_, changed=False)
        self.assertTrue(luigi.build([task], local_scheduler=True, workers=1))
        self.assertTrue(task.complete())
        self.assertTrue(os.path.exists(os.path.join(self.dir_, "dep-1")))
        self.assertFalse(os.path.exists(os.path.join(self.dir_, "dep-2")))

    def test_changed_dependency_identity_reruns_stage(self):
        task = _ReplayedStageTask(dir_=self.dir_, changed=True)
        self.assertTrue(luigi.build([task], local_scheduler=True, workers=1))
        self.assertTrue(os.path.exists(os.path.join(self.dir_, "dep-1")))
        self.assertTrue(os.path.exists(os.path.join(self.dir_, "dep-2")))
