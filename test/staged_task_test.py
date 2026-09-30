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

import os
import tempfile

from helpers import LuigiTestCase

import luigi
from luigi.scheduler import Scheduler
from luigi.task import Stage, stage_dependency_fingerprint
from luigi.task_status import DONE, FAILED, PENDING
from luigi.worker import Worker


class StagedTaskTest(LuigiTestCase):
    def setUp(self):
        super(StagedTaskTest, self).setUp()
        self.sch = Scheduler()
        self.tmpdir = tempfile.mkdtemp()

    def _run_worker(self, worker_id, task):
        worker = Worker(scheduler=self.sch, worker_id=worker_id, wait_interval=0.1, wait_jitter=0.1)
        worker.add(task)
        return worker.run()

    def _submissions(self, name):
        path = os.path.join(self.tmpdir, "%s_submissions" % name)
        if not os.path.exists(path):
            return []
        with open(path) as f:
            return f.read().splitlines()

    def _make_task_classes(self):
        tmpdir = self.tmpdir

        class ExtractJob(luigi.Task):
            version = luigi.IntParameter(default=1)

            def output(self):
                return luigi.LocalTarget(os.path.join(tmpdir, "extract_v%d.done" % self.version))

            def run(self):
                with self.output().open("w") as f:
                    f.write("extracted")

        class LoadJob(luigi.Task):
            def output(self):
                return luigi.LocalTarget(os.path.join(tmpdir, "load.done"))

            def run(self):
                with self.output().open("w") as f:
                    f.write("loaded")

        class TwoStageTask(luigi.Task):
            """
            Two-stage task. Each stage submits external work (recorded in a
            submissions file) and then waits for a dependency representing
            the external job. The first time the "load" stage is reached the
            task crashes, simulating the worker exiting mid-run.
            """

            extract_version = 1  # mutated by tests to simulate a code change

            def output(self):
                return luigi.LocalTarget(os.path.join(tmpdir, "final.done"))

            def _submit_external_work(self, name):
                with open(os.path.join(tmpdir, "%s_submissions" % name), "a") as f:
                    f.write("submitted\n")

            def run(self):
                extract = self.stage("extract", [ExtractJob(version=self.extract_version)])
                if not extract.already_complete:
                    self._submit_external_work("extract")
                yield extract

                crash_marker = os.path.join(tmpdir, "crashed_once")
                if not os.path.exists(crash_marker):
                    # simulate the worker exiting after the first stage
                    open(crash_marker, "w").close()
                    raise RuntimeError("simulated worker exit")

                load = self.stage("load", [LoadJob()])
                if not load.already_complete:
                    self._submit_external_work("load")
                yield load

                with self.output().open("w") as f:
                    f.write("done")

        return ExtractJob, LoadJob, TwoStageTask

    def test_resume_after_worker_exit(self):
        ExtractJob, _, TwoStageTask = self._make_task_classes()
        task = TwoStageTask()

        # first worker: completes the "extract" stage, then exits mid-run
        self.assertFalse(self._run_worker("worker-1", task))

        # the extract stage completed even though the task did not: stage
        # success is not task success
        stages = self.sch.get_task_stages(task.task_id)["stages"]
        self.assertEqual(stages["extract"]["status"], DONE)
        self.assertEqual(self.sch._state.get_task(task.task_id).status, FAILED)
        self.assertFalse(task.complete())

        # a new worker resumes the task
        self.assertTrue(self._run_worker("worker-2", task))
        self.assertTrue(task.complete())

        # the external work of the first stage was not submitted again
        self.assertEqual(self._submissions("extract"), ["submitted"])
        self.assertEqual(self._submissions("load"), ["submitted"])

        stages = self.sch.get_task_stages(task.task_id)["stages"]
        self.assertEqual(stages["extract"]["status"], DONE)
        self.assertEqual(stages["load"]["status"], DONE)
        self.assertEqual(stages["extract"]["deps"], [ExtractJob(version=1).task_id])

    def test_changed_dependencies_are_not_treated_as_complete(self):
        ExtractJob, _, TwoStageTask = self._make_task_classes()
        task = TwoStageTask()

        # complete the "extract" stage, then exit mid-run
        self.assertFalse(self._run_worker("worker-1", task))
        self.assertEqual(self._submissions("extract"), ["submitted"])

        # simulate a code change: the "extract" stage now yields a different
        # set of dependencies under the same stage name
        TwoStageTask.extract_version = 2

        self.assertTrue(self._run_worker("worker-2", task))

        # the changed stage was not mistaken for the completed one: the
        # external work was submitted again with the new dependencies
        self.assertEqual(self._submissions("extract"), ["submitted", "submitted"])

        # the scheduler recorded the new dependency identity for the stage
        stages = self.sch.get_task_stages(task.task_id)["stages"]
        self.assertEqual(stages["extract"]["status"], DONE)
        self.assertEqual(stages["extract"]["deps"], [ExtractJob(version=2).task_id])
        self.assertEqual(
            stages["extract"]["fingerprint"],
            stage_dependency_fingerprint([ExtractJob(version=2)]),
        )


class StageUnitTest(LuigiTestCase):
    def test_fingerprint_changes_with_dependencies(self):
        class Dep(luigi.Task):
            n = luigi.IntParameter()

        self.assertEqual(
            stage_dependency_fingerprint([Dep(n=1)]),
            stage_dependency_fingerprint([Dep(n=1)]),
        )
        self.assertNotEqual(
            stage_dependency_fingerprint([Dep(n=1)]),
            stage_dependency_fingerprint([Dep(n=2)]),
        )
        self.assertNotEqual(
            stage_dependency_fingerprint([Dep(n=1)]),
            stage_dependency_fingerprint([Dep(n=1), Dep(n=2)]),
        )

    def test_stage_requires_name(self):
        self.assertRaises(ValueError, Stage, "", [])

    def test_already_complete_without_scheduler_record(self):
        class Dep(luigi.Task):
            pass

        class MyTask(luigi.Task):
            pass

        task = MyTask()
        stage = task.stage("extract", [Dep()])
        self.assertFalse(stage.already_complete)

    def test_already_complete_checks_dependency_identity(self):
        class Dep(luigi.Task):
            n = luigi.IntParameter()

        class MyTask(luigi.Task):
            pass

        task = MyTask()
        task._completed_stages = {
            "extract": {
                "fingerprint": stage_dependency_fingerprint([Dep(n=1)]),
                "status": DONE,
                "deps": [Dep(n=1).task_id],
            }
        }
        # same name and same dependencies: complete
        self.assertTrue(task.stage("extract", [Dep(n=1)]).already_complete)
        # same name but different dependencies: not complete
        self.assertFalse(task.stage("extract", [Dep(n=2)]).already_complete)
        # different name: not complete
        self.assertFalse(task.stage("load", [Dep(n=1)]).already_complete)


class SchedulerStageTest(LuigiTestCase):
    def setUp(self):
        super(SchedulerStageTest, self).setUp()
        self.sch = Scheduler()

    def _add_task(self, task_id="Task_1"):
        self.sch.add_task(task_id=task_id, worker="w1", status=PENDING, runnable=True)

    def test_update_and_get_task_stage(self):
        self._add_task()
        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="abc", deps=["Dep_1"], status=PENDING)
        stages = self.sch.get_task_stages("Task_1")["stages"]
        self.assertEqual(stages["extract"]["fingerprint"], "abc")
        self.assertEqual(stages["extract"]["status"], PENDING)
        self.assertEqual(stages["extract"]["deps"], ["Dep_1"])

        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="abc", deps=["Dep_1"], status=DONE)
        self.assertEqual(self.sch.get_task_stages("Task_1")["stages"]["extract"]["status"], DONE)

    def test_completed_stage_does_not_regress(self):
        self._add_task()
        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="abc", deps=["Dep_1"], status=DONE)
        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="abc", deps=["Dep_1"], status=PENDING)
        self.assertEqual(self.sch.get_task_stages("Task_1")["stages"]["extract"]["status"], DONE)

    def test_changed_fingerprint_resets_stage(self):
        self._add_task()
        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="abc", deps=["Dep_1"], status=DONE)
        self.sch.update_task_stage(task_id="Task_1", worker="w1", stage_name="extract", fingerprint="xyz", deps=["Dep_2"], status=PENDING)
        stage = self.sch.get_task_stages("Task_1")["stages"]["extract"]
        self.assertEqual(stage["fingerprint"], "xyz")
        self.assertEqual(stage["status"], PENDING)
        self.assertEqual(stage["deps"], ["Dep_2"])

    def test_get_task_stages_unknown_task(self):
        self.assertEqual(self.sch.get_task_stages("DoesNotExist")["stages"], {})
