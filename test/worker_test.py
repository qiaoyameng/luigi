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

import email.parser
import functools
import logging
import multiprocessing
import os
import shutil
import signal
import tempfile
import threading
import time

import mock
import psutil
from helpers import LuigiTestCase, skipOnTravisAndGithubActions, temporary_unloaded_module, unittest, with_config

import luigi.notifications
import luigi.task_register
import luigi.worker
from luigi import Event, ExternalTask, RemoteScheduler, Task
from luigi.cmdline import luigi_run
from luigi.mock import MockFileSystem, MockTarget
from luigi.rpc import RPCError
from luigi.scheduler import Scheduler
from luigi.worker import Worker

luigi.notifications.DEBUG = True


def _multiprocessing_supported():
    """Detect sandboxes where multiprocessing manager sockets cannot bind."""
    manager = None
    try:
        manager = multiprocessing.Manager()
        shared = manager.dict()
        shared["ok"] = True
        return shared["ok"] is True
    except (EOFError, OSError):
        return False
    finally:
        if manager is not None:
            manager.shutdown()


class DummyTask(Task):
    def __init__(self, *args, **kwargs):
        super(DummyTask, self).__init__(*args, **kwargs)
        self.has_run = False

    def complete(self):
        return self.has_run

    def run(self):
        logging.debug("%s - setting has_run", self)
        self.has_run = True


class DynamicDummyTask(Task):
    p = luigi.Parameter()
    sleep = luigi.FloatParameter(default=0.5, significant=False)

    def output(self):
        return luigi.LocalTarget(self.p)

    def run(self):
        with self.output().open("w") as f:
            f.write("Done!")
        time.sleep(self.sleep)  # so we can benchmark & see if parallelization works


class DynamicDummyTaskWithNamespace(DynamicDummyTask):
    task_namespace = "banana"


class DynamicRequires(Task):
    p = luigi.Parameter()
    use_banana_task = luigi.BoolParameter(default=False)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.p, "parent"))

    def run(self):
        if self.use_banana_task:
            task_cls = DynamicDummyTaskWithNamespace
        else:
            task_cls = DynamicDummyTask
        dummy_targets = yield [task_cls(os.path.join(self.p, str(i))) for i in range(5)]
        dummy_targets += yield [task_cls(os.path.join(self.p, str(i))) for i in range(5, 7)]
        with self.output().open("w") as f:
            for i, d in enumerate(dummy_targets):
                for line in d.open("r"):
                    print("%d: %s" % (i, line.strip()), file=f)


class DynamicRequiresWrapped(Task):
    p = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.p, "parent"))

    def run(self):
        reqs = [DynamicDummyTask(p=os.path.join(self.p, "%s.txt" % i), sleep=0.0) for i in range(10)]

        # yield again as DynamicRequires
        yield luigi.DynamicRequirements(reqs)

        # and again with a custom complete function that does base name comparisons
        def custom_complete(complete_fn):
            if not complete_fn(reqs[0]):
                return False
            paths = [task.output().path for task in reqs]
            basenames = os.listdir(os.path.dirname(paths[0]))
            self._custom_complete_called = True
            self._custom_complete_result = all(os.path.basename(path) in basenames for path in paths)
            return self._custom_complete_result

        yield luigi.DynamicRequirements(reqs, custom_complete)

        with self.output().open("w") as f:
            f.write("Done!")


class DynamicRequiresOtherModule(Task):
    p = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.p, "baz"))

    def run(self):
        import other_module

        other_target_foo = yield other_module.OtherModuleTask(os.path.join(self.p, "foo"))  # NOQA
        other_target_bar = yield other_module.OtherModuleTask(os.path.join(self.p, "bar"))  # NOQA

        with self.output().open("w") as f:
            f.write("Done!")


class DummyErrorTask(Task):
    retry_index = 0

    def run(self):
        self.retry_index += 1
        raise Exception("Retry index is %s for %s" % (self.retry_index, self.task_family))


class WorkerTest(LuigiTestCase):
    def run(self, result=None):
        self.sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10, stable_done_cooldown_secs=0)
        self.time = time.time
        with Worker(scheduler=self.sch, worker_id="X") as w, Worker(scheduler=self.sch, worker_id="Y") as w2:
            self.w = w
            self.w2 = w2
            super(WorkerTest, self).run(result)

        if time.time != self.time:
            time.time = self.time

    def setTime(self, t):
        time.time = lambda: t

    def test_dep(self):
        class A(Task):
            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()
        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())
        self.assertTrue(a.has_run)
        self.assertTrue(b.has_run)

    def test_external_dep(self):
        class A(ExternalTask):
            def complete(self):
                return False

        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_externalized_dep(self):
        class A(Task):
            has_run = False

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        a = A()

        class B(A):
            def requires(self):
                return luigi.task.externalize(a)

        b = B()

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_legacy_externalized_dep(self):
        class A(Task):
            has_run = False

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        a = A()
        a.run = NotImplemented

        class B(A):
            def requires(self):
                return a

        b = B()

        self.assertTrue(self.w.add(b))
        self.assertTrue(self.w.run())

        self.assertFalse(a.has_run)
        self.assertFalse(b.has_run)

    def test_type_error_in_tracking_run_deprecated(self):
        class A(Task):
            num_runs = 0

            def complete(self):
                return False

            def run(self, tracking_url_callback=None):
                self.num_runs += 1
                raise TypeError("bad type")

        a = A()
        self.assertTrue(self.w.add(a))
        self.assertFalse(self.w.run())

        # Should only run and fail once, not retry because of the type error
        self.assertEqual(1, a.num_runs)

    def test_tracking_url(self):
        tracking_url = "http://test_url.com/"

        class A(Task):
            has_run = False

            def complete(self):
                return self.has_run

            def run(self):
                self.set_tracking_url(tracking_url)
                self.has_run = True

        a = A()
        self.assertTrue(self.w.add(a))
        self.assertTrue(self.w.run())
        tasks = self.sch.task_list("DONE", "")
        self.assertEqual(1, len(tasks))
        self.assertEqual(tracking_url, tasks[a.task_id]["tracking_url"])

    def test_fail(self):
        class CustomException(BaseException):
            def __init__(self, msg):
                self.msg = msg

        class A(Task):
            def run(self):
                self.has_run = True
                raise CustomException("bad things")

            def complete(self):
                return self.has_run

        a = A()

        class B(Task):
            def requires(self):
                return a

            def run(self):
                self.has_run = True

            def complete(self):
                return self.has_run

        b = B()

        a.has_run = False
        b.has_run = False

        self.assertTrue(self.w.add(b))
        self.assertFalse(self.w.run())

        self.assertTrue(a.has_run)
        self.assertFalse(b.has_run)

    def test_unknown_dep(self):
        # see related test_remove_dep test (grep for it)
        class A(ExternalTask):
            def complete(self):
                return False

        class C(Task):
            def complete(self):
                return True

        def get_b(dep):
            class B(Task):
                def requires(self):
                    return dep

                def run(self):
                    self.has_run = True

                def complete(self):
                    return False

            b = B()
            b.has_run = False
            return b

        b_a = get_b(A())
        b_c = get_b(C())

        self.assertTrue(self.w.add(b_a))
        # So now another worker goes in and schedules C -> B
        # This should remove the dep A -> B but will screw up the first worker
        self.assertTrue(self.w2.add(b_c))

        self.assertFalse(self.w.run())  # should not run anything - the worker should detect that A is broken
        self.assertFalse(b_a.has_run)
        # not sure what should happen??
        # self.w2.run() # should run B since C is fulfilled
        # self.assertTrue(b_c.has_run)

    def test_unfulfilled_dep(self):
        class A(Task):
            def complete(self):
                return self.done

            def run(self):
                self.done = True

        def get_b(a):
            class B(A):
                def requires(self):
                    return a

            b = B()
            b.done = False
            a.done = True
            return b

        a = A()
        b = get_b(a)

        self.assertTrue(self.w.add(b))
        a.done = False
        self.w.run()
        self.assertTrue(a.complete())
        self.assertTrue(b.complete())

    def test_check_unfulfilled_deps_config(self):
        class A(Task):
            i = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(A, self).__init__(*args, **kwargs)
                self.complete_count = 0
                self.has_run = False

            def complete(self):
                self.complete_count += 1
                return self.has_run

            def run(self):
                self.has_run = True

        class B(A):
            def requires(self):
                return A(i=self.i)

        # test the enabled features
        with Worker(scheduler=self.sch, worker_id="1") as w:
            w._config.check_unfulfilled_deps = True
            a1 = A(i=1)
            b1 = B(i=1)
            self.assertTrue(w.add(b1))
            self.assertEqual(a1.complete_count, 1)
            self.assertEqual(b1.complete_count, 1)
            w.run()
            self.assertTrue(a1.complete())
            self.assertTrue(b1.complete())
            self.assertEqual(a1.complete_count, 3)
            self.assertEqual(b1.complete_count, 2)

        # test the disabled features
        with Worker(scheduler=self.sch, worker_id="2") as w:
            w._config.check_unfulfilled_deps = False
            a2 = A(i=2)
            b2 = B(i=2)
            self.assertTrue(w.add(b2))
            self.assertEqual(a2.complete_count, 1)
            self.assertEqual(b2.complete_count, 1)
            w.run()
            self.assertTrue(a2.complete())
            self.assertTrue(b2.complete())
            self.assertEqual(a2.complete_count, 2)
            self.assertEqual(b2.complete_count, 2)

    def test_cache_task_completion_config(self):
        class A(Task):
            i = luigi.IntParameter()

            def __init__(self, *args, **kwargs):
                super(A, self).__init__(*args, **kwargs)
                self.complete_count = 0
                self.has_run = False

            def complete(self):
                self.complete_count += 1
                return self.has_run

            def run(self):
                self.has_run = True

        class B(A):
            def run(self):
                yield A(i=self.i + 0)
                yield A(i=self.i + 1)
                yield A(i=self.i + 2)
                self.has_run = True

        # test with enabled cache_task_completion
        with Worker(scheduler=self.sch, worker_id="2", cache_task_completion=True) as w:
            b0 = B(i=0)
            a0 = A(i=0)
            a1 = A(i=1)
            a2 = A(i=2)
            self.assertTrue(w.add(b0))
            # a's are required dynamically, so their counts must be 0
            self.assertEqual(b0.complete_count, 1)
            self.assertEqual(a0.complete_count, 0)
            self.assertEqual(a1.complete_count, 0)
            self.assertEqual(a2.complete_count, 0)
            w.run()
            # the complete methods of a's yielded first in b's run method were called equally often
            self.assertEqual(b0.complete_count, 1)
            self.assertEqual(a0.complete_count, 2)
            self.assertEqual(a1.complete_count, 2)
            self.assertEqual(a2.complete_count, 2)

        # test with disabled cache_task_completion
        with Worker(scheduler=self.sch, worker_id="2", cache_task_completion=False) as w:
            b10 = B(i=10)
            a10 = A(i=10)
            a11 = A(i=11)
            a12 = A(i=12)
            self.assertTrue(w.add(b10))
            # a's are required dynamically, so their counts must be 0
            self.assertEqual(b10.complete_count, 1)
            self.assertEqual(a10.complete_count, 0)
            self.assertEqual(a11.complete_count, 0)
            self.assertEqual(a12.complete_count, 0)
            w.run()
            # the complete methods of a's yielded first in b's run method were called more often
            self.assertEqual(b10.complete_count, 1)
            self.assertEqual(a10.complete_count, 5)
            self.assertEqual(a11.complete_count, 4)
            self.assertEqual(a12.complete_count, 3)

        # test with enabled check_complete_on_run
        with Worker(scheduler=self.sch, worker_id="2", check_complete_on_run=True) as w:
            b20 = B(i=20)
            a20 = A(i=20)
            a21 = A(i=21)
            a22 = A(i=22)
            self.assertTrue(w.add(b20))
            # a's are required dynamically, so their counts must be 0
            self.assertEqual(b20.complete_count, 1)
            self.assertEqual(a20.complete_count, 0)
            self.assertEqual(a21.complete_count, 0)
            self.assertEqual(a22.complete_count, 0)
            w.run()
            # the complete methods of a's yielded first in b's run method were called more often
            self.assertEqual(b20.complete_count, 2)
            self.assertEqual(a20.complete_count, 6)
            self.assertEqual(a21.complete_count, 5)
            self.assertEqual(a22.complete_count, 4)

    def test_gets_missed_work(self):
        class A(Task):
            done = False

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        a = A()
        self.assertTrue(self.w.add(a))

        # simulate a missed get_work response
        self.assertEqual(a.task_id, self.sch.get_work(worker="X")["task_id"])

        self.assertTrue(self.w.run())
        self.assertTrue(a.complete())

    def test_avoid_infinite_reschedule(self):
        class A(Task):
            def complete(self):
                return False

        class B(Task):
            def complete(self):
                return False

            def requires(self):
                return A()

        self.assertTrue(self.w.add(B()))
        self.assertFalse(self.w.run())

    def test_fails_registering_signal(self):
        with mock.patch("luigi.worker.signal", spec=["signal"]):
            # mock will raise an attribute error getting signal.SIGUSR1
            Worker()

    def test_allow_reschedule_with_many_missing_deps(self):
        class A(Task):
            """Task that must run twice to succeed"""

            i = luigi.IntParameter()

            runs = 0

            def complete(self):
                return self.runs >= 2

            def run(self):
                self.runs += 1

        class B(Task):
            done = False

            def requires(self):
                return map(A, range(20))

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        b = B()
        w = Worker(scheduler=self.sch, worker_id="X", max_reschedules=1)
        self.assertTrue(w.add(b))
        self.assertFalse(w.run())

        # For b to be done, we must have rescheduled its dependencies to run them twice
        self.assertTrue(b.complete())
        self.assertTrue(all(a.complete() for a in b.deps()))

    def test_interleaved_workers(self):
        class A(DummyTask):
            pass

        a = A()

        class B(DummyTask):
            def requires(self):
                return a

        ExternalB = luigi.task.externalize(B)

        b = B()
        eb = ExternalB()
        self.assertEqual(str(eb), "B()")

        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="X") as w, Worker(scheduler=sch, worker_id="Y") as w2:
            self.assertTrue(w.add(b))
            self.assertTrue(w2.add(eb))
            logging.debug("RUNNING BROKEN WORKER")
            self.assertTrue(w2.run())
            self.assertFalse(a.complete())
            self.assertFalse(b.complete())
            logging.debug("RUNNING FUNCTIONAL WORKER")
            self.assertTrue(w.run())
            self.assertTrue(a.complete())
            self.assertTrue(b.complete())

    def test_interleaved_workers2(self):
        # two tasks without dependencies, one external, one not
        class B(DummyTask):
            pass

        ExternalB = luigi.task.externalize(B)

        b = B()
        eb = ExternalB()

        self.assertEqual(str(eb), "B()")

        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="X") as w, Worker(scheduler=sch, worker_id="Y") as w2:
            self.assertTrue(w2.add(eb))
            self.assertTrue(w.add(b))

            self.assertTrue(w2.run())
            self.assertFalse(b.complete())
            self.assertTrue(w.run())
            self.assertTrue(b.complete())

    def test_interleaved_workers3(self):
        class A(DummyTask):
            def run(self):
                logging.debug("running A")
                time.sleep(0.1)
                super(A, self).run()

        a = A()

        class B(DummyTask):
            def requires(self):
                return a

            def run(self):
                logging.debug("running B")
                super(B, self).run()

        b = B()

        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

        with Worker(scheduler=sch, worker_id="X", keep_alive=True, count_uniques=True) as w:
            with Worker(scheduler=sch, worker_id="Y", keep_alive=True, count_uniques=True, wait_interval=0.1, wait_jitter=0.05) as w2:
                self.assertTrue(w.add(a))
                self.assertTrue(w2.add(b))

                threading.Thread(target=w.run).start()
                self.assertTrue(w2.run())

                self.assertTrue(a.complete())
                self.assertTrue(b.complete())

    def test_die_for_non_unique_pending(self):
        class A(DummyTask):
            def run(self):
                logging.debug("running A")
                time.sleep(0.1)
                super(A, self).run()

        a = A()

        class B(DummyTask):
            def requires(self):
                return a

            def run(self):
                logging.debug("running B")
                super(B, self).run()

        b = B()

        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

        with Worker(scheduler=sch, worker_id="X", keep_alive=True, count_uniques=True) as w:
            with Worker(scheduler=sch, worker_id="Y", keep_alive=True, count_uniques=True, wait_interval=0.1, wait_jitter=0.05) as w2:
                self.assertTrue(w.add(b))
                self.assertTrue(w2.add(b))

                self.assertEqual(w._get_work()[0], a.task_id)
                self.assertTrue(w2.run())

                self.assertFalse(a.complete())
                self.assertFalse(b.complete())

    def test_complete_exception(self):
        "Tests that a task is still scheduled if its sister task crashes in the complete() method"

        class A(DummyTask):
            def complete(self):
                raise Exception("doh")

        a = A()

        class C(DummyTask):
            pass

        c = C()

        class B(DummyTask):
            def requires(self):
                return a, c

        b = B()
        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="foo") as w:
            self.assertFalse(w.add(b))
            self.assertTrue(w.run())
            self.assertFalse(b.has_run)
            self.assertTrue(c.has_run)
            self.assertFalse(a.has_run)

    def test_requires_exception(self):
        class A(DummyTask):
            def requires(self):
                raise Exception("doh")

        a = A()

        class D(DummyTask):
            pass

        d = D()

        class C(DummyTask):
            def requires(self):
                return d

        c = C()

        class B(DummyTask):
            def requires(self):
                return c, a

        b = B()
        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="foo") as w:
            self.assertFalse(w.add(b))
            self.assertTrue(w.run())
            self.assertFalse(b.has_run)
            self.assertTrue(c.has_run)
            self.assertTrue(d.has_run)
            self.assertFalse(a.has_run)

    def test_run_csv_batch_job(self):
        completed = set()

        class CsvBatchJob(luigi.Task):
            values = luigi.parameter.Parameter(batch_method=",".join)
            has_run = False

            def run(self):
                completed.update(self.values.split(","))
                self.has_run = True

            def complete(self):
                return all(value in completed for value in self.values.split(","))

        tasks = [CsvBatchJob(str(i)) for i in range(10)]
        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertTrue(self.w.run())

        for task in tasks:
            self.assertTrue(task.complete())
            self.assertFalse(task.has_run)

    def test_run_max_batch_job(self):
        completed = set()

        class MaxBatchJob(luigi.Task):
            value = luigi.IntParameter(batch_method=max)
            has_run = False

            def run(self):
                completed.add(self.value)
                self.has_run = True

            def complete(self):
                return any(self.value <= ran for ran in completed)

        tasks = [MaxBatchJob(i) for i in range(10)]
        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertTrue(self.w.run())

        for task in tasks:
            self.assertTrue(task.complete())
            # only task number 9 should run
            self.assertFalse(task.has_run and task.value < 9)

    def test_run_batch_job_unbatched(self):
        completed = set()

        class MaxNonBatchJob(luigi.Task):
            value = luigi.IntParameter(batch_method=max)
            has_run = False

            batchable = False

            def run(self):
                completed.add(self.value)
                self.has_run = True

            def complete(self):
                return self.value in completed

        tasks = [MaxNonBatchJob((i,)) for i in range(10)]
        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertTrue(self.w.run())

        for task in tasks:
            self.assertTrue(task.complete())
            self.assertTrue(task.has_run)

    def test_run_batch_job_limit_batch_size(self):
        completed = set()
        runs = []

        class CsvLimitedBatchJob(luigi.Task):
            value = luigi.parameter.Parameter(batch_method=",".join)
            has_run = False

            max_batch_size = 4

            def run(self):
                completed.update(self.value.split(","))
                runs.append(self)

            def complete(self):
                return all(value in completed for value in self.value.split(","))

        tasks = [CsvLimitedBatchJob(str(i)) for i in range(11)]
        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertTrue(self.w.run())

        for task in tasks:
            self.assertTrue(task.complete())

        self.assertEqual(3, len(runs))

    def test_fail_max_batch_job(self):
        class MaxBatchFailJob(luigi.Task):
            value = luigi.IntParameter(batch_method=max)
            has_run = False

            def run(self):
                self.has_run = True
                assert False

            def complete(self):
                return False

        tasks = [MaxBatchFailJob(i) for i in range(10)]
        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertFalse(self.w.run())

        for task in tasks:
            # only task number 9 should run
            self.assertFalse(task.has_run and task.value < 9)

        self.assertEqual({task.task_id for task in tasks}, set(self.sch.task_list("FAILED", "")))

    def test_gracefully_handle_batch_method_failure(self):
        class BadBatchMethodTask(DummyTask):
            priority = 10
            batch_int_param = luigi.IntParameter(batch_method=int.__add__)  # should be sum

        bad_tasks = [BadBatchMethodTask(i) for i in range(5)]
        good_tasks = [DummyTask()]
        all_tasks = good_tasks + bad_tasks

        self.assertFalse(any(task.complete() for task in all_tasks))

        worker = Worker(scheduler=Scheduler(retry_count=1), keep_alive=True)

        for task in all_tasks:
            self.assertTrue(worker.add(task))
        self.assertFalse(worker.run())
        self.assertFalse(any(task.complete() for task in bad_tasks))

        # we only get to run the good task if the bad task failures were handled gracefully
        self.assertTrue(all(task.complete() for task in good_tasks))

    def test_post_error_message_for_failed_batch_methods(self):
        class BadBatchMethodTask(DummyTask):
            batch_int_param = luigi.IntParameter(batch_method=int.__add__)  # should be sum

        tasks = [BadBatchMethodTask(1), BadBatchMethodTask(2)]

        for task in tasks:
            self.assertTrue(self.w.add(task))
        self.assertFalse(self.w.run())

        failed_ids = set(self.sch.task_list("FAILED", ""))
        self.assertEqual({task.task_id for task in tasks}, failed_ids)
        self.assertTrue(all(self.sch.fetch_error(task_id)["error"] for task_id in failed_ids))


class WorkerKeepAliveTests(LuigiTestCase):
    def setUp(self):
        self.sch = Scheduler()
        super(WorkerKeepAliveTests, self).setUp()

    def _worker_keep_alive_test(self, first_should_live, second_should_live, task_status=None, **worker_args):
        worker_args.update(
            {
                "scheduler": self.sch,
                "worker_processes": 0,
                "wait_interval": 0.01,
                "wait_jitter": 0.0,
            }
        )
        w1 = Worker(worker_id="w1", **worker_args)
        w2 = Worker(worker_id="w2", **worker_args)
        with w1 as worker1, w2 as worker2:
            worker1.add(DummyTask())
            t1 = threading.Thread(target=worker1.run)
            t1.start()

            worker2.add(DummyTask())
            t2 = threading.Thread(target=worker2.run)
            t2.start()

            if task_status:
                self.sch.add_task(worker="DummyWorker", task_id=DummyTask().task_id, status=task_status)

            # allow workers to run their get work loops a few times
            time.sleep(0.1)

            try:
                self.assertEqual(first_should_live, t1.is_alive())
                self.assertEqual(second_should_live, t2.is_alive())

            finally:
                # mark the task done so the worker threads will die
                self.sch.add_task(worker="DummyWorker", task_id=DummyTask().task_id, status="DONE")
                t1.join()
                t2.join()

    def test_no_keep_alive(self):
        self._worker_keep_alive_test(
            first_should_live=False,
            second_should_live=False,
        )

    def test_keep_alive(self):
        self._worker_keep_alive_test(
            first_should_live=True,
            second_should_live=True,
            keep_alive=True,
        )

    def test_keep_alive_count_uniques(self):
        self._worker_keep_alive_test(
            first_should_live=False,
            second_should_live=False,
            keep_alive=True,
            count_uniques=True,
        )

    def test_keep_alive_count_last_scheduled(self):
        self._worker_keep_alive_test(
            first_should_live=False,
            second_should_live=True,
            keep_alive=True,
            count_last_scheduled=True,
        )

    def test_keep_alive_through_failure(self):
        self._worker_keep_alive_test(
            first_should_live=True,
            second_should_live=True,
            keep_alive=True,
            task_status="FAILED",
        )

    def test_do_not_keep_alive_through_disable(self):
        self._worker_keep_alive_test(
            first_should_live=False,
            second_should_live=False,
            keep_alive=True,
            task_status="DISABLED",
        )


class WorkerInterruptedTest(unittest.TestCase):
    def setUp(self):
        self.sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

    requiring_sigusr = unittest.skipUnless(hasattr(signal, "SIGUSR1"), "signal.SIGUSR1 not found on this system")

    def _test_stop_getting_new_work(self, worker):
        d = DummyTask()
        with worker:
            worker.add(d)  # For assistant its ok that other tasks add it
            self.assertFalse(d.complete())
            worker.handle_interrupt(signal.SIGUSR1, None)
            worker.run()
            self.assertFalse(d.complete())

    @requiring_sigusr
    def test_stop_getting_new_work(self):
        self._test_stop_getting_new_work(Worker(scheduler=self.sch))

    @requiring_sigusr
    def test_stop_getting_new_work_assistant(self):
        self._test_stop_getting_new_work(Worker(scheduler=self.sch, keep_alive=False, assistant=True))

    @requiring_sigusr
    def test_stop_getting_new_work_assistant_keep_alive(self):
        self._test_stop_getting_new_work(Worker(scheduler=self.sch, keep_alive=True, assistant=True))

    def test_existence_of_disabling_option(self):
        # any code equivalent of `os.kill(os.getpid(), signal.SIGUSR1)`
        # seem to give some sort of a "InvocationError"
        Worker(no_install_shutdown_handler=True)

    @with_config({"worker": {"no_install_shutdown_handler": "True"}})
    def test_can_run_luigi_in_thread(self):
        class A(DummyTask):
            pass

        task = A()
        # Note that ``signal.signal(signal.SIGUSR1, fn)`` can only be called in the main thread.
        # So if we do not disable the shutdown handler, this would fail.
        t = threading.Thread(target=lambda: luigi.build([task], local_scheduler=True))
        t.start()
        t.join()
        self.assertTrue(task.complete())


class WorkerDrainTest(LuigiTestCase):
    """Behavior of the explicit, observable worker drain entry point."""

    def setUp(self):
        super(WorkerDrainTest, self).setUp()
        self.sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

    def _worker(self, **kwargs):
        kwargs.setdefault("scheduler", self.sch)
        kwargs.setdefault("worker_id", "drainer")
        kwargs.setdefault("wait_interval", 0.05)
        kwargs.setdefault("wait_jitter", 0.0)
        kwargs.setdefault("ping_interval", 0.05)
        kwargs.setdefault("no_install_shutdown_handler", True)
        return Worker(**kwargs)

    def test_drain_finishes_accepted_task_and_refuses_new_root(self):
        gate = threading.Event()
        released = threading.Event()

        class BlockingTask(DummyTask):
            def run(self):
                gate.set()
                released.wait(5)
                super(BlockingTask, self).run()

        class LateTask(DummyTask):
            pass

        blocked = BlockingTask()
        with self._worker() as worker:
            worker.add(blocked)
            runner = threading.Thread(target=worker.run)
            runner.start()
            self.assertTrue(gate.wait(5))

            # Non-blocking drain flips the worker into draining mode while the
            # accepted task is still in flight.
            requested = worker.drain(timeout=0)
            self.assertTrue(worker.is_draining)
            self.assertFalse(requested.drained)
            self.assertTrue(requested.timed_out)

            # No new root task may enter the worker once draining.
            with self.assertRaises(luigi.worker.WorkerDrainError):
                worker.add(LateTask())

            # A timeout must not be reported as a completed drain.
            timed_out = worker.drain(timeout=0.1)
            self.assertFalse(timed_out.drained)
            self.assertTrue(timed_out.timed_out)
            self.assertEqual(timed_out.running_tasks, 1)

            # Repeated (idempotent) drain requests share the same outcome once
            # the blocker is released.
            released.set()
            result = worker.drain(timeout=5)
            runner.join(5)
            self.assertFalse(runner.is_alive())
            # All accepted work has released its execution resources: no task
            # process/slot is still occupied by the drained worker.
            self.assertEqual(worker._running_tasks, {})

        self.assertTrue(result.drained)
        self.assertFalse(result.timed_out)
        self.assertEqual(result.running_tasks, 0)
        self.assertEqual(result.pending_tasks, 0)
        self.assertTrue(blocked.complete())
        # The rejected root task never ran.
        self.assertFalse(LateTask().complete())

    def test_accepted_task_runs_exactly_once_while_draining(self):
        run_count = {"n": 0}

        class CountedTask(Task):
            def complete(self):
                return run_count["n"] >= 1

            def run(self):
                run_count["n"] += 1

        task = CountedTask()
        with self._worker() as worker:
            worker.add(task)
            worker.drain(timeout=0)
            run_success = worker.run()
            drain_result = worker.drain(timeout=5)

        self.assertTrue(run_success)
        self.assertTrue(drain_result.drained)
        self.assertEqual(run_count["n"], 1)
        self.assertEqual(drain_result.completed_tasks, 1)

    def test_repeated_drain_is_idempotent_and_returns_same_result(self):
        class A(DummyTask):
            pass

        with self._worker() as worker:
            worker.add(A())
            first = worker.drain(timeout=0)
            second = worker.drain(timeout=0)
            self.assertFalse(first.drained)
            self.assertFalse(second.drained)

            worker.run()
            finished_one = worker.drain(timeout=5)
            finished_two = worker.drain(timeout=5)

        self.assertTrue(finished_one.drained)
        self.assertEqual(finished_one, finished_two)
        self.assertIs(finished_one, finished_two)

    def test_drain_keeps_heartbeating_so_scheduler_keeps_worker_active(self):
        gate = threading.Event()
        released = threading.Event()
        pings = {"n": 0}

        class HeartbeatScheduler(Scheduler):
            def ping(self, **kwargs):
                pings["n"] += 1
                return super(HeartbeatScheduler, self).ping(**kwargs)

        class SlowTask(DummyTask):
            def run(self):
                gate.set()
                released.wait(5)
                super(SlowTask, self).run()

        scheduler = HeartbeatScheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with self._worker(scheduler=scheduler) as worker:
            worker.add(SlowTask())
            runner = threading.Thread(target=worker.run)
            runner.start()
            self.assertTrue(gate.wait(5))
            worker.drain(timeout=0)
            time.sleep(0.3)
            released.set()
            result = worker.drain(timeout=5)
            runner.join(5)

        self.assertTrue(result.drained)
        self.assertGreater(pings["n"], 1)
        # The scheduler still considers the worker registered, not dead.
        self.assertIn("drainer", scheduler._state.get_worker_ids())

    def test_dynamic_dependencies_are_processed_while_draining(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, ignore_errors=True)

        with self._worker() as worker:
            worker.add(DynamicRequires(tmp))
            worker.drain(timeout=0)
            result = worker.run()
            drain_result = worker.drain(timeout=10)

        self.assertTrue(result)
        self.assertTrue(drain_result.drained)
        parent = os.path.join(tmp, "parent")
        self.assertTrue(os.path.exists(parent))
        for i in range(7):
            self.assertTrue(os.path.exists(os.path.join(tmp, str(i))))

    def test_failed_task_retry_does_not_block_drain_and_is_reported(self):
        # retry_delay=0 so the failed task is immediately schedulable again and
        # gets retried to completion within the drain.
        scheduler = Scheduler(retry_delay=0, remove_delay=1000, worker_disconnect_delay=10)
        attempts = {"n": 0}

        class FlakyTask(Task):
            def complete(self):
                return attempts["n"] >= 2

            def run(self):
                attempts["n"] += 1
                if attempts["n"] < 2:
                    raise RuntimeError("transient failure %d" % attempts["n"])

        with self._worker(scheduler=scheduler) as worker:
            worker.add(FlakyTask())
            worker.drain(timeout=0)
            worker.run()
            result = worker.drain(timeout=5)

        # The task failed once and then retried to success within the drain.
        # run_succeeded stays False because Luigi records the earlier failure,
        # but the drain itself completes deterministically with the task done.
        self.assertTrue(FlakyTask().complete())
        self.assertTrue(result.drained)
        self.assertEqual(attempts["n"], 2)
        self.assertEqual(result.pending_tasks, 0)
        self.assertEqual(result.running_tasks, 0)

    def test_failed_task_waiting_for_retry_is_reported_not_force_stopped(self):
        # A long retry delay parks the failed task; the drain still completes
        # deterministically and surfaces the parked task instead of hanging.
        scheduler = Scheduler(retry_delay=10000, remove_delay=1000, worker_disconnect_delay=10)

        class AlwaysFails(Task):
            def complete(self):
                return False

            def run(self):
                raise RuntimeError("permanent failure")

        with self._worker(scheduler=scheduler) as worker:
            worker.add(AlwaysFails())
            worker.drain(timeout=0)
            success = worker.run()
            result = worker.drain(timeout=5)
            # The task process is gone; nothing is running anymore.
            self.assertEqual(worker._running_tasks, {})

        self.assertFalse(success)
        self.assertTrue(result.drained)
        self.assertFalse(result.timed_out)
        self.assertEqual(result.failed_retry_tasks, 1)
        self.assertEqual(result.running_tasks, 0)

    def test_force_stop_is_separate_from_drain(self):
        if not _multiprocessing_supported():
            self.skipTest("multiprocessing sockets unavailable in this environment")

        gate = threading.Event()
        release = threading.Event()

        class HungTask(DummyTask):
            def run(self):
                gate.set()
                release.wait(10)

        worker = self._worker(worker_processes=2, force_multiprocessing=True)
        with worker:
            worker.add(HungTask())
            runner = threading.Thread(target=worker.run)
            runner.start()
            self.assertTrue(gate.wait(5))

            # Drain must not terminate the in-flight task process.
            worker.drain(timeout=0)
            pending = worker.drain(timeout=0.2)
            self.assertFalse(pending.drained)
            self.assertEqual(pending.running_tasks, 1)
            self.assertTrue(all(process.is_alive() for process in worker._running_tasks.values()))

            release.set()
            finished = worker.drain(timeout=5)
            runner.join(5)

        self.assertTrue(finished.drained)
        self.assertEqual(finished.running_tasks, 0)

    def test_drain_after_run_returns_deterministic_result(self):
        class A(DummyTask):
            pass

        worker = self._worker()
        with worker:
            worker.add(A())
            self.assertTrue(worker.run())
            # Drain requested only after the run has already finished.
            result = worker.drain(timeout=5)

        self.assertTrue(result.drained)
        self.assertFalse(result.timed_out)

    def test_drain_timeout_without_run_returns_deterministic_answer(self):
        class A(DummyTask):
            pass

        with self._worker() as worker:
            worker.add(A())
            # run() is never started; the wait must not hang and must not
            # falsely report the worker as drained.
            start = time.monotonic()
            result = worker.drain(timeout=0.2)
            elapsed = time.monotonic() - start

        self.assertFalse(result.drained)
        self.assertTrue(result.timed_out)
        self.assertLess(elapsed, 2.0)
        # The accepted task was never executed.
        self.assertFalse(A().complete())

    def test_drain_requested_before_run_still_processes_accepted_chain(self):
        run_order = []

        class Leaf(Task):
            n = luigi.Parameter()

            def complete(self):
                return getattr(self, "_done", False)

            def run(self):
                run_order.append(str(self.n))
                self._done = True

        class ChainRoot(Task):
            def requires(self):
                return [Leaf(n=i) for i in range(3)]

            def complete(self):
                return all(Leaf(n=i).complete() for i in range(3))

            def run(self):
                run_order.append("root")

        with self._worker() as worker:
            worker.add(ChainRoot())
            # Request the drain before the run loop has started at all.
            requested = worker.drain(timeout=0)
            self.assertFalse(requested.drained)
            runner = threading.Thread(target=worker.run)
            runner.start()
            result = worker.drain(timeout=10)
            runner.join(5)
            self.assertFalse(runner.is_alive())

        self.assertTrue(result.drained)
        self.assertEqual(sorted(run_order), ["0", "1", "2", "root"])
        self.assertEqual(result.pending_tasks, 0)
        self.assertEqual(result.running_tasks, 0)

    def test_multiple_drain_waiters_all_get_definitive_result(self):
        gate = threading.Event()
        released = threading.Event()

        class SlowTask(DummyTask):
            def run(self):
                gate.set()
                released.wait(5)
                super(SlowTask, self).run()

        results = []

        def wait_for_drain(worker):
            results.append(worker.drain(timeout=10))

        with self._worker() as worker:
            worker.add(SlowTask())
            runner = threading.Thread(target=worker.run)
            runner.start()
            self.assertTrue(gate.wait(5))
            worker.drain(timeout=0)
            waiters = [threading.Thread(target=wait_for_drain, args=(worker,)) for _ in range(3)]
            for waiter in waiters:
                waiter.start()
            time.sleep(0.2)
            released.set()
            for waiter in waiters:
                waiter.join(5)
                self.assertFalse(waiter.is_alive())
            runner.join(5)

        self.assertEqual(len(results), 3)
        self.assertTrue(all(result.drained for result in results))


class WorkerDisabledTest(LuigiTestCase):
    def make_sch(self):
        return Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)

    def _test_stop_getting_new_work_build(self, sch, worker):
        """
        I got motivated to create this test case when I saw that the
        execution_summary crashed after my first attempted solution.
        """

        class KillWorkerTask(luigi.Task):
            did_actually_run = False

            def run(self):
                sch.disable_worker("my_worker_id")
                KillWorkerTask.did_actually_run = True

        class Factory:
            def create_local_scheduler(self, *args, **kwargs):
                return sch

            def create_worker(self, *args, **kwargs):
                return worker

        luigi.build([KillWorkerTask()], worker_scheduler_factory=Factory(), local_scheduler=True)
        self.assertTrue(KillWorkerTask.did_actually_run)

    def _test_stop_getting_new_work_manual(self, sch, worker):
        d = DummyTask()
        with worker:
            worker.add(d)  # For assistant its ok that other tasks add it
            self.assertFalse(d.complete())
            sch.disable_worker("my_worker_id")
            worker.run()  # Note: Test could fail by hanging on this line
            self.assertFalse(d.complete())

    def _test_stop_getting_new_work(self, **worker_kwargs):
        worker_kwargs["worker_id"] = "my_worker_id"

        sch = self.make_sch()
        worker_kwargs["scheduler"] = sch
        self._test_stop_getting_new_work_manual(sch, Worker(**worker_kwargs))

        sch = self.make_sch()
        worker_kwargs["scheduler"] = sch
        self._test_stop_getting_new_work_build(sch, Worker(**worker_kwargs))

    def test_stop_getting_new_work_keep_alive(self):
        self._test_stop_getting_new_work(keep_alive=True, assistant=False)

    def test_stop_getting_new_work_assistant(self):
        self._test_stop_getting_new_work(keep_alive=False, assistant=True)

    def test_stop_getting_new_work_assistant_keep_alive(self):
        self._test_stop_getting_new_work(keep_alive=True, assistant=True)


class DynamicDependenciesTest(LuigiTestCase):
    n_workers = 1
    timeout = float("inf")

    def setUp(self):
        self.p = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.p)

    def test_dynamic_dependencies(self, use_banana_task=False):
        t0 = time.time()
        t = DynamicRequires(p=self.p, use_banana_task=use_banana_task)
        luigi.build([t], local_scheduler=True, workers=self.n_workers)
        self.assertTrue(t.complete())

        # loop through output and verify
        with t.output().open("r") as f:
            for i in range(7):
                self.assertEqual(f.readline().strip(), "%d: Done!" % i)

        self.assertTrue(time.time() - t0 < self.timeout)

    def test_dynamic_dependencies_with_namespace(self):
        self.test_dynamic_dependencies(use_banana_task=True)

    def test_dynamic_dependencies_other_module(self):
        t = DynamicRequiresOtherModule(p=self.p)
        luigi.build([t], local_scheduler=True, workers=self.n_workers)
        self.assertTrue(t.complete())

    def test_wrapped_dynamic_requirements(self):
        t = DynamicRequiresWrapped(p=self.p)
        luigi.build([t], local_scheduler=True, workers=1)
        self.assertTrue(t.complete())
        self.assertTrue(getattr(t, "_custom_complete_called", False))
        self.assertTrue(getattr(t, "_custom_complete_result", False))


class DynamicDependenciesWithMultipleWorkersTest(DynamicDependenciesTest):
    n_workers = 100
    timeout = 10.0  # We run 7 tasks that take 0.5s each so it should take less than 3.5s


class WorkerPingThreadTests(unittest.TestCase):
    def test_ping_retry(self):
        """Worker ping fails once. Ping continues to try to connect to scheduler

        Kind of ugly since it uses actual timing with sleep to test the thread
        """
        sch = Scheduler(
            retry_delay=100,
            remove_delay=1000,
            worker_disconnect_delay=10,
        )

        self._total_pings = 0  # class var so it can be accessed from fail_ping

        def fail_ping(worker):
            # this will be called from within keep-alive thread...
            self._total_pings += 1
            raise Exception("Some random exception")

        sch.ping = fail_ping

        with Worker(
            scheduler=sch,
            worker_id="foo",
            ping_interval=0.01,  # very short between pings to make test fast
        ):
            # let the keep-alive thread run for a bit...
            time.sleep(0.1)  # yes, this is ugly but it's exactly what we need to test
        self.assertTrue(self._total_pings > 1, msg="Didn't retry pings (%d pings performed)" % (self._total_pings,))

    def test_ping_thread_shutdown(self):
        with Worker(ping_interval=0.01) as w:
            self.assertTrue(w._keep_alive_thread.is_alive())
        self.assertFalse(w._keep_alive_thread.is_alive())


def email_patch(test_func, email_config=None):
    EMAIL_CONFIG = {"email": {"receiver": "not-a-real-email-address-for-test-only", "force_send": "true"}}
    if email_config is not None:
        EMAIL_CONFIG.update(email_config)
    emails = []

    def mock_send_email(sender, recipients, msg):
        emails.append(msg)

    @with_config(EMAIL_CONFIG)
    @functools.wraps(test_func)
    @mock.patch("smtplib.SMTP")
    def run_test(self, smtp):
        smtp().sendmail.side_effect = mock_send_email
        test_func(self, emails)

    return run_test


def custom_email_patch(config):
    return functools.partial(email_patch, email_config=config)


class WorkerEmailTest(LuigiTestCase):
    def run(self, result=None):
        super(WorkerEmailTest, self).setUp()
        sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        with Worker(scheduler=sch, worker_id="foo") as self.worker:
            super(WorkerEmailTest, self).run(result)

    @email_patch
    def test_connection_error(self, emails):
        sch = RemoteScheduler("http://tld.invalid:1337", connect_timeout=1)
        sch._rpc_retry_wait = 1  # shorten wait time to speed up tests

        class A(DummyTask):
            pass

        a = A()
        self.assertEqual(emails, [])
        with Worker(scheduler=sch) as worker:
            try:
                worker.add(a)
            except RPCError as e:
                self.assertTrue(str(e).find("Errors (3 attempts)") != -1)
                self.assertNotEqual(emails, [])
                self.assertTrue(emails[0].find("Luigi: Framework error while scheduling %s" % (a,)) != -1)
            else:
                self.fail()

    @email_patch
    def test_complete_error(self, emails):
        class A(DummyTask):
            def complete(self):
                raise Exception("b0rk")

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.assertFalse(a.has_run)

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_complete_error_email_batch(self, emails):
        class A(DummyTask):
            def complete(self):
                raise Exception("b0rk")

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        a = A()
        self.assertEqual(emails, [])
        worker.add(a)
        self.assertEqual(emails, [])
        worker.run()
        self.assertEqual(emails, [])
        self.assertFalse(a.has_run)
        scheduler.prune()
        self.assertTrue("1 scheduling failure" in emails[0])

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_complete_error_email_batch_to_owner(self, emails):
        class A(DummyTask):
            owner_email = "a_owner@test.com"

            def complete(self):
                raise Exception("b0rk")

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        a = A()
        self.assertEqual(emails, [])
        worker.add(a)
        self.assertEqual(emails, [])
        worker.run()
        self.assertEqual(emails, [])
        self.assertFalse(a.has_run)
        scheduler.prune()
        self.assertTrue(any("1 scheduling failure" in email and "a_owner@test.com" in email for email in emails))

    @email_patch
    def test_announce_scheduling_failure_unexpected_error(self, emails):

        class A(DummyTask):
            owner_email = "a_owner@test.com"

            def complete(self):
                pass

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        a = A()

        with mock.patch.object(worker._scheduler, "announce_scheduling_failure", side_effect=Exception("Unexpected")), self.assertRaises(Exception):
            worker.add(a)
        self.assertTrue(len(emails) == 2)  # One for `complete` error, one for exception in announcing.
        self.assertTrue("Luigi: Framework error while scheduling" in emails[1])
        self.assertTrue("a_owner@test.com" in emails[1])

    @email_patch
    def test_requires_error(self, emails):
        class A(DummyTask):
            def requires(self):
                raise Exception("b0rk")

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
        self.assertFalse(a.has_run)

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_requires_error_email_batch(self, emails):
        class A(DummyTask):
            def requires(self):
                raise Exception("b0rk")

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        a = A()
        self.assertEqual(emails, [])
        worker.add(a)
        self.assertEqual(emails, [])
        worker.run()
        self.assertFalse(a.has_run)
        scheduler.prune()
        self.assertTrue("1 scheduling failure" in emails[0])

    @email_patch
    def test_complete_return_value(self, emails):
        class A(DummyTask):
            def complete(self):
                pass  # no return value should be an error

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.worker.run()
        self.assertTrue(emails[0].find("Luigi: %s failed scheduling" % (a,)) != -1)
        self.assertFalse(a.has_run)

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_complete_return_value_email_batch(self, emails):
        class A(DummyTask):
            def complete(self):
                pass  # no return value should be an error

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        a = A()
        self.assertEqual(emails, [])
        worker.add(a)
        self.assertEqual(emails, [])
        self.worker.run()
        self.assertEqual(emails, [])
        self.assertFalse(a.has_run)
        scheduler.prune()
        self.assertTrue("1 scheduling failure" in emails[0])

    @email_patch
    def test_run_error(self, emails):
        class A(luigi.Task):
            def run(self):
                raise Exception("b0rk")

        a = A()
        luigi.build([a], workers=1, local_scheduler=True)
        self.assertEqual(1, len(emails))
        self.assertTrue(emails[0].find("Luigi: %s FAILED" % (a,)) != -1)

    @email_patch
    def test_run_error_long_traceback(self, emails):
        class A(luigi.Task):
            def run(self):
                raise Exception("b0rk" * 10500)

        a = A()
        luigi.build([a], workers=1, local_scheduler=True)
        self.assertTrue(len(emails[0]) < 10000)
        self.assertTrue(emails[0].find("Traceback exceeds max length and has been truncated"))

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_run_error_email_batch(self, emails):
        class A(luigi.Task):
            owner_email = ["a@test.com", "b@test.com"]

            def run(self):
                raise Exception("b0rk")

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        worker.add(A())
        worker.run()
        scheduler.prune()
        self.assertEqual(3, len(emails))
        self.assertTrue(any("a@test.com" in email for email in emails))
        self.assertTrue(any("b@test.com" in email for email in emails))

    @with_config({"batch_email": {"email_interval": "0"}, "worker": {"send_failure_email": "False"}})
    @email_patch
    def test_run_error_batch_email_string(self, emails):
        class A(luigi.Task):
            owner_email = "a@test.com"

            def run(self):
                raise Exception("b0rk")

        scheduler = Scheduler(batch_emails=True)
        worker = Worker(scheduler)
        worker.add(A())
        worker.run()
        scheduler.prune()
        self.assertEqual(2, len(emails))
        self.assertTrue(any("a@test.com" in email for email in emails))

    @with_config({"worker": {"send_failure_email": "False"}})
    @email_patch
    def test_run_error_no_email(self, emails):
        class A(luigi.Task):
            def run(self):
                raise Exception("b0rk")

        luigi.build([A()], workers=1, local_scheduler=True)
        self.assertFalse(emails)

    @staticmethod
    def read_email(email_msg):
        subject_obj, body_obj = email.parser.Parser().parsestr(email_msg).walk()
        return str(subject_obj["Subject"]), str(body_obj.get_payload(decode=True))

    @email_patch
    def test_task_process_dies_with_email(self, emails):
        a = SendSignalTask(signal.SIGKILL)
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertEqual(1, len(emails))
        subject, body = self.read_email(emails[0])
        self.assertIn("Luigi: {} FAILED".format(a), subject)
        self.assertIn("died unexpectedly with exit code -9", body)

    @with_config({"worker": {"send_failure_email": "False"}})
    @email_patch
    def test_task_process_dies_no_email(self, emails):
        luigi.build([SendSignalTask(signal.SIGKILL)], workers=2, local_scheduler=True)
        self.assertEqual([], emails)

    @email_patch
    def test_task_times_out(self, emails):
        class A(luigi.Task):
            worker_timeout = 0.0001

            def run(self):
                time.sleep(5)

        a = A()
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertEqual(1, len(emails))
        subject, body = self.read_email(emails[0])
        self.assertIn("Luigi: %s FAILED" % (a,), subject)
        self.assertIn("timed out after 0.0001 seconds and was terminated.", body)

    @with_config({"worker": {"send_failure_email": "False"}})
    @email_patch
    def test_task_times_out_no_email(self, emails):
        class A(luigi.Task):
            worker_timeout = 0.0001

            def run(self):
                time.sleep(5)

        luigi.build([A()], workers=2, local_scheduler=True)
        self.assertEqual([], emails)

    @with_config(dict(worker=dict(retry_external_tasks="true")))
    @email_patch
    def test_external_task_retries(self, emails):
        """
        Test that we do not send error emails on the failures of external tasks
        """

        class A(luigi.ExternalTask):
            pass

        a = A()
        luigi.build([a], workers=2, local_scheduler=True)
        self.assertEqual(emails, [])

    @email_patch
    def test_no_error(self, emails):
        class A(DummyTask):
            pass

        a = A()
        self.assertEqual(emails, [])
        self.worker.add(a)
        self.assertEqual(emails, [])
        self.worker.run()
        self.assertEqual(emails, [])
        self.assertTrue(a.complete())

    @custom_email_patch({"email": {"receiver": "not-a-real-email-address-for-test-only", "format": "none"}})
    def test_disable_emails(self, emails):
        class A(luigi.Task):
            def complete(self):
                raise Exception("b0rk")

        self.worker.add(A())
        self.assertEqual(emails, [])


class RaiseSystemExit(luigi.Task):
    def run(self):
        raise SystemExit("System exit!!")


class SendSignalTask(luigi.Task):
    signal = luigi.IntParameter()

    def run(self):
        os.kill(os.getpid(), self.signal)


class HangTheWorkerTask(luigi.Task):
    worker_timeout = luigi.IntParameter(default=None)

    def run(self):
        while True:
            pass

    def complete(self):
        return False


class MultipleWorkersTest(LuigiTestCase):
    @unittest.skip("Always skip. There are many intermittent failures")
    def test_multiple_workers(self):
        # Test using multiple workers
        # Also test generating classes dynamically since this may reflect issues with
        # various platform and how multiprocessing is implemented. If it's using os.fork
        # under the hood it should be fine, but dynamic classses can't be pickled, so
        # other implementations of multiprocessing (using spawn etc) may fail
        class MyDynamicTask(luigi.Task):
            x = luigi.Parameter()

            def run(self):
                time.sleep(0.1)

        t0 = time.time()
        luigi.build([MyDynamicTask(i) for i in range(100)], workers=100, local_scheduler=True)
        self.assertTrue(time.time() < t0 + 5.0)  # should ideally take exactly 0.1s, but definitely less than 10.0

    def test_zero_workers(self):
        d = DummyTask()
        luigi.build([d], workers=0, local_scheduler=True)
        self.assertFalse(d.complete())

    def test_system_exit(self):
        # This would hang indefinitely before this fix:
        # https://github.com/spotify/luigi/pull/439
        luigi.build([RaiseSystemExit()], workers=2, local_scheduler=True)

    def test_term_worker(self):
        luigi.build([SendSignalTask(signal.SIGTERM)], workers=2, local_scheduler=True)

    def test_kill_worker(self):
        luigi.build([SendSignalTask(signal.SIGKILL)], workers=2, local_scheduler=True)

    def test_purge_multiple_workers(self):
        w = Worker(worker_processes=2, wait_interval=0.01)
        t1 = SendSignalTask(signal.SIGTERM)
        t2 = SendSignalTask(signal.SIGKILL)
        w.add(t1)
        w.add(t2)

        w._run_task(t1.task_id)
        w._run_task(t2.task_id)
        time.sleep(1.0)

        w._handle_next_task()
        w._handle_next_task()
        w._handle_next_task()

    def test_stop_worker_kills_subprocesses(self):
        with Worker(worker_processes=2) as w:
            hung_task = HangTheWorkerTask()
            w.add(hung_task)

            w._run_task(hung_task.task_id)
            pids = [p.pid for p in w._running_tasks.values()]
            self.assertEqual(1, len(pids))
            pid = pids[0]

            def is_running():
                return pid in {p.pid for p in psutil.Process().children()}

            self.assertTrue(is_running())
        self.assertFalse(is_running())

    @mock.patch("luigi.worker.time")
    def test_no_process_leak_from_repeatedly_running_same_task(self, worker_time):
        with Worker(worker_processes=2) as w:
            hung_task = HangTheWorkerTask()
            w.add(hung_task)

            w._run_task(hung_task.task_id)
            children = set(psutil.Process().children())

            # repeatedly try to run the same task id
            for _ in range(10):
                worker_time.sleep.reset_mock()
                w._run_task(hung_task.task_id)

                # should sleep after each attempt
                worker_time.sleep.assert_called_once_with(mock.ANY)

            # only one process should be running
            self.assertEqual(children, set(psutil.Process().children()))

    def test_time_out_hung_worker(self):
        luigi.build([HangTheWorkerTask(0.1)], workers=2, local_scheduler=True)

    def test_time_out_hung_single_worker(self):
        luigi.build([HangTheWorkerTask(0.1)], workers=1, local_scheduler=True)

    @skipOnTravisAndGithubActions("https://travis-ci.org/spotify/luigi/jobs/72953986")
    @mock.patch("luigi.worker.time")
    def test_purge_hung_worker_default_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask()
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 5
        w._handle_next_task()
        self.assertEqual(1, len(w._running_tasks))

        mock_time.time.return_value = 6
        w._handle_next_task()
        self.assertEqual(0, len(w._running_tasks))

    @skipOnTravisAndGithubActions("https://travis-ci.org/spotify/luigi/jobs/76645264")
    @mock.patch("luigi.worker.time")
    def test_purge_hung_worker_override_timeout_time(self, mock_time):
        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask(worker_timeout=10)
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 10
        w._handle_next_task()
        self.assertEqual(1, len(w._running_tasks))

        mock_time.time.return_value = 11
        w._handle_next_task()
        self.assertEqual(0, len(w._running_tasks))


class Dummy2Task(Task):
    p = luigi.Parameter()

    def output(self):
        return MockTarget(self.p)

    def run(self):
        f = self.output().open("w")
        f.write("test")
        f.close()


class AssistantTest(LuigiTestCase):
    def run(self, result=None):
        self.sch = Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.assistant = Worker(scheduler=self.sch, worker_id="Y", assistant=True)
        with Worker(scheduler=self.sch, worker_id="X") as w:
            self.w = w
            super(AssistantTest, self).run(result)

    def test_get_work(self):
        d = Dummy2Task("123")
        self.w.add(d)

        self.assertFalse(d.complete())
        self.assistant.run()
        self.assertTrue(d.complete())

    def test_bad_job_type(self):
        class Dummy3Task(Dummy2Task):
            task_family = "UnknownTaskFamily"

        d = Dummy3Task("123")
        self.w.add(d)

        self.assertFalse(d.complete())
        self.assertFalse(self.assistant.run())
        self.assertFalse(d.complete())
        self.assertEqual(list(self.sch.task_list("FAILED", "").keys()), [d.task_id])

    def test_unimported_job_type(self):
        MODULE_CONTENTS = b"""
import luigi


class UnimportedTask(luigi.Task):
    def complete(self):
        return False
"""
        reg = luigi.task_register.Register._get_reg()

        class UnimportedTask(luigi.Task):
            task_module = None  # Set it here, so it's generally settable

        luigi.task_register.Register._set_reg(reg)

        task = UnimportedTask()

        # verify that it can't run the task without the module info necessary to import it
        self.w.add(task)
        self.assertFalse(self.assistant.run())
        self.assertEqual(list(self.sch.task_list("FAILED", "").keys()), [task.task_id])

        # check that it can import with the right module
        with temporary_unloaded_module(MODULE_CONTENTS) as task.task_module:
            self.w.add(task)
            self.assertTrue(self.assistant.run())
            self.assertEqual(list(self.sch.task_list("DONE", "").keys()), [task.task_id])

    def test_unimported_job_sends_failure_message(self):
        class NotInAssistantTask(luigi.Task):
            task_family = "Unknown"
            task_module = None

        task = NotInAssistantTask()
        self.w.add(task)
        self.assertFalse(self.assistant.run())
        self.assertEqual(list(self.sch.task_list("FAILED", "").keys()), [task.task_id])
        self.assertTrue(self.sch.fetch_error(task.task_id)["error"])


class ForkBombTask(luigi.Task):
    depth = luigi.IntParameter()
    breadth = luigi.IntParameter()
    p = luigi.Parameter(default=(0,))  # ehm for some weird reason [0] becomes a tuple...?

    def output(self):
        return MockTarget(".".join(map(str, self.p)))

    def run(self):
        with self.output().open("w") as f:
            f.write("Done!")

    def requires(self):
        if len(self.p) < self.depth:
            for i in range(self.breadth):
                yield ForkBombTask(self.depth, self.breadth, self.p + (i,))


class TaskLimitTest(unittest.TestCase):
    def tearDown(self):
        MockFileSystem().remove("")

    @with_config({"worker": {"task_limit": "6"}})
    def test_task_limit_exceeded(self):
        w = Worker()
        t = ForkBombTask(3, 2)
        w.add(t)
        w.run()
        self.assertFalse(t.complete())
        leaf_tasks = [ForkBombTask(3, 2, branch) for branch in [(0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1)]]
        self.assertEqual(
            3,
            sum(t.complete() for t in leaf_tasks),
            "should have gracefully completed as much as possible even though the single last leaf didn't get scheduled",
        )

    @with_config({"worker": {"task_limit": "7"}})
    def test_task_limit_not_exceeded(self):
        w = Worker()
        t = ForkBombTask(3, 2)
        w.add(t)
        w.run()
        self.assertTrue(t.complete())

    def test_no_task_limit(self):
        w = Worker()
        t = ForkBombTask(4, 2)
        w.add(t)
        w.run()
        self.assertTrue(t.complete())


class WorkerConfigurationTest(unittest.TestCase):
    def test_asserts_for_worker(self):
        """
        Test that Worker() asserts that it's sanely configured
        """
        Worker(wait_interval=1)  # This shouldn't raise
        self.assertRaises(AssertionError, Worker, wait_interval=0)


class WorkerWaitJitterTest(unittest.TestCase):
    @with_config({"worker": {"wait_jitter": "10.0"}})
    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_wait_jitter(self, mock_sleep, mock_random):
        """verify configured jitter amount"""
        mock_random.return_value = 1.0

        w = Worker()
        x = w._sleeper()
        next(x)
        mock_random.assert_called_with(0, 10.0)
        mock_sleep.assert_called_with(2.0)

        mock_random.return_value = 2.0
        next(x)
        mock_random.assert_called_with(0, 10.0)
        mock_sleep.assert_called_with(3.0)

    @mock.patch("random.uniform")
    @mock.patch("time.sleep")
    def test_wait_jitter_default(self, mock_sleep, mock_random):
        """verify default jitter is as expected"""
        mock_random.return_value = 1.0
        w = Worker()
        x = w._sleeper()
        next(x)
        mock_random.assert_called_with(0, 5.0)
        mock_sleep.assert_called_with(2.0)

        mock_random.return_value = 3.3
        next(x)
        mock_random.assert_called_with(0, 5.0)
        mock_sleep.assert_called_with(4.3)


class KeyboardInterruptBehaviorTest(LuigiTestCase):
    def test_propagation_when_executing(self):
        """
        Ensure that keyboard interrupts causes luigi to quit when you are
        executing tasks.

        TODO: Add a test that tests the multiprocessing (--worker >1) case
        """

        class KeyboardInterruptTask(luigi.Task):
            def run(self):
                raise KeyboardInterrupt()

        cmd = "KeyboardInterruptTask --local-scheduler --no-lock".split(" ")
        self.assertRaises(KeyboardInterrupt, luigi_run, cmd)

    def test_propagation_when_scheduling(self):
        """
        Test that KeyboardInterrupt causes luigi to quit while scheduling.
        """

        class KeyboardInterruptTask(luigi.Task):
            def complete(self):
                raise KeyboardInterrupt()

        class ExternalKeyboardInterruptTask(luigi.ExternalTask):
            def complete(self):
                raise KeyboardInterrupt()

        self.assertRaises(KeyboardInterrupt, luigi_run, ["KeyboardInterruptTask", "--local-scheduler", "--no-lock"])
        self.assertRaises(KeyboardInterrupt, luigi_run, ["ExternalKeyboardInterruptTask", "--local-scheduler", "--no-lock"])


class WorkerPurgeEventHandlerTest(unittest.TestCase):
    @mock.patch("luigi.worker.ContextManagedTaskProcess")
    def test_process_killed_handler(self, task_proc):
        result = []

        @HangTheWorkerTask.event_handler(Event.PROCESS_FAILURE)
        def store_task(t, error_msg):
            self.assertTrue(error_msg)
            result.append(t)

        w = Worker()
        task = HangTheWorkerTask()
        task_process = mock.MagicMock(is_alive=lambda: False, exitcode=-14, task=task)
        task_proc.return_value = task_process

        w.add(task)
        w._run_task(task.task_id)
        w._handle_next_task()

        self.assertEqual(result, [task])

    @mock.patch("luigi.worker.time")
    def test_timeout_handler(self, mock_time):
        result = []

        @HangTheWorkerTask.event_handler(Event.TIMEOUT)
        def store_task(t, error_msg):
            self.assertTrue(error_msg)
            result.append(t)

        w = Worker(worker_processes=2, wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask(worker_timeout=1)
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 3
        w._handle_next_task()

        self.assertEqual(result, [task])

    @mock.patch("luigi.worker.time")
    def test_timeout_handler_single_worker(self, mock_time):
        result = []

        @HangTheWorkerTask.event_handler(Event.TIMEOUT)
        def store_task(t, error_msg):
            self.assertTrue(error_msg)
            result.append(t)

        w = Worker(wait_interval=0.01, timeout=5)
        mock_time.time.return_value = 0
        task = HangTheWorkerTask(worker_timeout=1)
        w.add(task)
        w._run_task(task.task_id)

        mock_time.time.return_value = 3
        w._handle_next_task()

        self.assertEqual(result, [task])


class PerTaskRetryPolicyBehaviorTest(LuigiTestCase):
    def setUp(self):
        super(PerTaskRetryPolicyBehaviorTest, self).setUp()
        self.per_task_retry_count = 3
        self.default_retry_count = 1
        self.sch = Scheduler(retry_delay=0.1, retry_count=self.default_retry_count, prune_on_get_work=True)

    def test_with_all_disabled_with_single_worker(self):
        """
        With this test, a case which has a task (TestWrapperTask), requires two another tasks (TestErrorTask1,TestErrorTask1) which both is failed, is
        tested.

        Task TestErrorTask1 has default retry_count which is 1, but Task TestErrorTask2 has retry_count at task level as 2.

        This test is running on single worker
        """

        class TestErrorTask1(DummyErrorTask):
            pass

        e1 = TestErrorTask1()

        class TestErrorTask2(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e2 = TestErrorTask2()

        class TestWrapperTask(luigi.WrapperTask):
            def requires(self):
                return [e2, e1]

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            self.assertTrue(w1.add(wt))

            self.assertFalse(w1.run())

            self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))

            self.assertEqual(sorted([e1.task_id, e2.task_id]), sorted(self.sch.task_list("DISABLED", "").keys()))

            self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
            self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e2.task_id).num_failures())
            self.assertEqual(self.default_retry_count, self.sch._state.get_task(e1.task_id).num_failures())

    def test_with_all_disabled_with_multiple_worker(self):
        """
        With this test, a case which has a task (TestWrapperTask), requires two another tasks (TestErrorTask1,TestErrorTask1) which both is failed, is
        tested.

        Task TestErrorTask1 has default retry_count which is 1, but Task TestErrorTask2 has retry_count at task level as 2.

        This test is running on multiple worker
        """

        class TestErrorTask1(DummyErrorTask):
            pass

        e1 = TestErrorTask1()

        class TestErrorTask2(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e2 = TestErrorTask2()

        class TestWrapperTask(luigi.WrapperTask):
            def requires(self):
                return [e2, e1]

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            with Worker(scheduler=self.sch, worker_id="Y", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w2:
                with Worker(scheduler=self.sch, worker_id="Z", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w3:
                    self.assertTrue(w1.add(wt))
                    self.assertTrue(w2.add(e2))
                    self.assertTrue(w3.add(e1))

                    self.assertFalse(w3.run())
                    self.assertFalse(w2.run())
                    self.assertTrue(w1.run())

                    self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))

                    self.assertEqual(sorted([e1.task_id, e2.task_id]), sorted(self.sch.task_list("DISABLED", "").keys()))

                    self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
                    self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e2.task_id).num_failures())
                    self.assertEqual(self.default_retry_count, self.sch._state.get_task(e1.task_id).num_failures())

    def test_with_includes_success_with_single_worker(self):
        """
        With this test, a case which has a task (TestWrapperTask), requires one (TestErrorTask1) FAILED and one (TestSuccessTask1) SUCCESS, is tested.

        Task TestSuccessTask1 will be DONE successfully, but Task TestErrorTask1 will be failed and it has retry_count at task level as 2.

        This test is running on single worker
        """

        class TestSuccessTask1(DummyTask):
            pass

        s1 = TestSuccessTask1()

        class TestErrorTask1(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e1 = TestErrorTask1()

        class TestWrapperTask(luigi.WrapperTask):
            def requires(self):
                return [e1, s1]

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            self.assertTrue(w1.add(wt))

            self.assertFalse(w1.run())

            self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))
            self.assertEqual([e1.task_id], list(self.sch.task_list("DISABLED", "").keys()))
            self.assertEqual([s1.task_id], list(self.sch.task_list("DONE", "").keys()))

            self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
            self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e1.task_id).num_failures())
            self.assertEqual(0, self.sch._state.get_task(s1.task_id).num_failures())

    def test_with_includes_success_with_multiple_worker(self):
        """
        With this test, a case which has a task (TestWrapperTask), requires one (TestErrorTask1) FAILED and one (TestSuccessTask1) SUCCESS, is tested.

        Task TestSuccessTask1 will be DONE successfully, but Task TestErrorTask1 will be failed and it has retry_count at task level as 2.

        This test is running on multiple worker
        """

        class TestSuccessTask1(DummyTask):
            pass

        s1 = TestSuccessTask1()

        class TestErrorTask1(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e1 = TestErrorTask1()

        class TestWrapperTask(luigi.WrapperTask):
            def requires(self):
                return [e1, s1]

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            with Worker(scheduler=self.sch, worker_id="Y", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w2:
                with Worker(scheduler=self.sch, worker_id="Z", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w3:
                    self.assertTrue(w1.add(wt))
                    self.assertTrue(w2.add(e1))
                    self.assertTrue(w3.add(s1))

                    self.assertTrue(w3.run())
                    self.assertFalse(w2.run())
                    self.assertTrue(w1.run())

                    self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))
                    self.assertEqual([e1.task_id], list(self.sch.task_list("DISABLED", "").keys()))
                    self.assertEqual([s1.task_id], list(self.sch.task_list("DONE", "").keys()))

                    self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
                    self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e1.task_id).num_failures())
                    self.assertEqual(0, self.sch._state.get_task(s1.task_id).num_failures())

    def test_with_dynamic_dependencies_with_single_worker(self):
        """
        With this test, a case includes dependency tasks(TestErrorTask1,TestErrorTask2) which both are failed.

        Task TestErrorTask1 has default retry_count which is 1, but Task TestErrorTask2 has retry_count at task level as 2.

        This test is running on single worker
        """

        class TestErrorTask1(DummyErrorTask):
            pass

        e1 = TestErrorTask1()

        class TestErrorTask2(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e2 = TestErrorTask2()

        class TestSuccessTask1(DummyTask):
            pass

        s1 = TestSuccessTask1()

        class TestWrapperTask(DummyTask):
            def requires(self):
                return [s1]

            def run(self):
                super(TestWrapperTask, self).run()
                yield e2, e1

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            self.assertTrue(w1.add(wt))

            self.assertFalse(w1.run())

            self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))

            self.assertEqual(sorted([e1.task_id, e2.task_id]), sorted(self.sch.task_list("DISABLED", "").keys()))

            self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
            self.assertEqual(0, self.sch._state.get_task(s1.task_id).num_failures())
            self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e2.task_id).num_failures())
            self.assertEqual(self.default_retry_count, self.sch._state.get_task(e1.task_id).num_failures())

    def test_with_dynamic_dependencies_with_multiple_workers(self):
        """
        With this test, a case includes dependency tasks(TestErrorTask1,TestErrorTask2) which both are failed.

        Task TestErrorTask1 has default retry_count which is 1, but Task TestErrorTask2 has retry_count at task level as 2.

        This test is running on multiple worker
        """

        class TestErrorTask1(DummyErrorTask):
            pass

        e1 = TestErrorTask1()

        class TestErrorTask2(DummyErrorTask):
            retry_count = self.per_task_retry_count

        e2 = TestErrorTask2()

        class TestSuccessTask1(DummyTask):
            pass

        s1 = TestSuccessTask1()

        class TestWrapperTask(DummyTask):
            def requires(self):
                return [s1]

            def run(self):
                super(TestWrapperTask, self).run()
                yield e2, e1

        wt = TestWrapperTask()

        with Worker(scheduler=self.sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w1:
            with Worker(scheduler=self.sch, worker_id="Y", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w2:
                self.assertTrue(w1.add(wt))
                self.assertTrue(w2.add(s1))

                self.assertTrue(w2.run())
                self.assertFalse(w1.run())

                self.assertEqual([wt.task_id], list(self.sch.task_list("PENDING", "UPSTREAM_DISABLED").keys()))

                self.assertEqual(sorted([e1.task_id, e2.task_id]), sorted(self.sch.task_list("DISABLED", "").keys()))

                self.assertEqual(0, self.sch._state.get_task(wt.task_id).num_failures())
                self.assertEqual(0, self.sch._state.get_task(s1.task_id).num_failures())
                self.assertEqual(self.per_task_retry_count, self.sch._state.get_task(e2.task_id).num_failures())
                self.assertEqual(self.default_retry_count, self.sch._state.get_task(e1.task_id).num_failures())

    def test_per_task_disable_persist_with_single_worker(self):
        """
        Ensure that `Task.disable_window` impacts the task retrying policy:
        - with the scheduler retry policy (disable_window=3), task fails twice and gets disabled
        - with the task retry policy (disable_window=0.5) task never gets into the DISABLED state
        """

        class TwoErrorsThenSuccessTask(Task):
            """
            The task is failing two times and then succeeds, waiting 1s before each try
            """

            retry_index = 0
            disable_window = None

            def run(self):
                time.sleep(1)
                self.retry_index += 1
                if self.retry_index < 3:
                    raise Exception("Retry index is %s for %s" % (self.retry_index, self.task_family))

        t = TwoErrorsThenSuccessTask()

        sch = Scheduler(retry_delay=0.1, retry_count=2, prune_on_get_work=True, disable_window=2)
        with Worker(scheduler=sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w:
            self.assertTrue(w.add(t))
            self.assertFalse(w.run())

            self.assertEqual(2, t.retry_index)
            self.assertEqual([t.task_id], list(sch.task_list("DISABLED").keys()))
            self.assertEqual(2, sch._state.get_task(t.task_id).num_failures())

        t = TwoErrorsThenSuccessTask()
        t.retry_index = 0
        t.disable_window = 0.5

        sch = Scheduler(retry_delay=0.1, retry_count=2, prune_on_get_work=True, disable_window=2)
        with Worker(scheduler=sch, worker_id="X", keep_alive=True, wait_interval=0.1, wait_jitter=0.05) as w:
            self.assertTrue(w.add(t))
            # Worker.run return False even if a task failed first but eventually succeeded.
            self.assertFalse(w.run())

            self.assertEqual(3, t.retry_index)
            self.assertEqual([t.task_id], list(sch.task_list("DONE").keys()))
            self.assertEqual(1, len(sch._state.get_task(t.task_id).failures))
