import os
import tempfile
import time
import unittest

import luigi
from luigi.scheduler import DONE, FAILED, PENDING, RUNNING, Scheduler
from luigi.worker import Worker


class WorkerDouble:
    """In-memory worker stand-in that keeps its attempt token like a real client."""

    def __init__(self, scheduler, worker_id, client_id):
        self.scheduler = scheduler
        self.worker_id = worker_id
        self.client_id = client_id
        self.attempts = {}

    def register(self, attempt_lease_support=True):
        return self.scheduler.add_worker(
            self.worker_id,
            [],
            client_id=self.client_id,
            attempt_lease_support=attempt_lease_support,
        )

    def schedule(self, task_id, **kwargs):
        return self.scheduler.add_task(worker=self.worker_id, task_id=task_id, **kwargs)

    def get_work(self):
        response = self.scheduler.get_work(worker=self.worker_id)
        task_id = response.get("task_id")
        attempt_id = response.get("attempt_id")
        if task_id is not None and attempt_id is not None:
            self.attempts[task_id] = attempt_id
        return response

    def report(self, task_id, status, attempt_id=None, **kwargs):
        if attempt_id is None:
            attempt_id = self.attempts.get(task_id)
        response = self.scheduler.add_task(
            worker=self.worker_id,
            task_id=task_id,
            status=status,
            attempt_id=attempt_id,
            **kwargs,
        )
        if status in (DONE, FAILED, PENDING):
            self.attempts.pop(task_id, None)
        return response

    def heartbeat(self):
        return self.scheduler.ping(
            worker=self.worker_id,
            client_id=self.client_id,
            attempt_leases=dict(self.attempts),
        )


class AttemptLeaseTest(unittest.TestCase):
    def make_scheduler(self, resources=None, **kwargs):
        config = {
            "retry_delay": 0,
            "remove_delay": 10000,
            "worker_disconnect_delay": 5,
            "stable_done_cooldown_secs": 0,
        }
        config.update(kwargs)
        return Scheduler(resources=resources or {"slots": 1}, **config)

    def test_stale_attempt_cannot_mutate_reassigned_task(self):
        sch = self.make_scheduler()
        old = WorkerDouble(sch, "worker-a", "client-a-1")
        old.register()
        old.schedule("base", status=DONE)
        old.schedule("T", deps=["base"], resources={"slots": 1}, family="T")

        first = old.get_work()
        first_attempt = first["attempt_id"]
        task = sch._state.get_task("T")
        self.assertEqual(task.status, RUNNING)
        self.assertEqual(sch._used_resources()["slots"], 1)

        # The first worker stops heartbeating and is pruned.
        sch._state.get_worker("worker-a").last_active = time.time() - 100
        sch.prune()
        self.assertIn(task.status, (FAILED, PENDING))
        self.assertNotEqual(task.status, RUNNING)

        # Retry delay is zero; make the retry eligible and let a new worker take it.
        if task.status == FAILED:
            task.retry = time.time() - 1
            sch.prune()
        self.assertEqual(task.status, PENDING)

        new = WorkerDouble(sch, "worker-b", "client-b-1")
        new.register()
        new.schedule("T", deps=["base"], resources={"slots": 1}, family="T")
        second = new.get_work()
        second_attempt = second["attempt_id"]
        self.assertNotEqual(first_attempt, second_attempt)
        self.assertEqual(task.status, RUNNING)
        self.assertEqual(task.worker_running, "worker-b")

        # The old worker reconnects under its original identity.
        old.register()

        failures_before = len(task.failures)
        heartbeat_before = task.attempt_heartbeat
        deps_before = set(task.deps)

        # Late DONE/FAILED reports and mutations from the old lease are diagnostics only.
        done = sch.add_task(worker="worker-a", task_id="T", status=DONE, attempt_id=first_attempt)
        failed = sch.add_task(worker="worker-a", task_id="T", status=FAILED, attempt_id=first_attempt)
        sch.decrease_running_task_resources("T", {"slots": 1}, attempt_id=first_attempt)
        sch.add_task(
            worker="worker-a",
            task_id="T",
            status=PENDING,
            new_deps=["late-dependency"],
            attempt_id=first_attempt,
        )
        sch.ping(worker="worker-a", client_id="client-a-1", attempt_leases={"T": first_attempt})

        self.assertFalse(done["accepted"])
        self.assertFalse(failed["accepted"])
        self.assertEqual(task.status, RUNNING)
        self.assertEqual(task.attempt_id, second_attempt)
        self.assertEqual(task.worker_running, "worker-b")
        self.assertEqual(len(task.failures), failures_before)
        self.assertEqual(task.attempt_heartbeat, heartbeat_before)
        self.assertEqual(task.resources_running["slots"], 1)
        self.assertEqual(sch._used_resources()["slots"], 1)
        self.assertEqual(task.deps, deps_before)

        diagnostics = sch.get_attempt_diagnostics("T")["events"]
        rejected_statuses = {event["status"] for event in diagnostics if event["action"] == "report_rejected"}
        self.assertIn(DONE, rejected_statuses)
        self.assertIn(FAILED, rejected_statuses)
        self.assertIn("resources", rejected_statuses)
        self.assertIn(PENDING, rejected_statuses)
        self.assertTrue(any(event["action"] == "heartbeat_rejected" for event in diagnostics))

        # A legitimate terminal report from the current lease is accepted once.
        completed = new.report("T", DONE)
        self.assertTrue(completed["accepted"])
        self.assertEqual(task.status, DONE)
        self.assertEqual(sch._used_resources()["slots"], 0)

        duplicate = new.report("T", DONE, attempt_id=second_attempt)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(task.status, DONE)
        self.assertEqual(len(task.failures), failures_before)

    def test_duplicate_failed_report_does_not_add_another_failure(self):
        sch = self.make_scheduler(retry_delay=100)
        worker = WorkerDouble(sch, "worker-a", "client-a-1")
        worker.register()
        worker.schedule("T")
        attempt_id = worker.get_work()["attempt_id"]

        first = worker.report("T", FAILED, attempt_id=attempt_id)
        task = sch._state.get_task("T")
        failures_after_first = len(task.failures)
        second = worker.report("T", FAILED, attempt_id=attempt_id)
        conflicting = worker.report("T", DONE, attempt_id=attempt_id)

        self.assertTrue(first["accepted"])
        self.assertTrue(second["duplicate"])
        self.assertFalse(conflicting["accepted"])
        self.assertEqual(task.status, FAILED)
        self.assertEqual(len(task.failures), failures_after_first)

    def test_duplicate_resource_adjustment_is_idempotent(self):
        sch = self.make_scheduler(resources={"slots": 2})
        worker = WorkerDouble(sch, "worker-a", "client-a-1")
        worker.register()
        worker.schedule("T", resources={"slots": 2})
        attempt_id = worker.get_work()["attempt_id"]

        sch.decrease_running_task_resources("T", {"slots": 1}, attempt_id=attempt_id)
        self.assertEqual(sch.get_running_task_resources("T")["resources"]["slots"], 1)
        sch.decrease_running_task_resources("T", {"slots": 1}, attempt_id=attempt_id)
        self.assertEqual(sch.get_running_task_resources("T")["resources"]["slots"], 1)

    def test_handshake_states_lease_support_explicitly(self):
        sch = self.make_scheduler()

        legacy = sch.add_worker("legacy", [], client_id="legacy-client")
        self.assertIsNone(legacy["attempt_lease_support"])
        self.assertEqual(legacy["attempt_lease_mode"], "legacy")

        explicit_legacy = sch.add_worker("legacy-2", [], client_id="legacy-client-2", attempt_lease_support=False)
        self.assertFalse(explicit_legacy["attempt_lease_support"])
        self.assertEqual(explicit_legacy["attempt_lease_mode"], "legacy")

        leased = sch.add_worker("leased", [], client_id="leased-client", attempt_lease_support=True)
        self.assertTrue(leased["attempt_lease_support"])
        self.assertEqual(leased["attempt_lease_mode"], "lease")
        self.assertTrue(leased["scheduler_generation"])

        sch.add_task(worker="legacy", task_id="legacy-task")
        legacy_work = sch.get_work(worker="legacy")
        self.assertNotIn("attempt_id", legacy_work)

        sch.add_task(worker="leased", task_id="leased-task")
        leased_work = sch.get_work(worker="leased")
        self.assertTrue(leased_work["attempt_id"])
        self.assertEqual(leased_work["attempt_lease_mode"], "lease")

    def test_reregistered_worker_gets_a_new_attempt(self):
        sch = self.make_scheduler(retry_delay=100)
        first = WorkerDouble(sch, "worker-a", "client-a-1")
        first.register()
        first.schedule("T", resources={"slots": 1})
        first_attempt = first.get_work()["attempt_id"]

        second = WorkerDouble(sch, "worker-a", "client-a-2")
        second.register()
        task = sch._state.get_task("T")
        self.assertEqual(task.status, PENDING)
        self.assertIsNone(task.attempt_id)

        late = sch.add_task(worker="worker-a", task_id="T", status=DONE, attempt_id=first_attempt)
        self.assertFalse(late["accepted"])
        self.assertEqual(task.status, PENDING)

        second_attempt = second.get_work()["attempt_id"]
        self.assertNotEqual(first_attempt, second_attempt)
        self.assertEqual(task.status, RUNNING)

    def test_scheduler_reload_does_not_reuse_persisted_attempt(self):
        sch = self.make_scheduler()
        old = WorkerDouble(sch, "worker-a", "client-a-1")
        old_generation = old.register()["scheduler_generation"]
        old.schedule("T", resources={"slots": 1})
        old_attempt = old.get_work()["attempt_id"]

        fd, state_path = tempfile.mkstemp()
        os.close(fd)
        try:
            sch._state._state_path = state_path
            sch.dump()

            reloaded = self.make_scheduler()
            reloaded._state._state_path = state_path
            reloaded.load()

            task = reloaded._state.get_task("T")
            self.assertEqual(task.status, PENDING)
            self.assertIsNone(task.attempt_id)
            self.assertIn(old_attempt, task.revoked_attempt_ids)

            handshake = reloaded.add_worker("worker-a", [], client_id="client-a-2", attempt_lease_support=True)
            self.assertNotEqual(old_generation, handshake["scheduler_generation"])

            late = reloaded.add_task(worker="worker-a", task_id="T", status=DONE, attempt_id=old_attempt)
            self.assertFalse(late["accepted"])
            self.assertEqual(task.status, PENDING)

            new = WorkerDouble(reloaded, "worker-a", "client-a-2")
            new_attempt = new.get_work()["attempt_id"]
            self.assertNotEqual(old_attempt, new_attempt)
            self.assertEqual(task.status, RUNNING)

            still_late = new.scheduler.add_task(worker="worker-a", task_id="T", status=DONE, attempt_id=old_attempt)
            self.assertFalse(still_late["accepted"])
            self.assertEqual(task.status, RUNNING)

            completed = new.report("T", DONE)
            self.assertTrue(completed["accepted"])
            self.assertEqual(task.status, DONE)
        finally:
            os.unlink(state_path)

    def test_normal_single_worker_flow_still_completes(self):
        sch = self.make_scheduler()

        class OneTask(luigi.Task):
            done = False

            def complete(self):
                return self.done

            def run(self):
                self.done = True

        task = OneTask()
        with Worker(scheduler=sch, worker_id="solo", wait_interval=0.01, wait_jitter=0) as worker:
            self.assertTrue(worker.add(task))
            self.assertTrue(worker.run())

        self.assertEqual(sch._state.get_task(task.task_id).status, DONE)
        self.assertEqual(sch._used_resources()["slots"], 0)


if __name__ == "__main__":
    unittest.main()
