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

"""Tests for :class:`luigi.SecretParameter` and secret reference resolution.

The tests use a highly visible marker token and scan scheduler state,
failure explanations, task reprs and captured logs to prove the plaintext
only ever exists while a task is actually running.
"""

import io
import json
import logging

from helpers import LuigiTestCase

import luigi
import luigi.worker
from luigi.scheduler import Scheduler
from luigi.secrets import (
    DEFAULT_RESOLVER_NAME,
    ResolvedSecret,
    SecretReference,
    SecretReferenceParseException,
    SecretResolutionError,
    SecretResolver,
)
from luigi.worker import Worker

# Deliberately obvious so an accidental leak is easy to grep for.
MARKER_TOKEN = "SECRET-TEST-TOKEN-DO-NOT-LOG-7Q4X9Z2"


class DictSecretResolver(SecretResolver):
    """In-memory resolver; optionally fails the first *fail_times* calls."""

    def __init__(self, values=None, fail_times=0, leak_in_errors=True, tainted_secret_error=False):
        self._values = dict(values or {})
        self.fail_times = fail_times
        self.calls = []
        self.leak_in_errors = leak_in_errors
        self.tainted_secret_error = tainted_secret_error

    def resolve(self, reference):
        self.calls.append((reference.name, reference.version))
        if len(self.calls) <= self.fail_times:
            if self.tainted_secret_error:
                # A SecretResolutionError whose message/reason was poisoned
                # with the plaintext: the worker must rebuild it.
                error = SecretResolutionError.__new__(SecretResolutionError)
                Exception.__init__(error, "resolver internals " + MARKER_TOKEN)
                error.reference = reference
                error.reason = MARKER_TOKEN
                raise error
            if self.leak_in_errors:
                # A hostile resolver that includes the would-be value in the
                # error message: Luigi must not propagate it.
                raise RuntimeError("vault said no: " + MARKER_TOKEN)
            raise SecretResolutionError(reference, "vault unavailable")
        if (reference.name, reference.version) in self._values:
            return self._values[(reference.name, reference.version)]
        raise SecretResolutionError(reference, "unknown secret")


class _LogCapture:
    def __init__(self):
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.logger = logging.getLogger("luigi-interface")

    def __enter__(self):
        self.logger.addHandler(self.handler)
        self._old_level = self.logger.level
        self.logger.setLevel(logging.DEBUG)
        return self

    def __exit__(self, *exc):
        self.logger.removeHandler(self.handler)
        self.logger.setLevel(self._old_level)

    @property
    def text(self):
        return self.stream.getvalue()


def make_secret_task():
    """Return a fresh task class whose run-state cannot leak between tests."""

    class _SecretTask(luigi.Task):
        token = luigi.SecretParameter()
        ran = False
        observed = None

        def complete(self):
            return type(self).ran

        def run(self):
            type(self).observed = str(self.token)
            type(self).ran = True

    return _SecretTask


class SecretReferenceTest(LuigiTestCase):
    def test_parse_and_serialize(self):
        ref = SecretReference.parse("api-token@v3")
        self.assertEqual((ref.name, ref.version), ("api-token", "v3"))
        self.assertEqual(str(ref), "api-token@v3")
        self.assertEqual(ref.serialize(), "api-token@v3")
        self.assertEqual(repr(ref), "SecretReference(api-token@v3)")

    def test_roundtrip_through_mapping(self):
        self.assertEqual(SecretReference.parse({"name": "n", "version": 1}), SecretReference("n", "1"))

    def test_invalid_references_rejected(self):
        for bad in ("", None, 123, ["a@v1"], {"name": "n"}, {"version": "v1"}):
            with self.assertRaises(SecretReferenceParseException):
                SecretReference.parse(bad)
        with self.assertRaises(SecretReferenceParseException):
            SecretReference("name@bad", "v1")

    def test_raw_value_rejected(self):
        with self.assertRaises(SecretReferenceParseException):
            SecretReference.parse(MARKER_TOKEN)
        with self.assertRaises(SecretReferenceParseException):
            SecretReference.parse("missing-version")

    def test_reference_is_identity(self):
        ref = SecretReference("api-token", "v1")
        resolved = ResolvedSecret(ref, MARKER_TOKEN)
        self.assertEqual(resolved, ref)
        self.assertEqual(hash(resolved), hash(ref))
        # str() exposes the plaintext for task code, repr/serialize don't.
        self.assertEqual(str(resolved), MARKER_TOKEN)
        self.assertNotIn(MARKER_TOKEN, repr(resolved))
        self.assertNotIn(MARKER_TOKEN, resolved.serialize())
        self.assertEqual(resolved.serialize(), "api-token@v1")

    def test_resolved_secret_requires_string_value(self):
        with self.assertRaises(luigi.secrets.SecretException):
            ResolvedSecret(SecretReference("a", "v1"), 123)
        self.assertNotEqual(SecretReference("a", "v1"), object())


class SecretParameterTest(LuigiTestCase):
    def test_repr_and_str_params_only_carry_reference(self):
        class T(luigi.Task):
            token = luigi.SecretParameter()

        task = T(token="api-token@v1")
        self.assertNotIn(MARKER_TOKEN, repr(task))
        self.assertEqual(repr(task), "T(token=api-token@v1)")
        self.assertEqual(task.to_str_params(), {"token": "api-token@v1"})
        self.assertEqual(task.to_str_params(only_significant=True), {"token": "api-token@v1"})

    def test_version_participates_in_task_identity(self):
        class T(luigi.Task):
            token = luigi.SecretParameter()

        v1 = T(token="api-token@v1")
        v2 = T(token="api-token@v2")
        self.assertNotEqual(v1.task_id, v2.task_id)
        self.assertEqual(v1, T(token="api-token@v1"))
        # The reference (with invalid chars normalized) is visible in the id.
        self.assertIn("api_token_v1", v1.task_id)
        self.assertIn("api_token_v2", v2.task_id)

    def test_from_str_params_round_trip(self):
        class T(luigi.Task):
            token = luigi.SecretParameter()

        task = T(token="api-token@v1")
        rebuilt = T.from_str_params({"token": "api-token@v1"})
        self.assertEqual(rebuilt.task_id, task.task_id)
        self.assertIsInstance(rebuilt.token, SecretReference)

    def test_must_be_significant_and_public(self):
        from luigi.parameter import ParameterException, ParameterVisibility

        with self.assertRaises(ParameterException):
            luigi.SecretParameter(significant=False)
        with self.assertRaises(ParameterException):
            luigi.SecretParameter(visibility=ParameterVisibility.HIDDEN)


class SecretWorkerTest(LuigiTestCase):
    def _build_worker(self, scheduler, **kwargs):
        kwargs.setdefault("wait_interval", 0.01)
        kwargs.setdefault("wait_jitter", 0.0)
        return Worker(scheduler=scheduler, **kwargs)

    def _scheduler(self, retry_count=1):
        return Scheduler(retry_delay=0, retry_count=retry_count, prune_on_get_work=True)

    def test_plaintext_available_only_during_run(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver({("svc", "v1"): MARKER_TOKEN})
        scheduler = self._scheduler()
        task = task_cls(token="svc@v1")

        with _LogCapture() as logs:
            with self._build_worker(scheduler, secret_resolver=resolver) as worker:
                worker.add(task)
                ok = worker.run()

        self.assertTrue(ok)
        self.assertEqual(task_cls.observed, MARKER_TOKEN)
        # After the run the task attribute holds the reference again.
        self.assertEqual(task.token, SecretReference("svc", "v1"))
        self.assertNotIn(MARKER_TOKEN, repr(task))
        self.assertNotIn(MARKER_TOKEN, logs.text)
        self.assertEqual(resolver.calls, [("svc", "v1")])

    def test_scheduler_state_and_rpc_payload_only_have_reference(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver({("svc", "v1"): MARKER_TOKEN})
        scheduler = self._scheduler()
        task = task_cls(token="svc@v1")

        with _LogCapture():
            with self._build_worker(scheduler, secret_resolver=resolver) as worker:
                worker.add(task)
                worker.run()

        graph = scheduler.dep_graph(task_id=task.task_id)
        entry = graph[task.task_id]
        serialized = json.dumps(entry)
        self.assertEqual(entry["params"], {"token": "svc@v1"})
        self.assertNotIn(MARKER_TOKEN, serialized)
        self.assertIn("svc@v1", serialized)

        stored = scheduler._state.get_task(task.task_id)
        self.assertNotIn(MARKER_TOKEN, str(stored.params))
        self.assertNotIn(MARKER_TOKEN, str(stored.expl))
        self.assertNotIn(MARKER_TOKEN, repr(stored))

    def test_resolver_failure_prevents_start_and_is_safe(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver(fail_times=10 ** 6)
        scheduler = self._scheduler(retry_count=2)
        task = task_cls(token="svc-broken@v1")

        with _LogCapture() as logs:
            with self._build_worker(scheduler, secret_resolver=resolver) as worker:
                worker.add(task)
                ok = worker.run()

        self.assertFalse(ok)
        self.assertFalse(task_cls.ran, "run() must not execute when the secret cannot be resolved")
        self.assertGreaterEqual(len(resolver.calls), 1)
        stored = scheduler._state.get_task(task.task_id)
        self.assertIn("svc-broken@v1", stored.expl)
        self.assertNotIn(MARKER_TOKEN, stored.expl)
        self.assertNotIn(MARKER_TOKEN, logs.text)

    def test_tainted_secret_error_message_is_stripped(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver(fail_times=10 ** 6, tainted_secret_error=True)
        scheduler = self._scheduler(retry_count=1)
        task = task_cls(token="svc-broken@v1")

        with _LogCapture() as logs:
            with self._build_worker(scheduler, secret_resolver=resolver) as worker:
                worker.add(task)
                worker.run()

        expl = scheduler._state.get_task(task.task_id).expl
        self.assertIn("svc-broken@v1", expl)
        self.assertNotIn(MARKER_TOKEN, expl)
        self.assertNotIn(MARKER_TOKEN, logs.text)

    def test_retry_re_resolves_the_secret(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver({("svc", "v1"): MARKER_TOKEN}, fail_times=1, leak_in_errors=False)
        scheduler = self._scheduler(retry_count=3)
        task = task_cls(token="svc@v1")

        with self._build_worker(scheduler, secret_resolver=resolver) as worker:
            worker.add(task)
            worker.run()

        self.assertTrue(task_cls.ran)
        self.assertEqual(len(resolver.calls), 2)
        self.assertEqual(task_cls.observed, MARKER_TOKEN)

    def test_distinct_secret_failures_are_not_merged(self):
        class TwoSecretTask(luigi.Task):
            token = luigi.SecretParameter()

            def complete(self):
                return False

            def run(self):
                raise AssertionError("should never run")

        scheduler = self._scheduler(retry_count=1)
        with self._build_worker(scheduler, secret_resolver=DictSecretResolver(fail_times=10 ** 6)) as worker:
            a = TwoSecretTask(token="svc-a@v1")
            b = TwoSecretTask(token="svc-b@v1")
            worker.add(a)
            worker.add(b)
            worker.run()

        expl_a = scheduler._state.get_task(a.task_id).expl
        expl_b = scheduler._state.get_task(b.task_id).expl
        self.assertIn("svc-a@v1", expl_a)
        self.assertIn("svc-b@v1", expl_b)
        self.assertNotIn("svc-b@v1", expl_a)
        self.assertNotIn("svc-a@v1", expl_b)

    def test_no_resolver_configured_is_reference_only_failure(self):
        scheduler = self._scheduler(retry_count=1)
        task_cls = make_secret_task()
        task = task_cls(token="svc-nope@v9")
        with self._build_worker(scheduler) as worker:
            worker.add(task)
            ok = worker.run()
        self.assertFalse(ok)
        self.assertFalse(task_cls.ran)
        expl = scheduler._state.get_task(task.task_id).expl
        self.assertIn("svc-nope@v9", expl)
        self.assertIn("no secret resolver", expl)

    def test_named_and_parameter_level_resolvers(self):
        class NamedTask(luigi.Task):
            token = luigi.SecretParameter(resolver_name="vault")

            seen = None

            def complete(self):
                return NamedTask.seen is not None

            def run(self):
                NamedTask.seen = str(self.token)

        class DirectTask(luigi.Task):
            token = luigi.SecretParameter(resolver=DictSecretResolver({("d", "v1"): "DIRECT"}))

            seen = None

            def complete(self):
                return DirectTask.seen is not None

            def run(self):
                DirectTask.seen = str(self.token)

        scheduler = Scheduler(prune_on_get_work=True)
        named = NamedTask(token="n@v1")
        direct = DirectTask(token="d@v1")
        resolvers = {
            "vault": DictSecretResolver({("n", "v1"): "NAMED"}),
            DEFAULT_RESOLVER_NAME: DictSecretResolver({("x", "v1"): "DEFAULT"}),
        }
        with self._build_worker(scheduler, secret_resolvers=resolvers) as worker:
            worker.add(named)
            worker.add(direct)
            worker.run()

        self.assertEqual(NamedTask.seen, "NAMED")
        self.assertEqual(DirectTask.seen, "DIRECT")

    def test_build_accepts_secret_resolver(self):
        task_cls = make_secret_task()
        resolver = DictSecretResolver({("svc", "v1"): MARKER_TOKEN})
        task = task_cls(token="svc@v1")
        result = luigi.build(
            [task],
            local_scheduler=True,
            no_lock=True,
            secret_resolver=resolver,
            log_level="WARNING",
        )
        self.assertTrue(result)
        self.assertEqual(task_cls.observed, MARKER_TOKEN)

    def test_plaintext_unavailable_during_scheduling(self):
        class TaskWithProbe(luigi.Task):
            token = luigi.SecretParameter()
            seen_in_complete = None

            def complete(self):
                # complete() runs while the task is being scheduled, before
                # the resolver is ever consulted.
                TaskWithProbe.seen_in_complete = str(self.token)
                return TaskWithProbe.seen_in_complete == MARKER_TOKEN

            def run(self):
                self.task_ran = str(self.token)

        scheduler = self._scheduler()
        task = TaskWithProbe(token="svc@v1")
        with self._build_worker(scheduler, secret_resolver=DictSecretResolver({("svc", "v1"): MARKER_TOKEN})) as worker:
            worker.add(task)
            worker.run()

        # During complete() the plaintext must not be available; otherwise the
        # task would have been considered complete without ever running.
        self.assertEqual(TaskWithProbe.seen_in_complete, "svc@v1")
        self.assertTrue(hasattr(task, "task_ran"))


class SecretParameterSchedulerRpcTest(LuigiTestCase):
    """Exercise the scheduler-RPC boundary without needing a real socket.

    The worker talks to a scheduler through ``add_task``/``get_work``; we spy
    on every payload handed to a scheduler and assert only the reference (and
    never the marker token) crosses that boundary.
    """

    def test_marker_never_crosses_scheduler_boundary(self):
        class SpyingScheduler(Scheduler):
            def __init__(self):
                super().__init__(retry_delay=0, retry_count=1, prune_on_get_work=True)
                self.calls = []

            def add_task(self, *args, **kwargs):
                self.calls.append(kwargs)
                return super().add_task(*args, **kwargs)

            def get_work(self, *args, **kwargs):
                response = super().get_work(*args, **kwargs)
                self.calls.append({"get_work": response})
                return response

        task_cls = make_secret_task()
        scheduler = SpyingScheduler()
        resolver = DictSecretResolver({("svc", "v1"): MARKER_TOKEN})
        task = task_cls(token="svc@v1")

        with Worker(scheduler=scheduler, secret_resolver=resolver, wait_interval=0.01, wait_jitter=0.0) as worker:
            worker.add(task)
            worker.run()

        self.assertEqual(task_cls.observed, MARKER_TOKEN)
        wire = json.dumps(scheduler.calls, default=str, sort_keys=True)
        self.assertIn("svc@v1", wire)
        self.assertNotIn(MARKER_TOKEN, wire)

        # The failure/error view served by the scheduler stays reference-only.
        error_view = json.dumps(scheduler.fetch_error(task_id=task.task_id), default=str)
        self.assertNotIn(MARKER_TOKEN, error_view)
