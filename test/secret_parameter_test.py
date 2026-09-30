# -*- coding: utf-8 -*-
#
# Copyright 2026 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Tests for secret reference parameters (``luigi.SecretParameter``).

The plaintext tokens used here carry an obvious ``MARKED-SECRET-TOKEN``
marker so that scheduler state, logs, reprs, exceptions and RPC payloads
can be scanned for accidental leakage.
"""

import json
import logging
import os
import tempfile
import unittest

import luigi
import luigi.rpc
import luigi.scheduler
from helpers import LuigiTestCase
from luigi.parameter import ParameterException
from luigi.secret import (
    MappingSecretResolver,
    SecretRef,
    SecretResolutionError,
    SecretResolver,
    set_secret_resolver,
)
from luigi.worker import Worker

# Obviously-marked plaintext tokens. These must ONLY ever appear inside the
# actual task invocation (i.e. what run() does with the resolved value),
# never in task ids, scheduler state, logs, reprs, exceptions or RPC payloads.
MARKED_TOKEN = "MARKED-SECRET-TOKEN-4f9c2b7e1d8a"
MARKED_TOKEN_V2 = "MARKED-SECRET-TOKEN-v2-0b3c6a9f2e5d"


class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())

    def text(self):
        return "\n".join(self.messages)


class SecretRefTest(unittest.TestCase):
    def test_from_str_with_version(self):
        ref = SecretRef.from_str("db/token@v3")
        self.assertEqual(ref.name, "db/token")
        self.assertEqual(ref.version, "v3")

    def test_from_str_default_version(self):
        ref = SecretRef.from_str("db/token")
        self.assertEqual(ref.version, "latest")

    def test_from_str_invalid(self):
        for bad in ("", "@v1", "name@", 123, None):
            with self.assertRaises(ValueError):
                SecretRef.from_str(bad)

    def test_str_roundtrip(self):
        self.assertEqual(str(SecretRef.from_str("db/token@v3")), "db/token@v3")
        self.assertEqual(SecretRef.from_str(str(SecretRef("a", "1"))), SecretRef("a", "1"))

    def test_equality_and_hash(self):
        self.assertEqual(SecretRef("a", "1"), SecretRef("a", "1"))
        self.assertNotEqual(SecretRef("a", "1"), SecretRef("a", "2"))
        self.assertNotEqual(SecretRef("a", "1"), SecretRef("b", "1"))
        self.assertEqual(hash(SecretRef("a", "1")), hash(SecretRef("a", "1")))
        self.assertIn(SecretRef("a", "1"), {SecretRef("a", "1")})

    def test_repr_shows_only_reference(self):
        ref = SecretRef("db/token", "v3")
        self.assertIn("db/token", repr(ref))
        self.assertIn("v3", repr(ref))
        self.assertNotIn(MARKED_TOKEN, repr(ref))


class SecretParameterIdentityTest(LuigiTestCase):
    def _task_cls(self):
        class SecretIdentityTask(luigi.Task):
            credential = luigi.SecretParameter()

        return SecretIdentityTask

    def test_attribute_is_reference_not_plaintext(self):
        task = self._task_cls()(credential="db/token@v1")
        self.assertIsInstance(task.credential, SecretRef)
        self.assertEqual(task.credential, SecretRef("db/token", "v1"))

    def test_accepts_secret_ref_instance(self):
        task = self._task_cls()(credential=SecretRef("db/token", "v9"))
        self.assertEqual(task.credential.version, "v9")

    def test_invalid_value_raises(self):
        with self.assertRaises(ParameterException):
            self._task_cls()(credential=123)

    def test_task_id_distinguishes_reference_and_version(self):
        cls = self._task_cls()
        base = cls(credential="db/token@v1")
        other_version = cls(credential="db/token@v2")
        other_name = cls(credential="db/other@v1")
        same = cls(credential="db/token@v1")

        self.assertNotEqual(base.task_id, other_version.task_id)
        self.assertNotEqual(base.task_id, other_name.task_id)
        self.assertEqual(base.task_id, same.task_id)
        # tasks are only equal when name AND version match: no wrong merging
        self.assertNotEqual(base, other_version)
        self.assertNotEqual(base, other_name)
        self.assertEqual(base, same)

    def test_manifest_carries_only_reference_and_version(self):
        task = self._task_cls()(credential="db/token@v1")
        manifest = task.to_str_params()
        self.assertEqual(manifest["credential"], "db/token@v1")
        self.assertNotIn(MARKED_TOKEN, json.dumps(manifest))

    def test_repr_and_str_carry_only_reference(self):
        task = self._task_cls()(credential="db/token@v1")
        self.assertIn("db/token@v1", repr(task))
        self.assertNotIn(MARKED_TOKEN, repr(task))
        self.assertNotIn(MARKED_TOKEN, str(task))

    def test_from_str_params_roundtrip(self):
        cls = self._task_cls()
        task = cls(credential="db/token@v1")
        clone = cls.from_str_params(task.to_str_params())
        self.assertEqual(clone, task)
        self.assertEqual(clone.credential, SecretRef("db/token", "v1"))

    def test_clone_preserves_reference(self):
        task = self._task_cls()(credential="db/token@v1")
        self.assertEqual(task.clone().credential, SecretRef("db/token", "v1"))


class CountingResolver(SecretResolver):
    def __init__(self, secrets):
        self.secrets = dict(secrets)
        self.calls = []

    def resolve(self, ref):
        self.calls.append((ref.name, ref.version))
        key = (ref.name, ref.version)
        if key not in self.secrets:
            raise SecretResolutionError("unknown secret reference '%s'" % ref)
        return self.secrets[key]


class SecretResolutionTest(unittest.TestCase):
    def tearDown(self):
        set_secret_resolver(None)

    def test_resolve_with_explicit_resolver(self):
        ref = SecretRef("db/token", "v1")
        resolver = MappingSecretResolver({("db/token", "v1"): MARKED_TOKEN})
        self.assertEqual(ref.resolve(resolver), MARKED_TOKEN)

    def test_resolve_with_registered_resolver(self):
        set_secret_resolver(MappingSecretResolver({("db/token", "v1"): MARKED_TOKEN}))
        self.assertEqual(SecretRef("db/token", "v1").resolve(), MARKED_TOKEN)

    def test_mapping_resolver_name_fallback_and_missing(self):
        resolver = MappingSecretResolver({"db/token": MARKED_TOKEN})
        self.assertEqual(SecretRef("db/token", "any-version").resolve(resolver), MARKED_TOKEN)
        with self.assertRaises(SecretResolutionError):
            SecretRef("db/missing", "v1").resolve(resolver)

    def test_resolve_without_resolver_fails(self):
        set_secret_resolver(None)
        with self.assertRaises(SecretResolutionError) as ctx:
            SecretRef("db/token", "v1").resolve()
        # the error may name the reference, but never any plaintext
        self.assertIn("db/token", str(ctx.exception))
        self.assertNotIn(MARKED_TOKEN, str(ctx.exception))

    def test_resolver_exception_is_wrapped_without_plaintext(self):
        class BoomResolver(SecretResolver):
            def resolve(self, ref):
                raise RuntimeError("backend exploded")

        with self.assertRaises(SecretResolutionError) as ctx:
            SecretRef("db/token", "v1").resolve(BoomResolver())
        self.assertIn("db/token", str(ctx.exception))
        self.assertNotIn(MARKED_TOKEN, str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, RuntimeError)

    def test_resolve_fetches_every_time(self):
        resolver = CountingResolver({("db/token", "v1"): MARKED_TOKEN})
        ref = SecretRef("db/token", "v1")
        self.assertEqual(ref.resolve(resolver), MARKED_TOKEN)
        # a retry re-fetches: update the backend value and resolve again
        resolver.secrets[("db/token", "v1")] = MARKED_TOKEN_V2
        self.assertEqual(ref.resolve(resolver), MARKED_TOKEN_V2)
        self.assertEqual(len(resolver.calls), 2)

    def test_resolved_value_is_not_stored_on_ref(self):
        ref = SecretRef("db/token", "v1")
        ref.resolve(MappingSecretResolver({("db/token", "v1"): MARKED_TOKEN}))
        self.assertNotIn(MARKED_TOKEN, repr(ref))
        self.assertNotIn(MARKED_TOKEN, str(ref))
        self.assertEqual(ref, SecretRef("db/token", "v1"))

    def test_set_secret_resolver_type_check(self):
        with self.assertRaises(TypeError):
            set_secret_resolver("not-a-resolver")


class SecretWriterTask(luigi.Task):
    """Writes the resolved secret to its output, like a real task would use a token."""

    credential = luigi.SecretParameter()
    out_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_dir, "secret-%s.txt" % self.credential.version))

    def run(self):
        token = self.credential.resolve()
        with self.output().open("w") as f:
            f.write(token)


class SecretWorkerExecutionTest(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.sch = luigi.scheduler.Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.log_handler = ListHandler()
        root = logging.getLogger()
        self._old_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(self.log_handler)

    def tearDown(self):
        root = logging.getLogger()
        root.removeHandler(self.log_handler)
        root.setLevel(self._old_level)
        set_secret_resolver(None)
        super().tearDown()

    def _scheduler_state_text(self):
        """Everything an operator (or the web UI / error pages) could observe."""
        parts = [json.dumps(self.sch.task_list("", ""))]
        for task in self.sch._state.get_active_tasks():
            parts.append(task.id)
            parts.append(json.dumps(task.params))
            parts.append(json.dumps(task.public_params))
            parts.append(json.dumps(task.hidden_params))
            parts.append(repr(task))
            parts.append(str(task.expl))
            parts.append(str(task.status_message))
        return "\n".join(parts)

    def _run_task(self, task, scheduler=None):
        sch = scheduler or self.sch
        with Worker(scheduler=sch, worker_id="secret-worker") as w:
            added = w.add(task)
            ran = w.run()
        return added, ran

    def test_plaintext_only_available_at_invocation(self):
        set_secret_resolver(MappingSecretResolver({("db/token", "v1"): MARKED_TOKEN}))
        task = SecretWriterTask(credential="db/token@v1", out_dir=self.tmpdir)

        added, ran = self._run_task(task)
        self.assertTrue(added)
        self.assertTrue(ran)

        # the plaintext was available to the actual task invocation
        with open(task.output().path) as f:
            self.assertEqual(f.read(), MARKED_TOKEN)

        # ... but nowhere else: scheduler state, task ids, error pages, logs
        observable = self._scheduler_state_text() + "\n" + self.log_handler.text()
        self.assertNotIn(MARKED_TOKEN, observable)
        # the reference (with version) is what identifies the task everywhere
        self.assertIn("db/token@v1", task.to_str_params()["credential"])
        self.assertIn("db_token_v1", task.task_id.replace("@", "_"))

    def test_different_secret_tasks_are_not_merged(self):
        set_secret_resolver(
            MappingSecretResolver(
                {
                    ("db/token", "v1"): MARKED_TOKEN,
                    ("db/token", "v2"): MARKED_TOKEN_V2,
                }
            )
        )
        task_v1 = SecretWriterTask(credential="db/token@v1", out_dir=self.tmpdir)
        task_v2 = SecretWriterTask(credential="db/token@v2", out_dir=self.tmpdir)
        self.assertNotEqual(task_v1.task_id, task_v2.task_id)

        with Worker(scheduler=self.sch, worker_id="secret-worker") as w:
            self.assertTrue(w.add(task_v1))
            self.assertTrue(w.add(task_v2))
            self.assertTrue(w.run())

        # both distinct tasks ran with their own secret version
        with open(task_v1.output().path) as f:
            self.assertEqual(f.read(), MARKED_TOKEN)
        with open(task_v2.output().path) as f:
            self.assertEqual(f.read(), MARKED_TOKEN_V2)

        done = self.sch.task_list("DONE", "")
        done_ids = json.dumps(done)
        self.assertIn(task_v1.task_id, done_ids)
        self.assertIn(task_v2.task_id, done_ids)
        self.assertNotIn(MARKED_TOKEN, done_ids)
        self.assertNotIn(MARKED_TOKEN_V2, done_ids)

    def test_resolver_failure_blocks_task_and_retry_refetches(self):
        resolver = CountingResolver({("db/token", "v1"): MARKED_TOKEN})
        # no resolver registered: resolution must fail and the task must not start
        set_secret_resolver(None)
        task = SecretWriterTask(credential="db/token@v1", out_dir=self.tmpdir)

        added, ran = self._run_task(task)
        self.assertTrue(added)
        self.assertFalse(ran)
        self.assertFalse(os.path.exists(task.output().path))
        self.assertEqual(resolver.calls, [])

        failed = self.sch.task_list("FAILED", "")
        self.assertIn(task.task_id, json.dumps(failed))

        # the error page carries the reference, never plaintext
        error = self.sch.fetch_error(task.task_id)
        self.assertIn("db/token", json.dumps(error))
        self.assertNotIn(MARKED_TOKEN, json.dumps(error))
        self.assertNotIn(MARKED_TOKEN, self.log_handler.text())

        # retry with a working resolver: the secret is fetched again, task succeeds
        set_secret_resolver(resolver)
        sch2 = luigi.scheduler.Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        added, ran = self._run_task(task, scheduler=sch2)
        self.assertTrue(added)
        self.assertTrue(ran)
        self.assertEqual(resolver.calls, [("db/token", "v1")])
        with open(task.output().path) as f:
            self.assertEqual(f.read(), MARKED_TOKEN)

        self.assertNotIn(MARKED_TOKEN, self.log_handler.text())


class SecretRpcWriterTask(luigi.Task):
    credential = luigi.SecretParameter()
    out_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.out_dir, "rpc-secret.txt"))

    def run(self):
        token = self.credential.resolve()
        with self.output().open("w") as f:
            f.write(token)


class InProcessRpcTransport:
    """
    Routes :class:`~luigi.rpc.RemoteScheduler` calls to an in-process
    central scheduler, JSON round-tripping every payload exactly like the
    real HTTP RPC layer does. Lets us scan the precise bytes that cross
    the wire without binding a socket.
    """

    def __init__(self):
        self.scheduler = luigi.scheduler.Scheduler(retry_delay=100, remove_delay=1000, worker_disconnect_delay=10)
        self.remote = luigi.rpc.RemoteScheduler("http://in-process-rpc/")
        self.sent_payloads = []
        self.remote._fetch = self._fetch

    def _fetch(self, url_suffix, body):
        self.sent_payloads.append(body["data"])
        method = url_suffix.rsplit("/", 1)[-1]
        result = getattr(self.scheduler, method)(**json.loads(body["data"]))
        return json.dumps({"response": result})

    def wire_text(self):
        return "\n".join(self.sent_payloads)


class SecretRemoteSchedulerTest(LuigiTestCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = tempfile.mkdtemp()
        self.transport = InProcessRpcTransport()

    def tearDown(self):
        set_secret_resolver(None)
        super().tearDown()

    def _remote_state_text(self, remote, task_id=None):
        parts = [
            json.dumps(remote.task_list("", "")),
            json.dumps(remote.task_list("DONE", "")),
            json.dumps(remote.task_list("FAILED", "")),
            json.dumps(remote.task_search("SecretRpcWriterTask")),
        ]
        if task_id is not None:
            parts.append(json.dumps(remote.fetch_error(task_id)))
        return "\n".join(parts)

    def _build_over_rpc(self, task):
        with Worker(scheduler=self.transport.remote, worker_id="secret-rpc-worker") as w:
            added = w.add(task)
            ran = w.run()
        return added, ran

    def test_no_plaintext_in_scheduler_rpc(self):
        set_secret_resolver(MappingSecretResolver({("db/token", "v1"): MARKED_TOKEN}))
        task = SecretRpcWriterTask(credential="db/token@v1", out_dir=self.tmpdir)

        added, ran = self._build_over_rpc(task)
        self.assertTrue(added)
        self.assertTrue(ran)

        # plaintext reached the actual invocation
        with open(task.output().path) as f:
            self.assertEqual(f.read(), MARKED_TOKEN)

        # nothing that crossed the wire contains the plaintext
        self.assertNotIn(MARKED_TOKEN, self.transport.wire_text())
        self.assertIn("db/token@v1", self.transport.wire_text())

        # nothing observable from the scheduler over RPC contains it either
        remote_state = self._remote_state_text(self.transport.remote)
        self.assertNotIn(MARKED_TOKEN, remote_state)
        self.assertIn("db/token@v1", remote_state)

    def test_no_plaintext_in_rpc_error_page(self):
        # no resolver registered on the worker: the task cannot start
        set_secret_resolver(None)
        task = SecretRpcWriterTask(credential="db/token@v1", out_dir=self.tmpdir)

        added, ran = self._build_over_rpc(task)
        self.assertTrue(added)
        self.assertFalse(ran)
        self.assertFalse(os.path.exists(task.output().path))

        self.assertNotIn(MARKED_TOKEN, self.transport.wire_text())

        remote_state = self._remote_state_text(self.transport.remote, task_id=task.task_id)
        self.assertNotIn(MARKED_TOKEN, remote_state)
        self.assertIn("db/token", remote_state)
