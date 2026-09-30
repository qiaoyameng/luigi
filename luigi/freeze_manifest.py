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
"""
Freeze manifest API: audit a Luigi task graph before running it, then
execute against the audited graph.

The typical production workflow is:

1. Build the graph once and record it with :py:func:`freeze_graph`. The
   resulting :py:class:`TaskGraphManifest` stores, per task node, the task
   family, the normalized (canonicalized) serialized parameters and a
   description of the declared output targets, plus the static dependency
   edges between nodes.
2. Persist the manifest (it is plain JSON) and compute its stable
   :py:meth:`TaskGraphManifest.digest` for review/sign-off. The digest does
   not depend on dict ordering inside parameters, on the traversal order of
   the graph, or on how shared dependencies are reached.
3. At execution time, restore the tasks with :py:func:`restore_graph`. Task
   classes are resolved exclusively through the already-imported task
   registry, optionally restricted to an explicit whitelist of task
   families. The manifest is data only: it can never trigger imports of
   modules it names.
4. Re-check the restored graph with :py:func:`verify_graph`. If the task
   implementations drifted since the manifest was approved (e.g. a
   ``requires()`` now yields an extra dependency), a
   :py:exc:`DependencyDriftError` is raised instead of silently running the
   changed graph.

Tasks whose dependencies are only known at run time (dynamic requirements
yielded from ``run()``) cannot be fully frozen. They must be declared via
the ``dynamic_tasks`` argument of :py:func:`freeze_graph`; the manifest then
records them in :py:attr:`TaskGraphManifest.incomplete` and
:py:attr:`TaskGraphManifest.is_fully_frozen` is ``False``. Such a manifest
never claims the whole graph is determined, and :py:func:`verify_graph`
refuses to vouch for it unless explicitly told otherwise.
"""

import hashlib
import json
from collections import deque
from collections.abc import Mapping

from luigi.task import Task, flatten, task_id_str
from luigi.task_register import TaskClassException


class FreezeManifestError(Exception):
    """Base class for all freeze manifest related errors."""


class UnknownTaskFamilyError(FreezeManifestError):
    """
    Raised when a manifest names a task family that is not present in the
    (optionally whitelisted) task registry. The family is never imported;
    resolution only consults already-registered task classes.
    """


class DependencyDriftError(FreezeManifestError):
    """
    Raised when the dependency graph described by current task code no
    longer matches an approved :py:class:`TaskGraphManifest`.
    """


class GraphNotFullyFrozenError(FreezeManifestError):
    """
    Raised when an operation requires a fully determined graph but the
    manifest contains tasks with dynamic (run-time only) dependencies.
    """


def _canonicalize_json_value(value):
    """
    Recursively sort mapping keys so logically equal JSON values always
    serialize identically, regardless of the original insertion order.
    """
    if isinstance(value, Mapping):
        return {key: _canonicalize_json_value(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize_json_value(item) for item in value]
    return value


def normalize_param_string(serialized):
    """
    Canonicalize a single serialized parameter value.

    JSON-looking values (e.g. :py:class:`~luigi.DictParameter` values, whose
    serialization preserves the original dict ordering) are re-emitted with
    sorted keys; anything else is returned unchanged.
    """
    try:
        decoded = json.loads(serialized)
    except (TypeError, ValueError):
        return serialized
    return json.dumps(_canonicalize_json_value(decoded), sort_keys=True, separators=(",", ":"))


def normalize_task_params(task):
    """
    Return the significant, public parameters of ``task`` as a mapping of
    name to canonicalized serialized value.
    """
    str_params = task.to_str_params(only_significant=True, only_public=True)
    return {name: normalize_param_string(value) for name, value in str_params.items()}


def node_id_str(task_family, normalized_params):
    """
    Compute the manifest node id for a task family and its normalized
    parameters. Reuses Luigi's own task id scheme so ids stay recognizable.
    """
    return task_id_str(task_family, normalized_params)


def describe_target(target):
    """
    Human-readable, deterministic description of an output target, e.g.
    ``luigi.local_target.LocalTarget:/tmp/result.txt``.
    """
    cls = type(target)
    return "{}.{}:{}".format(cls.__module__, cls.__qualname__, target)


def _node_record(task):
    params = normalize_task_params(task)
    node_id = node_id_str(task.get_task_family(), params)
    record = {
        "family": task.get_task_family(),
        "params": params,
        "outputs": sorted(describe_target(target) for target in flatten(task.output())),
    }
    return node_id, record


def _flatten_requires(requires):
    return [dep for dep in flatten(requires) if isinstance(dep, Task)]


class TaskGraphManifest:
    """
    Immutable, JSON-serializable description of a frozen task graph.

    Attributes:

    * ``tasks``: mapping of node id to ``{"family", "params", "outputs"}``
    * ``edges``: sorted tuple of ``(task_node_id, requirement_node_id)``
    * ``incomplete``: sorted tuple of node ids whose tasks declare dynamic
      dependencies and are therefore not fully frozen
    """

    def __init__(self, tasks, edges, incomplete=()):
        self.tasks = {node_id: tasks[node_id] for node_id in sorted(tasks)}
        self.edges = tuple(sorted(set(edges)))
        self.incomplete = tuple(sorted(incomplete))

    @property
    def is_fully_frozen(self):
        """
        ``True`` only when no task in the graph was marked as having dynamic
        dependencies. A graph containing dynamic tasks must never be
        presented as fully determined.
        """
        return not self.incomplete

    def assert_fully_frozen(self):
        if not self.is_fully_frozen:
            raise GraphNotFullyFrozenError(
                "Manifest is not fully frozen; these tasks have dynamic dependencies: {}".format(", ".join(self.incomplete))
            )

    def to_dict(self):
        return {
            "tasks": {node_id: dict(record) for node_id, record in self.tasks.items()},
            "edges": [list(edge) for edge in self.edges],
            "incomplete": list(self.incomplete),
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            tasks=data["tasks"],
            edges=[tuple(edge) for edge in data["edges"]],
            incomplete=data.get("incomplete", ()),
        )

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, payload):
        return cls.from_dict(json.loads(payload))

    def digest(self):
        """
        Stable SHA-256 hex digest of the manifest. Identical graphs produce
        identical digests regardless of parameter dict ordering, traversal
        order, or how shared dependencies were reached.
        """
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    def __eq__(self, other):
        return isinstance(other, TaskGraphManifest) and self.to_dict() == other.to_dict()

    def __hash__(self):
        return hash(self.digest())

    def __repr__(self):
        return "TaskGraphManifest(tasks={}, edges={}, incomplete={})".format(len(self.tasks), len(self.edges), len(self.incomplete))


def freeze_graph(root_task, dynamic_tasks=()):
    """
    Walk the static ``requires()`` graph of ``root_task`` and return a
    :py:class:`TaskGraphManifest`.

    :param root_task: the task at the root of the graph to freeze.
    :param dynamic_tasks: tasks (instances, classes or family names) whose
        dependencies are only known at run time. Matching nodes are recorded
        in the manifest's ``incomplete`` set so the manifest honestly reports
        that the full graph is not determined.
    """
    dynamic_families = set()
    for entry in dynamic_tasks:
        if isinstance(entry, Task):
            dynamic_families.add(entry.get_task_family())
        elif isinstance(entry, str):
            dynamic_families.add(entry)
        else:
            dynamic_families.add(entry.get_task_family())

    tasks = {}
    edges = []
    incomplete = set()
    seen = set()
    queue = deque([root_task])
    while queue:
        task = queue.popleft()
        node_id, record = _node_record(task)
        if node_id in seen:
            continue
        seen.add(node_id)
        tasks[node_id] = record
        if record["family"] in dynamic_families:
            incomplete.add(node_id)
        for dep in _flatten_requires(task.requires()):
            dep_id, _ = _node_record(dep)
            edges.append((node_id, dep_id))
            queue.append(dep)

    return TaskGraphManifest(tasks=tasks, edges=edges, incomplete=incomplete)


def _resolve_task_class(family, allowed_families):
    if allowed_families is not None and family not in allowed_families:
        raise UnknownTaskFamilyError("Task family {!r} is not in the allowed task whitelist".format(family))
    try:
        # Resolution goes through the registry of already-imported task
        # classes only. The manifest is pure data and must never cause
        # arbitrary modules to be imported.
        return Task.get_task_cls(family)
    except TaskClassException as exc:
        raise UnknownTaskFamilyError("Task family {!r} is not registered: {}".format(family, exc)) from exc


def restore_graph(manifest, allowed_families=None):
    """
    Rebuild task instances from a manifest.

    Task classes are looked up exclusively in the task registry of
    already-imported modules, optionally restricted to ``allowed_families``
    (an iterable of task family names). Families outside the whitelist, or
    not registered at all, raise :py:exc:`UnknownTaskFamilyError`; the
    manifest can never be used to import arbitrary modules.

    :return: a mapping of node id to task instance.
    """
    allowed = None if allowed_families is None else set(allowed_families)
    restored = {}
    for node_id, record in manifest.tasks.items():
        task_cls = _resolve_task_class(record["family"], allowed)
        restored[node_id] = task_cls.from_str_params(record["params"])
    return restored


def verify_graph(manifest, allowed_families=None, require_fully_frozen=True):
    """
    Restore the manifest through the whitelisted registry and check that the
    dependency graph produced by the *current* task implementations still
    matches the approved manifest.

    Raises :py:exc:`GraphNotFullyFrozenError` if the manifest contains
    dynamic tasks (unless ``require_fully_frozen`` is ``False``), and
    :py:exc:`DependencyDriftError` if any node or edge drifted.

    :return: the mapping of node id to restored task instance on success.
    """
    if require_fully_frozen:
        manifest.assert_fully_frozen()

    restored = restore_graph(manifest, allowed_families=allowed_families)

    problems = []
    current_edges = set()
    for node_id, task in restored.items():
        current_id, current_record = _node_record(task)
        if current_id != node_id:
            problems.append("node {!r} now identifies as {!r}".format(node_id, current_id))
        elif current_record["outputs"] != manifest.tasks[node_id]["outputs"]:
            problems.append("outputs of {!r} changed".format(node_id))
        for dep in _flatten_requires(task.requires()):
            dep_id, _ = _node_record(dep)
            current_edges.add((node_id, dep_id))

    approved_edges = set(manifest.edges)
    for edge in sorted(current_edges - approved_edges):
        problems.append("new dependency edge {} -> {}".format(*edge))
    for edge in sorted(approved_edges - current_edges):
        problems.append("missing dependency edge {} -> {}".format(*edge))
    for edge in sorted(current_edges):
        if edge[1] not in manifest.tasks:
            problems.append("dependency {!r} of {!r} is not part of the approved manifest".format(edge[1], edge[0]))

    if problems:
        raise DependencyDriftError("Task graph drifted from the approved manifest: " + "; ".join(problems))
    return restored
