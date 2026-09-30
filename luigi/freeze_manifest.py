#
# Copyright (c) 2026 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

"""
Freeze-manifest API for review-then-execute production workflows.

The typical flow is:

1. Build the task graph locally and call :func:`freeze_graph` to obtain a
   :class:`FreezeManifest`. The manifest records, for every reachable task,
   its task family, normalized (order-independent) parameters, its output
   target descriptions and the static dependency edges of the graph.
2. Store or transmit ``manifest.to_json()`` for review/auditing. Because all
   collections inside the manifest are sorted and parameters are normalized,
   :meth:`FreezeManifest.digest` is a stable content hash of the reviewed
   graph.
3. At execution time, call :func:`restore_graph` (or :func:`run_manifest`)
   with a :class:`TaskRegistry` whitelist. Tasks are re-instantiated *only*
   from classes explicitly registered in that whitelist -- the manifest never
   triggers module imports. The live dependency graph of the restored tasks
   is then re-frozen and compared against the manifest; any drift (e.g. a
   task implementation that grew a new dependency since review) raises
   :class:`DependencyDriftError`.

Tasks with dynamic requirements (dependencies yielded from ``run()``) cannot
be fully captured by a static walk. They are explicitly marked with
``fully_frozen=False`` in the manifest, and :attr:`FreezeManifest.is_complete`
is ``False`` for such graphs, so callers can never mistake a partially
dynamic graph for a fully determined one.
"""

import hashlib
import inspect
import json
from collections import deque
from collections.abc import Mapping

from luigi.task import Task, flatten, flatten_output, task_id_str

MANIFEST_VERSION = 1


class FreezeManifestError(Exception):
    """Base class for all freeze-manifest related errors."""


class UnknownTaskFamilyError(FreezeManifestError):
    """A manifest entry references a task family missing from the registry."""


class DependencyDriftError(FreezeManifestError):
    """The restored graph no longer matches the reviewed manifest."""


class IncompleteManifestError(FreezeManifestError):
    """Execution was requested for a manifest that is not fully frozen."""


def _normalize_param_value(serialized):
    """
    Make a serialized parameter value order-independent.

    Structured parameters (e.g. :py:class:`luigi.DictParameter`) serialize to
    JSON whose key order follows the insertion order of the source dict. To
    keep the manifest digest stable across equivalent orderings, JSON values
    are re-dumped with sorted keys; non-JSON values are returned unchanged.
    """
    try:
        parsed = json.loads(serialized)
    except (TypeError, ValueError):
        return serialized
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


def _normalized_str_params(task):
    return {
        name: _normalize_param_value(value)
        for name, value in sorted(task.to_str_params().items())
    }


def _describe_target(target):
    path = getattr(target, "path", None)
    if path is not None:
        return "%s:%s" % (type(target).__name__, path)
    return repr(target)


def _is_fully_frozen(task):
    """
    A task is fully frozen only if all of its dependencies are statically
    declared through ``requires()``. Tasks that yield dynamic requirements
    from a generator ``run()`` (or opt in via the ``has_dynamic_requirements``
    class attribute) are marked as not fully frozen.
    """
    if getattr(task, "has_dynamic_requirements", False):
        return False
    return not inspect.isgeneratorfunction(task.run)


def _freeze_task(task):
    family = task.get_task_family()
    params = _normalized_str_params(task)
    return {
        "id": task_id_str(family, params),
        "family": family,
        "params": params,
        "outputs": sorted(_describe_target(target) for target in flatten_output(task)),
        "fully_frozen": _is_fully_frozen(task),
    }


class FreezeManifest:
    """
    An immutable, canonical description of a frozen task graph.

    All collections are sorted at construction time, so two freezes of
    equivalent graphs always produce identical :meth:`digest` values,
    regardless of parameter dict ordering or traversal order.
    """

    def __init__(self, tasks, edges):
        self._tasks = tuple(sorted(tasks, key=lambda entry: entry["id"]))
        self._edges = tuple(sorted(tuple(edge) for edge in edges))

    @property
    def tasks(self):
        """Tuple of frozen task entries, sorted by task id."""
        return self._tasks

    @property
    def edges(self):
        """Tuple of ``(task_id, dependency_id)`` pairs, sorted."""
        return self._edges

    @property
    def is_complete(self):
        """
        ``True`` only if every task in the graph is fully frozen, i.e. no
        task relies on dynamic requirements discovered at run time.
        """
        return all(entry["fully_frozen"] for entry in self._tasks)

    def incomplete_tasks(self):
        """Ids of tasks that are marked as not fully frozen."""
        return tuple(entry["id"] for entry in self._tasks if not entry["fully_frozen"])

    def to_dict(self):
        return {
            "version": MANIFEST_VERSION,
            "tasks": [dict(entry) for entry in self._tasks],
            "edges": [list(edge) for edge in self._edges],
        }

    def digest(self):
        """
        A stable SHA-256 content hash of the manifest, suitable for signing
        off a reviewed graph and later verifying that nothing changed.
        """
        canonical = json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_json(self, indent=None):
        return json.dumps(self.to_dict(), sort_keys=True, indent=indent)

    @classmethod
    def from_dict(cls, data):
        if data.get("version") != MANIFEST_VERSION:
            raise FreezeManifestError(
                "Unsupported manifest version: %r" % (data.get("version"),)
            )
        return cls(tasks=data["tasks"], edges=data["edges"])

    @classmethod
    def from_json(cls, data):
        return cls.from_dict(json.loads(data))

    def save(self, path):
        with open(path, "w") as fp:
            fp.write(self.to_json(indent=2))

    @classmethod
    def load(cls, path):
        with open(path) as fp:
            return cls.from_json(fp.read())

    def __eq__(self, other):
        return isinstance(other, FreezeManifest) and self.digest() == other.digest()

    def __repr__(self):
        return "FreezeManifest(tasks=%d, edges=%d, complete=%r)" % (
            len(self._tasks),
            len(self._edges),
            self.is_complete,
        )


def freeze_graph(tasks):
    """
    Walk the static dependency graph of ``tasks`` (a single task or an
    iterable of tasks) and return a :class:`FreezeManifest`.

    Only dependencies declared via ``requires()`` are captured. Tasks with
    dynamic requirements are included but marked ``fully_frozen=False``.
    """
    if isinstance(tasks, Task):
        tasks = [tasks]

    entries = {}
    edges = set()
    queue = deque(flatten(list(tasks)))
    while queue:
        task = queue.popleft()
        entry = _freeze_task(task)
        if entry["id"] in entries:
            continue
        entries[entry["id"]] = entry
        for dep in task.deps():
            dep_entry = _freeze_task(dep)
            edges.add((entry["id"], dep_entry["id"]))
            queue.append(dep)
    return FreezeManifest(tasks=entries.values(), edges=edges)


class TaskRegistry:
    """
    Explicit whitelist mapping task families to task classes.

    This is the *only* source of task classes at restore time: manifests are
    pure data and never cause module imports, so a reviewed manifest cannot
    smuggle in arbitrary code by naming a module to import.
    """

    def __init__(self):
        self._classes = {}

    def register(self, task_class):
        """Register a task class; usable as a decorator."""
        self._classes[task_class.get_task_family()] = task_class
        return task_class

    def get(self, family):
        try:
            return self._classes[family]
        except KeyError:
            raise UnknownTaskFamilyError(
                "Task family %r is not in the task registry whitelist" % (family,)
            )

    def __contains__(self, family):
        return family in self._classes


def _diff_manifests(expected, actual):
    expected_tasks = {entry["id"] for entry in expected.tasks}
    actual_tasks = {entry["id"] for entry in actual.tasks}
    expected_edges = set(expected.edges)
    actual_edges = set(actual.edges)
    return {
        "added_tasks": sorted(actual_tasks - expected_tasks),
        "removed_tasks": sorted(expected_tasks - actual_tasks),
        "added_edges": sorted(actual_edges - expected_edges),
        "removed_edges": sorted(expected_edges - actual_edges),
    }


def restore_graph(manifest, registry, allow_incomplete=False):
    """
    Re-instantiate the tasks described by ``manifest`` using only the classes
    whitelisted in ``registry`` (a :class:`TaskRegistry` or a mapping of
    family to task class), and verify that the live dependency graph has not
    drifted from the reviewed manifest.

    :param allow_incomplete: permit restoring manifests that contain tasks
        with dynamic requirements. The drift check still applies to the
        statically declared part of the graph.
    :returns: the list of restored root tasks (tasks nothing else depends on).
    :raises IncompleteManifestError: if the manifest is not fully frozen and
        ``allow_incomplete`` is false.
    :raises UnknownTaskFamilyError: if a manifest entry is not whitelisted.
    :raises DependencyDriftError: if the restored graph differs from the
        manifest in any task, parameter, output or dependency edge.
    """
    if not manifest.is_complete and not allow_incomplete:
        raise IncompleteManifestError(
            "Manifest is not fully frozen; these tasks have dynamic "
            "requirements: %s" % (", ".join(manifest.incomplete_tasks()),)
        )

    if not isinstance(registry, TaskRegistry):
        if isinstance(registry, Mapping):
            wrapped = TaskRegistry()
            wrapped._classes.update(registry)
            registry = wrapped
        else:
            raise TypeError("registry must be a TaskRegistry or a mapping")

    restored = {}
    for entry in manifest.tasks:
        task_class = registry.get(entry["family"])
        task = task_class.from_str_params(entry["params"])
        restored[entry["id"]] = task

    refrozen = freeze_graph(list(restored.values()))
    if refrozen.digest() != manifest.digest():
        diff = _diff_manifests(manifest, refrozen)
        raise DependencyDriftError(
            "Restored task graph has drifted from the reviewed manifest: %s"
            % (json.dumps(diff, sort_keys=True),)
        )

    depended_on = {dep_id for _, dep_id in manifest.edges}
    return [task for task_id, task in restored.items() if task_id not in depended_on]


def run_manifest(manifest, registry, allow_incomplete=False, **build_kwargs):
    """
    Restore a reviewed manifest and run it with :py:func:`luigi.build`.

    The graph is restored exclusively from the ``registry`` whitelist and is
    verified against the manifest before anything is scheduled.
    """
    from luigi.interface import build

    tasks = restore_graph(manifest, registry, allow_incomplete=allow_incomplete)
    return build(tasks, **build_kwargs)
