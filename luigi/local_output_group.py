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
:class:`LocalOutputGroup` groups several :class:`~luigi.local_target.LocalTarget`
outputs of a single task run (e.g. body, index and statistics files of an ETL
job) into one logical :py:class:`~luigi.target.Target`.

Plain ``LocalTarget`` outputs are considered complete as soon as every path
exists, so a task can look "done" because of files left behind by *different*
runs. ``LocalOutputGroup`` instead ties the member files to an explicit run
identity and describes them with a manifest: the group only exists when the
manifest is published and every member file matches the run identity, size and
SHA-256 digest recorded in it. ``Task.complete()`` and bulk completeness
checks (e.g. via :class:`~luigi.task.MixinNaiveBulkComplete`) both delegate to
:meth:`LocalOutputGroup.exists`, so they always apply the exact same identity,
size and digest rules.

Write protocol
--------------

1. Members are written through :meth:`LocalOutputGroup.open` with mode ``'w'``.
   Data lands in a per-run *staged* file (``<final path>.<run_id>.staged``)
   via an atomic temp-file rename. Staged files are never exposed by the read
   path, so half-written output is never mistaken for a committed product.
2. :meth:`LocalOutputGroup.commit` verifies that every member was staged,
   moves each staged file to its final path and only then publishes the
   manifest. The manifest is the single completeness marker.
3. If anything fails before the manifest is published, the group is simply
   not complete: a previously committed version stays untouched and valid, or
   the group has no complete state at all. Old committed files are never used
   to fill gaps in a new run's output, because the new run only becomes
   visible through a new manifest.

Atomicity boundaries (local filesystem only)
--------------------------------------------

* Each single file (staged member, final member, manifest) is written to a
  temp file in the same directory and published with ``os.replace``, which is
  atomic on local POSIX filesystems.
* Committing the *group* is **not** atomic across files: members are renamed
  one by one and the manifest is published last. A crash in the middle of
  ``commit()`` can leave a mix of new and old member files. This is always
  detected, because the old manifest's digests no longer match, so the group
  reads as incomplete (never falsely complete) and rerunning the task
  recovers. Note that in this narrow window the previously committed version
  may already have been overwritten; failures *before* ``commit()`` do
  preserve the old committed version.
* Only the local filesystem is supported. There is no inter-process locking:
  two processes committing the same group concurrently is a usage error.
"""

import glob
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

from luigi.local_target import LocalFileSystem, LocalTarget
from luigi.target import Target

MANIFEST_VERSION = 1
STAGED_SUFFIX = ".staged"
DEFAULT_MANIFEST_NAME = ".luigi-output-group.manifest.json"


class IncompleteOutputGroupError(Exception):
    """
    Raised when :meth:`LocalOutputGroup.commit` is called before every member
    of the group has been staged.
    """

    pass


def _sha256_and_size(path, chunk_size=1024 * 1024):
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fileobj:
        while True:
            chunk = fileobj.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


class LocalOutputGroup(Target):
    """
    A named group of local files produced by one run of a task, committed and
    validated as a unit through a manifest.

    :param members: mapping of member name to a path or a
        :class:`~luigi.local_target.LocalTarget`. Member names are stable
        identifiers (e.g. ``body``, ``index``, ``stats``).
    :param manifest_path: where the manifest is published. Defaults to
        ``.luigi-output-group.manifest.json`` in the common directory of the
        member paths.
    :param run_id: explicit identity of this run. Defaults to a random UUID.
        Staged files are namespaced by it, so a re-run never clobbers the
        committed output of a previous run before ``commit()``.
    """

    fs = LocalFileSystem()

    def __init__(self, members, manifest_path=None, run_id=None):
        if not members:
            raise ValueError("members must be a non-empty mapping of name to path or LocalTarget")
        self.members = {}
        for name, member in members.items():
            self.members[name] = member if isinstance(member, LocalTarget) else LocalTarget(member)
        self.run_id = run_id or uuid.uuid4().hex
        if manifest_path is None:
            directories = [os.path.dirname(os.path.abspath(target.path)) for target in self.members.values()]
            manifest_path = os.path.join(os.path.commonpath(directories), DEFAULT_MANIFEST_NAME)
        self.manifest_path = manifest_path

    def member(self, name):
        """
        Return the :class:`~luigi.local_target.LocalTarget` for the committed
        (final) path of member ``name``.
        """
        return self.members[name]

    @property
    def paths(self):
        """
        Mapping of member name to its committed (final) path.
        """
        return {name: target.path for name, target in self.members.items()}

    def staged_path(self, name):
        """
        Path this run stages member ``name`` into before commit. Never used by
        the read path.
        """
        return "%s.%s%s" % (self.members[name].path, self.run_id, STAGED_SUFFIX)

    def open(self, name, mode="r"):
        """
        Open member ``name``. Mode ``'r'`` always reads the committed final
        path (staged data of an unfinished run is invisible). Mode ``'w'``
        writes to this run's staged path, honouring the member's format.
        """
        rwmode = mode.replace("b", "").replace("t", "")
        if rwmode == "r":
            return self.members[name].open(mode)
        elif rwmode == "w":
            member = self.members[name]
            return LocalTarget(self.staged_path(name), format=member.format).open(mode)
        else:
            raise Exception("mode must be 'r' or 'w' (got: %s)" % mode)

    def commit(self):
        """
        Publish this run's output: move every staged member to its final path
        and publish the manifest last. Raises
        :class:`IncompleteOutputGroupError` without changing any committed
        state if not all members were staged.
        """
        missing = [name for name in self.members if not self.fs.exists(self.staged_path(name))]
        if missing:
            raise IncompleteOutputGroupError("cannot commit, members not staged for run %s: %s" % (self.run_id, ", ".join(sorted(missing))))

        entries = {}
        for name, target in self.members.items():
            sha256, size = _sha256_and_size(self.staged_path(name))
            entries[name] = {"path": target.path, "size": size, "sha256": sha256}

        for name, target in self.members.items():
            os.replace(self.staged_path(name), target.path)

        manifest = {
            "version": MANIFEST_VERSION,
            "run_id": self.run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "members": entries,
        }
        manifest_target = LocalTarget(self.manifest_path)
        with manifest_target.open("w") as fileobj:
            fileobj.write(json.dumps(manifest, indent=2, sort_keys=True))

        self.prune_staging()

    def abort(self):
        """
        Discard this run's staged files. Committed state is never touched.
        """
        for name in self.members:
            staged = self.staged_path(name)
            if self.fs.exists(staged):
                self.fs.remove(staged)

    def prune_staging(self):
        """
        Best-effort removal of staged files left behind by *other* (failed or
        abandoned) runs of this group.
        """
        for target in self.members.values():
            pattern = glob.escape(target.path) + ".*" + STAGED_SUFFIX
            for leftover in glob.glob(pattern):
                if leftover != "%s.%s%s" % (target.path, self.run_id, STAGED_SUFFIX):
                    try:
                        self.fs.remove(leftover)
                    except OSError:
                        pass

    def manifest(self):
        """
        Return the published manifest as a dict, or ``None`` if there is no
        readable manifest.
        """
        try:
            with open(self.manifest_path, "rb") as fileobj:
                manifest = json.loads(fileobj.read().decode("utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(manifest, dict) or manifest.get("version") != MANIFEST_VERSION:
            return None
        if not isinstance(manifest.get("run_id"), str) or not isinstance(manifest.get("members"), dict):
            return None
        return manifest

    def validation_errors(self):
        """
        Return a list of reasons why the committed output is not valid, an
        empty list meaning the group is complete. The same rules back
        :meth:`exists`, and therefore ``Task.complete()`` and bulk completeness
        checks: manifest identity plus per-member presence, size and SHA-256.
        """
        manifest = self.manifest()
        if manifest is None:
            return ["no valid manifest at %s" % self.manifest_path]

        errors = []
        entries = manifest["members"]
        if set(entries) != set(self.members):
            errors.append("manifest members %s do not match group members %s" % (sorted(entries), sorted(self.members)))
        for name, target in self.members.items():
            entry = entries.get(name)
            if entry is None:
                continue
            if entry.get("path") != target.path:
                errors.append("member %s: manifest path %r does not match %r" % (name, entry.get("path"), target.path))
                continue
            if not self.fs.exists(target.path):
                errors.append("member %s: missing committed file %s" % (name, target.path))
                continue
            sha256, size = _sha256_and_size(target.path)
            if size != entry.get("size"):
                errors.append("member %s: size %d does not match manifest size %r" % (name, size, entry.get("size")))
            if sha256 != entry.get("sha256"):
                errors.append("member %s: sha256 mismatch with manifest" % name)
        return errors

    def exists(self):
        """
        ``True`` only when the manifest is published and every member file
        matches the run identity, size and SHA-256 recorded in it.
        """
        return not self.validation_errors()

    def remove(self):
        """
        Remove the manifest and all committed member files.
        """
        if self.fs.exists(self.manifest_path):
            self.fs.remove(self.manifest_path)
        for target in self.members.values():
            if target.exists():
                target.remove()
