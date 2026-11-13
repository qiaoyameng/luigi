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
import shutil
import tempfile

from helpers import unittest

import luigi
from luigi.local_output_group import IncompleteOutputGroupError, LocalOutputGroup
from luigi.task import MixinNaiveBulkComplete

MEMBERS = ("body", "index", "stats")


class EtlTask(MixinNaiveBulkComplete, luigi.Task):
    """
    ETL task writing body, index and stats files as one output group.
    ``fail_member`` simulates a crash while writing that member,
    ``skip_commit`` simulates exiting before the commit point.
    """

    date = luigi.Parameter()
    fail_member = luigi.OptionalParameter(default=None)
    skip_commit = luigi.BoolParameter(default=False)

    def output_dir(self):
        return os.path.join(self._test_tmpdir, "output", self.date)

    def output(self):
        return LocalOutputGroup({name: os.path.join(self.output_dir(), "%s.txt" % name) for name in MEMBERS})

    def run(self):
        group = self.output()
        for name in MEMBERS:
            if self.fail_member == name:
                raise RuntimeError("simulated failure while writing %s" % name)
            with group.open(name, "w") as fileobj:
                fileobj.write("%s for %s\n" % (name, self.date))
        if not self.skip_commit:
            group.commit()


class LocalOutputGroupTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="luigi-output-group-test-")
        EtlTask._test_tmpdir = self.tmpdir

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def make_task(self, **kwargs):
        kwargs.setdefault("date", "2026-11-13")
        return EtlTask(**kwargs)

    def assert_read_back(self, group, date="2026-11-13"):
        for name in MEMBERS:
            with group.open(name, "r") as fileobj:
                self.assertEqual(fileobj.read(), "%s for %s\n" % (name, date))

    def test_successful_run_commits_group_and_reads_back(self):
        task = self.make_task()
        self.assertFalse(task.complete())
        task.run()
        self.assertTrue(task.complete())
        self.assertTrue(os.path.exists(task.output().manifest_path))
        self.assert_read_back(task.output())

    def test_second_file_write_failure_is_rerunnable(self):
        failing = self.make_task(fail_member="index")
        self.assertRaises(RuntimeError, failing.run)
        self.assertFalse(failing.complete())
        # body was staged but nothing was committed
        group = failing.output()
        self.assertFalse(os.path.exists(group.manifest_path))
        for name in MEMBERS:
            self.assertFalse(os.path.exists(group.paths[name]))

        rerun = self.make_task()
        rerun.run()
        self.assertTrue(rerun.complete())
        self.assert_read_back(rerun.output())

    def test_exit_before_commit_is_rerunnable(self):
        crashed = self.make_task(skip_commit=True)
        crashed.run()
        self.assertFalse(crashed.complete())
        # all members were staged, none committed
        group = crashed.output()
        staged = os.listdir(os.path.dirname(group.paths["body"]))
        for name in MEMBERS:
            self.assertFalse(os.path.exists(group.paths[name]))
        self.assertEqual(len([f for f in staged if f.endswith(".staged")]), len(MEMBERS))

        rerun = self.make_task()
        rerun.run()
        self.assertTrue(rerun.complete())
        self.assert_read_back(rerun.output())
        # staged leftovers of the crashed run were pruned on commit
        leftovers = [f for f in os.listdir(os.path.dirname(group.paths["body"])) if f.endswith(".staged")]
        self.assertEqual(leftovers, [])

    def test_stale_directory_residue_is_rerunnable(self):
        task = self.make_task()
        group = task.output()
        os.makedirs(os.path.dirname(group.paths["body"]), exist_ok=True)
        # residue from an abandoned older run: staged files left in the output dir
        for name in MEMBERS:
            residue = "%s.stale-run-000.staged" % group.paths[name]
            with open(residue, "w") as fileobj:
                fileobj.write("stale partial data\n")
            self.assertTrue(os.path.exists(residue))
        self.assertFalse(task.complete())

        task.run()
        self.assertTrue(task.complete())
        self.assert_read_back(task.output())
        leftovers = [f for f in os.listdir(os.path.dirname(group.paths["body"])) if f.endswith(".staged")]
        self.assertEqual(leftovers, [])

    def test_failure_preserves_previous_committed_version(self):
        first = self.make_task()
        first.run()
        self.assertTrue(first.complete())

        failing = self.make_task(fail_member="stats")
        self.assertRaises(RuntimeError, failing.run)
        # the old committed version is still complete and readable
        self.assertTrue(failing.complete())
        self.assert_read_back(failing.output())

    def test_old_files_cannot_fill_gaps_of_new_run(self):
        first = self.make_task()
        first.run()
        group = first.output()
        # a foreign file from another run replaces one committed member
        with open(group.paths["index"], "w") as fileobj:
            fileobj.write("foreign content from another run\n")
        self.assertFalse(first.complete())
        errors = group.validation_errors()
        self.assertTrue(any("index" in error for error in errors))

    def test_missing_member_invalidates_group(self):
        task = self.make_task()
        task.run()
        os.remove(task.output().paths["stats"])
        self.assertFalse(task.complete())

    def test_commit_requires_all_members_staged(self):
        group = self.make_task().output()
        with group.open("body", "w") as fileobj:
            fileobj.write("only body\n")
        self.assertRaises(IncompleteOutputGroupError, group.commit)
        self.assertFalse(group.exists())
        group.abort()
        self.assertFalse(os.path.exists(group.staged_path("body")))

    def test_bulk_complete_matches_complete(self):
        done = self.make_task(date="2026-11-10")
        done.run()
        crashed = self.make_task(date="2026-11-11", skip_commit=True)
        crashed.run()
        pending = self.make_task(date="2026-11-12")

        dates = [("2026-11-10",), ("2026-11-11",), ("2026-11-12",)]
        self.assertEqual(EtlTask.bulk_complete(dates), [("2026-11-10",)])
        self.assertEqual(
            [EtlTask(date=date).complete() for date, in dates],
            [done.complete(), crashed.complete(), pending.complete()],
        )

    def test_mixed_runs_are_not_complete(self):
        # paths alone are not enough: files from different runs without a
        # manifest never count as a committed group
        task = self.make_task()
        group = task.output()
        os.makedirs(os.path.dirname(group.paths["body"]), exist_ok=True)
        for name in MEMBERS:
            with open(group.paths[name], "w") as fileobj:
                fileobj.write("leftover from some older run\n")
        self.assertFalse(task.complete())


if __name__ == "__main__":
    unittest.main()
