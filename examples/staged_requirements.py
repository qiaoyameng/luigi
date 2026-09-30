# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE/2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""

Named stages example
====================

Dynamic requirements can be *named* with ``DynamicRequirements(..., stage=...)``.
The scheduler records, for every stage, the exact set of dependencies it was
yielded with and whether that stage has completed. If the worker dies in the
middle of ``run`` and the task is later re-entered, the task is handed back the
completed stage markers so the external work guarded by those stages does not
have to be submitted a second time.

A stage is only treated as finished on resume if the same stage name is
re-yielded with the exact same dependency task ids. If the code has changed in a
way that produces a different dependency set, the stored marker is discarded and
the stage runs again - a previously completed stage can never silently stand in
for a different piece of work.

Completing a stage does not mark the task as done. Final success is still
decided by ``output()``/``complete()``.

This example simulates an external batch system (e.g. a Hive/Spark job):

1. ``run`` "submits" an external job once and yields a task that waits for its
   acceptance (the ``submit_job`` stage).
2. It yields a second task waiting for the job's result (the ``fetch_result``
   stage), then writes its own output.

Run a central scheduler and a first worker, kill the worker after the external
job is accepted, and start a second worker. The external job is submitted once.

.. code-block:: console

    luigid --state-path /tmp/luigi-state.pickle &
    luigi --module examples.staged_requirements StagedExternalJob \\
        --local-dir /tmp/staged-job --workers 2 --scheduler-host localhost
    # ... kill the worker once "external job accepted" is printed ...
    luigi --module examples.staged_requirements StagedExternalJob \\
        --local-dir /tmp/staged-job --workers 2 --scheduler-host localhost

"""

import os
import time

import luigi


class SubmitExternalJob(luigi.Task):
    """Completes as soon as the (simulated) external job has been accepted."""

    local_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.local_dir, "job_accepted"))

    def run(self):
        with self.output().open("w") as out:
            out.write("external-job-4242")
        print("external job accepted", flush=True)


class ExternalJobResult(luigi.Task):
    """Completes when the (simulated) external job has produced its result."""

    local_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.local_dir, "job_result"))

    def run(self):
        # Simulate the external system taking a while to finish.
        time.sleep(10)
        with open(os.path.join(self.local_dir, "job_accepted")) as accepted:
            job_id = accepted.read()
        with self.output().open("w") as out:
            out.write("result for {}".format(job_id))


class StagedExternalJob(luigi.Task):
    """
    A two-stage task. The first stage submits external work; the second stage
    consumes its result. On resume after a worker failure the ``submit_job``
    stage is skipped because the scheduler hands back its completion marker.
    """

    local_dir = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(self.local_dir, "final_output"))

    def _submit_external_job_once(self):
        # Pretend to submit work to an external system. A resume after a worker
        # crash must never submit it again, so the submission itself is made
        # idempotent and guarded by the named stage marker.
        if not self.stage_complete("submit_job"):
            marker = os.path.join(self.local_dir, "submission_count")
            if not os.path.exists(marker):
                with open(marker, "w") as out:
                    out.write("1")
                print("submitting external job", flush=True)
        return SubmitExternalJob(local_dir=self.local_dir)

    def run(self):
        submit = self._submit_external_job_once()
        yield luigi.DynamicRequirements(submit, stage="submit_job")

        result = ExternalJobResult(local_dir=self.local_dir)
        yield luigi.DynamicRequirements(result, stage="fetch_result")

        with self.output().open("w") as out, result.output().open("r") as src:
            out.write(src.read())


if __name__ == "__main__":
    luigi.run()
