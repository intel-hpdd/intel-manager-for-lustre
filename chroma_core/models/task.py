# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

from django.db import models
from django.db.models import CASCADE, SET_NULL
from django.contrib.postgres.fields import ArrayField, JSONField
import django.utils.timezone

from chroma_core.lib.job import DependOn, DependAll, Step
from chroma_core.models import ManagedFilesystem, ManagedHost
from chroma_core.models import AdvertisedJob


class LustreFidField(models.Field):
    description = "A Lustre FID"

    seq = models.BigIntegerField()
    oid = models.IntegerField()
    ver = models.IntegerField()

    def db_type(self, connection):
        return "lustre_fid"

    class Meta:
        app_label = "chroma_core"


class Task(models.Model):
    """ List of task queues """

    filesystem = models.ForeignKey("ManagedFilesystem", on_delete=CASCADE)

    name = models.CharField(max_length=128)
    start = models.DateTimeField(null=False)
    finish = models.DateTimeField(null=True, blank=True)

    # states = [ "created", "finished", "closed" ]
    # Created - Task is started and mailbox is created on servers
    #   - main ingest phase
    #   - outgest can run
    # Finished - Mailbox is removed from servers
    #   - ingest is complete
    #   - outgest is running
    # Closed - outgest is completed
    state = models.CharField(max_length=16)

    fids_total = models.BigIntegerField(default=0)
    fids_completed = models.BigIntegerField(default=0)
    fids_failed = models.BigIntegerField(default=0)
    data_transfered = models.BigIntegerField(default=0)

    single_runner = models.BooleanField(default=False, null=False)
    keep_failed = models.BooleanField(default=True, null=False)
    # this is an array of text fields because an array of Char(16) causes problems for
    # diesel on the rust side
    actions = ArrayField(models.TextField())

    running_on = models.ForeignKey("ManagedHost", blank=True, null=True, on_delete=SET_NULL)

    args = JSONField(default={})

    class Meta:
        app_label = "chroma_core"
        unique_together = ("name",)


class CreateTaskJob(AdvertisedJob):
    task = models.ForeignKey("Task", on_delete=CASCADE)
    requires_confirmation = False
    classes = ["Task"]
    verb = "Create"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return "Create Task on servers"

    @classmethod
    def get_args(cls, task):
        return {"task_id": task.id}

    def description(self):
        return "Create Task on servers"

    def get_steps(self):
        steps = []
        for host in self.task.filesystem.get_servers():
            steps.append((CreateTaskStep, {"task": self.task.name, "host": host.fqdn}))

        return steps


class CreateTaskStep(Step):
    def run(self, args):
        self.invoke_rust_agent_expect_result(args["host"], "postoffice_add", args["task"])


class RemoveTaskJob(AdvertisedJob):
    task = models.ForeignKey("Task", on_delete=CASCADE)
    requires_confirmation = False
    classes = ["Task"]
    verb = "Remove"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return "Remove Task from servers"

    @classmethod
    def get_args(cls, task):
        return {"task_id": task.id}

    def description(self):
        return "Remove Task from servers"

    def get_steps(self):
        steps = []
        if self.task.state != "removed":
            for host in self.task.filesystem.get_servers():
                steps.append((RemoveTaskStep, {"task": self.task.name, "host": host.fqdn}))

        return steps

    def on_success(self):
        self.task.finish = django.utils.timezone.now()
        self.task.state = "removed"
        self.task.save(update_fields=["state", "finish"])


class RemoveTaskStep(Step):
    def run(self, args):
        self.invoke_rust_agent_expect_result(args["host"], "postoffice_remove", args["task"])


class FidTaskQueue(models.Model):
    class Meta:
        app_label = "chroma_core"

    fid = LustreFidField()

    task = models.ForeignKey("Task", on_delete=CASCADE)

    data = JSONField(default={})


class FidTaskError(models.Model):
    class Meta:
        app_label = "chroma_core"

    fid = LustreFidField()

    task = models.ForeignKey("Task", on_delete=CASCADE)

    data = JSONField(default={})
    errno = models.PositiveSmallIntegerField(default=0, null=False)
