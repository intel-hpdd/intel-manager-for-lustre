# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

import logging
import os

from django.db import models
from django.db.models import CASCADE, Q
from chroma_core.lib.cache import ObjectCache
from chroma_core.lib.job import DependOn, DependAll, Step, job_log
from chroma_core.models.jobs import StatefulObject, StateChangeJob, Job
from chroma_core.models.utils import (
    DeletableMetaclass,
    StartResourceStep,
    StopResourceStep,
    MountStep,
    UnmountStep,
    CreateSystemdResourceStep,
    RemoveSystemdResourceStep,
)
from chroma_core.models import (
    ManagedFilesystem,
    ManagedMdt,
    OstPool,
    Task,
)
from chroma_help.help import help_text

############################################################
# Hotpools
#
# Assumptions:
# * components will be controlled via pacemaker
# * will be dependent on local client mount as "single on/off switch"
# * Only 1 hotpool setup per filesystem


class HotpoolConfiguration(StatefulObject):
    __metaclass__ = DeletableMetaclass

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]
        unique_together = ("filesystem",)

    states = ["unconfigured", "configured", "stopped", "started", "removed"]
    initial_state = "unconfigured"

    filesystem = models.ForeignKey("ManagedFilesystem", null=False, on_delete=CASCADE)
    ha_label = models.CharField(
        max_length=64,
        null=False,
        blank=False,
        help_text="Single resource that will controll entire hotpool configuration",
    )
    version = models.PositiveSmallIntegerField(default=2, null=False)

    def get_label(self):
        return "Hotpool Configuration"

    @property
    def mountpoint(self):
        return "/lustre/" + self.filesystem.name + "/client"

    def get_components(self):
        # self.version == 2:
        component_types = [Lamigo, Lpurge]

        components = []
        for ctype in component_types:
            components.extend([x for x in ctype.objects.filter(configuration__hotpool=self)])

        return components

    def get_tasks(self):
        # self.version == 2:
        component_types = [LamigoConfiguration, LpurgeConfiguration]

        amigo = LamigoConfiguration.objects.get(hotpool=self)
        tasks = [amigo.extend, amigo.resync]

        purge = LpurgeConfiguration.objects.get(hotpool=self)
        tasks.append(purge.purge)

        return tasks


class ConfigureHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "unconfigured", "configured")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Configure Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["configure_hotpool"]

    def description(self):
        return help_text["configure_hotpool"]

    def get_steps(self):
        steps = []

        fs = self.hotpool_configuration.filesystem
        mp = self.hotpool_configuration.mountpoint

        # create cloned client mount
        for host in fs.get_servers():
            # c.f. iml-wire-types::client::Mount
            # Mount.persist equates to automount
            steps.append((MountStep, {"host": host.fqdn, "auto": False, "spec": fs.mount_path(), "mountpoint": mp}))

        for host in (l[0] for l in fs.get_server_groups()):
            steps.append(
                (
                    CreateClonedClientStep,
                    {
                        "host": host,
                        "mountpoint": mp,
                        "fs_name": fs.name,
                    },
                )
            )

        return steps


class CreateClonedClientStep(Step):
    """
    Create a cloned Client mount on server
    """

    def run(self, kwargs):
        host = kwargs["host"]
        fsname = kwargs["fs_name"]
        mp = kwargs["mountpoint"]

        self.invoke_rust_agent_expect_result(host, "ha_cloned_client_create", [fsname, mp])


class DeployHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "configured", "stopped")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Deploy Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["configure_hotpool"]

    def description(self):
        return help_text["configure_hotpool"]

    def get_deps(self):
        deps = []

        for comp in self.hotpool_configuration.get_components():
            deps.append(DependOn(comp, "configured", fix_state="configured"))

        return DependAll(deps)


class StartHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "stopped", "started")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Start Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["start_hotpool"]

    def description(self):
        return help_text["start_hotpool"]

    def get_deps(self):
        deps = []

        fs = self.hotpool_configuration.filesystem
        deps.append(DependOn(fs, "available", fix_state="stopped"))

        for comp in self.hotpool_configuration.get_components():
            deps.append(DependOn(comp, "configured"))

        return DependAll(deps)

    def get_steps(self):
        steps = []
        fs = self.hotpool_configuration.filesystem
        for host in (l[0] for l in fs.get_server_groups()):
            steps.append((StartResourceStep, {"host": host, "ha_label": self.hotpool_configuration.ha_label}))
            # @@ wait for component starts?

        return steps


class StopHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "started", "stopped")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Stop Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["stop_hotpool"]

    def description(self):
        return help_text["stop_hotpool"]

    def get_steps(self):
        steps = []
        fs = self.hotpool_configuration.filesystem

        for host in (l[0] for l in fs.get_server_groups()):
            steps.append((StopResourceStep, {"host": host, "ha_label": self.hotpool_configuration.ha_label}))

        return steps


class RemoveHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "stopped", "removed")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["stop_hotpool"]

    def description(self):
        return help_text["stop_hotpool"]

    def get_deps(self):
        deps = []

        for comp in self.hotpool_configuration.get_components():
            deps.append(DependOn(comp, "removed", fix_state="stopped"))

        return DependAll(deps)

    def get_steps(self):
        steps = []
        fs = self.hotpool_configuration.filesystem
        mp = self.hotpool_configuration.mountpoint
        for host in (l[0] for l in fs.get_server_groups()):
            steps.append((RemoveClonedClientStep, {"host": host, "fs_name": fs.name, "mountpoint": mp}))

        for host in fs.get_servers():
            steps.append((UnmountStep, {"host": host.fqdn, "mountpoint": mp}))
        return steps

    def on_success(self):
        self.hotpool_configuration.mark_deleted()
        super(RemoveHotpoolJob, self).on_success()


class RemoveClonedClientStep(Step):
    def run(self, kwargs):
        host = kwargs["host"]
        fsname = kwargs["fs_name"]
        mp = kwargs["mountpoint"]

        self.invoke_rust_agent_expect_result(host, "ha_cloned_client_destroy", fsname)


class RemoveConfiguredHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "configured", "removed")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["stop_hotpool"]

    def description(self):
        return help_text["stop_hotpool"]

    def get_deps(self):
        deps = []

        for comp in self.hotpool_configuration.get_components():
            deps.append(DependOn(comp, "removed", fix_state="configured"))

        return DependAll(deps)

    def get_steps(self):
        steps = []
        fs = self.hotpool_configuration.filesystem
        mp = self.hotpool_configuration.mountpoint
        for host in (l[0] for l in fs.get_server_groups()):
            steps.append((RemoveClonedClientStep, {"host": host, "fs_name": fs.name, "mountpoint": mp}))

        for host in fs.get_servers():
            steps.append((UnmountStep, {"host": host.fqdn, "mountpoint": mp}))

        return steps

    def on_success(self):
        self.hotpool_configuration.mark_deleted()
        super(RemoveConfiguredHotpoolJob, self).on_success()


class RemoveUnconfiguredHotpoolJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(HotpoolConfiguration, "unconfigured", "removed")
    stateful_object = "hotpool_configuration"
    hotpool_configuration = models.ForeignKey(HotpoolConfiguration, on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Hotpool"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["stop_hotpool"]

    def description(self):
        return help_text["stop_hotpool"]

    def get_deps(self):
        deps = []
        fs = self.hotpool_configuration.filesystem

        for comp in self.hotpool_configuration.get_components():
            deps.append(DependOn(comp, "removed", fix_state="unconfigured"))

        return DependAll(deps)

    def on_success(self):
        self.hotpool_configuration.mark_deleted()
        super(RemoveUnconfiguredHotpoolJob, self).on_success()


############################################################
# LAmigo
#


class LamigoConfiguration(models.Model):
    """
    Shared configuration for all lamigo instances for a given hotpool configutation
    """

    __metaclass__ = DeletableMetaclass

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    hotpool = models.ForeignKey("HotpoolConfiguration", null=False, on_delete=CASCADE)
    hot = models.ForeignKey("OstPool", related_name="hot", null=False, on_delete=CASCADE)
    cold = models.ForeignKey("OstPool", related_name="cold", null=False, on_delete=CASCADE)

    extend = models.ForeignKey("Task", related_name="extend", null=True, on_delete=CASCADE)
    resync = models.ForeignKey("Task", related_name="resync", null=True, on_delete=CASCADE)

    minage = models.IntegerField(help_text="Seconds to wait after close to sync to cold pool", null=False)

    def lamigo_config(self, mdt, user):
        # C.F. iml-agent::action_plugins::lamigo::Config
        config = {
            "mdt": mdt.index,
            "fs": self.hotpool.filesystem.name,
            "user": user,
            "min_age": self.minage,
            "mountpoint": self.hotpool.mountpoint,
            "mailbox_extend": self.extend.name,
            "mailbox_resync": self.resync.name,
            "hot_pool": self.hot.name,
            "cold_pool": self.cold.name,
        }
        return config


class Lamigo(StatefulObject):
    """
    lamigo instance for a given MDT
    """

    states = ["unconfigured", "configured", "removed"]
    initial_state = "unconfigured"

    __metaclass__ = DeletableMetaclass

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    configuration = models.ForeignKey("LamigoConfiguration", null=False, on_delete=CASCADE)
    mdt = models.ForeignKey("ManagedMdt", null=False, on_delete=CASCADE)
    changelog_user = models.CharField(max_length=8, null=True, help_text="Name of changelog user for MDT")

    def lamigo_config(self):
        assert self.changelog_user is not None
        return self.configuration.lamigo_config(self.mdt, self.changelog_user)

    @property
    def unit_name(self):
        return "lamigo@" + self.mdt.name + ".service"

    @property
    def ha_label(self):
        return "lamigo-" + self.mdt.name


class ConfigureLamigoJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lamigo, "unconfigured", "configured")
    stateful_object = "lamigo"
    lamigo = models.ForeignKey("Lamigo", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Configure Lamigo"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["configure_hotpool"]

    def description(self):
        return help_text["configure_hotpool"]

    def get_deps(self):
        hp = self.lamigo.configuration.hotpool

        deps = [DependOn(hp, "configured", acceptable_states=["stopped", "started"])]

        return DependAll(deps)

    def get_steps(self):
        steps = []

        fs = self.lamigo.configuration.hotpool.filesystem

        if not self.lamigo.changelog_user:
            steps.append(
                (CreateChangelogUserStep, {"host": self.lamigo.mdt.active_host.fqdn, "lamigo_id": self.lamigo.id})
            )

        for mtm in self.lamigo.mdt.managedtargetmount_set.all():
            steps.append((ConfigureLamigoStep, {"host": mtm.host.fqdn, "lamigo_id": self.lamigo.id}))

        host = self.lamigo.mdt.active_host
        after = [self.lamigo.mdt.ha_label, self.lamigo.configuration.hotpool.ha_label]
        steps.append(
            (
                CreateSystemdResourceStep,
                {
                    "host": host.fqdn,
                    "unit": self.lamigo.unit_name,
                    "ha_label": self.lamigo.ha_label,
                    "monitor": "30s",
                    "start": "15m",
                    "after": after,
                    "with": [self.lamigo.mdt.ha_label],
                },
            )
        )

        return steps


class CreateChangelogUserStep(Step):
    idempotent = True
    database = True

    def run(self, kwargs):
        lamigo_id = kwargs["lamigo_id"]
        lamigo = ObjectCache.get_by_id(Lamigo, int(lamigo_id))
        host = kwargs["host"]
        # @@ ideally this should lock lamigo object
        # with lamigo._lock:
        if not lamigo.changelog_user:
            user = self.invoke_rust_agent_expect_result(
                host, "lctl", ["--device", lamigo.mdt.name, "changelog_register", "-n"]
            )
            lamigo.changelog_user = user.strip()
            lamigo.save()
            ObjectCache.update(lamigo)


class ConfigureLamigoStep(Step):
    idempotent = True
    database = True

    def run(self, kwargs):
        host = kwargs["host"]
        lamigo_id = kwargs["lamigo_id"]
        lamigo = ObjectCache.get_by_id(Lamigo, int(lamigo_id))

        self.invoke_rust_agent_expect_result(host, "create_lamigo_conf", lamigo.lamigo_config())


class RemoveLamigoJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lamigo, "configured", "removed")
    stateful_object = "lamigo"
    lamigo = models.ForeignKey("Lamigo", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Lamigo"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["destroy_hotpool"]

    def description(self):
        return help_text["destroy_hotpool"]

    def get_steps(self):
        host = self.lamigo.mdt.active_host
        after = [self.lamigo.mdt.ha_label, self.lamigo.configuration.hotpool.ha_label]

        steps = [
            (
                RemoveSystemdResourceStep,
                {
                    "host": host.fqdn,
                    "ha_label": self.lamigo.ha_label,
                    "after": after,
                    "with": [self.lamigo.mdt.ha_label],
                },
            )
        ]

        return steps

    def on_success(self):
        self.lamigo.mark_deleted()
        super(RemoveLamigoJob, self).on_success()


class RemoveUnconfiguredLamigoJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lamigo, "unconfigured", "removed")
    stateful_object = "lamigo"
    lamigo = models.ForeignKey("Lamigo", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Lamigo"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["destroy_hotpool"]

    def description(self):
        return help_text["destroy_hotpool"]

    def on_success(self):
        self.lamigo.mark_deleted()
        super(RemoveUnconfiguredLamigoJob, self).on_success()


############################################################
# LPurge
#


class LpurgeConfiguration(models.Model):
    """
    Shared configuration for all lpurge instances for a given hotpool configutation
    """

    __metaclass__ = DeletableMetaclass

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    hotpool = models.ForeignKey("HotpoolConfiguration", null=False, on_delete=CASCADE)
    cold = models.ForeignKey("OstPool", null=False, on_delete=CASCADE)

    purge = models.ForeignKey("Task", null=True, on_delete=CASCADE)

    freehi = models.PositiveSmallIntegerField(help_text="Percent of free space which causes purge to stop", null=False)
    freelo = models.PositiveSmallIntegerField(help_text="Percent of free space which causes purge to start", null=False)

    def lpurge_config(self, ost):
        # C.F. iml-agent::action_plugins::lpurge::Config
        config = {
            "ost": ost.index,
            "fs": self.hotpool.filesystem.name,
            "mailbox": self.purge.name,
            "pool": self.cold.name,
            "freehi": self.freehi,
            "freelo": self.freelo,
        }
        return config


class Lpurge(StatefulObject):
    """
    lpurge instance for a given OST
    """

    states = ["unconfigured", "configured", "removed"]
    initial_state = "unconfigured"

    __metaclass__ = DeletableMetaclass

    configuration = models.ForeignKey("LpurgeConfiguration", null=False, on_delete=CASCADE)
    ost = models.ForeignKey("ManagedOst", null=False, on_delete=CASCADE)

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def lpurge_config(self):
        return self.configuration.lpurge_config(self.ost)

    @property
    def unit_name(self):
        return "lpurge@" + self.ost.name + ".service"

    @property
    def ha_label(self):
        return "lpurge-" + self.ost.name


class ConfigureLpurgeJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lpurge, "unconfigured", "configured")
    stateful_object = "lpurge"
    lpurge = models.ForeignKey("Lpurge", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Configure Lpurge"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["configure_hotpool"]

    def description(self):
        return help_text["configure_hotpool"]

    def get_deps(self):
        hp = self.lpurge.configuration.hotpool

        deps = [DependOn(hp, "configured", acceptable_states=["stopped", "started"])]

        return DependAll(deps)

    def get_steps(self):
        steps = []

        fs = self.lpurge.configuration.hotpool.filesystem

        for mtm in self.lpurge.ost.managedtargetmount_set.all():
            steps.append((ConfigureLpurgeStep, {"host": mtm.host.fqdn, "lpurge_conf": self.lpurge.lpurge_config()}))

        host = self.lpurge.ost.active_host
        after = [self.lpurge.ost.ha_label, self.lpurge.configuration.hotpool.ha_label]
        steps.append(
            (
                CreateSystemdResourceStep,
                {
                    "host": host.fqdn,
                    "unit": self.lpurge.unit_name,
                    "ha_label": self.lpurge.ha_label,
                    "monitor": "30s",
                    "after": after,
                    "with": [self.lpurge.ost.ha_label],
                },
            )
        )

        return steps


class ConfigureLpurgeStep(Step):
    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        conf = kwargs["lpurge_conf"]

        self.invoke_rust_agent_expect_result(host, "create_lpurge_conf", conf)


class RemoveLpurgeJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lpurge, "configured", "removed")
    stateful_object = "lpurge"
    lpurge = models.ForeignKey("Lpurge", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Lpurge"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["destroy_hotpool"]

    def description(self):
        return help_text["destroy_hotpool"]

    def get_steps(self):
        host = self.lpurge.ost.active_host
        after = [self.lpurge.ost.ha_label, self.lpurge.configuration.hotpool.ha_label]
        steps = [
            (
                RemoveSystemdResourceStep,
                {
                    "host": host.fqdn,
                    "ha_label": self.lpurge.ha_label,
                    "after": after,
                    "with": [self.lpurge.ost.ha_label],
                },
            )
        ]

        return steps

    def on_success(self):
        self.lpurge.mark_deleted()
        super(RemoveLpurgeJob, self).on_success()


class RemoveUnconfiguredLpurgeJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Lpurge, "unconfigured", "removed")
    stateful_object = "lpurge"
    lpurge = models.ForeignKey("Lpurge", on_delete=CASCADE)

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 10

    requires_confirmation = False
    state_verb = "Remove Lpurge"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["destroy_hotpool"]

    def description(self):
        return help_text["destroy_hotpool"]

    def on_success(self):
        self.lpurge.mark_deleted()
        super(RemoveUnconfiguredLpurgeJob, self).on_success()
