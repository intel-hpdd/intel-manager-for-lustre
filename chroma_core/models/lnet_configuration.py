# -*- coding: utf-8 -*-
# Copyright (c) 2018 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

import json
import logging

from django.db import models

from django.core.exceptions import ObjectDoesNotExist

from chroma_core.models import AlertStateBase
from chroma_core.models import AlertEvent
from chroma_core.models import DeletableStatefulObject
from chroma_core.models import StateChangeJob
from chroma_core.models import NullStateChangeJob
from chroma_core.models import Nid
from chroma_core.models import Job, StateLock
from chroma_core.models import NetworkInterface
from chroma_core.lib.job import DependOn, DependAll, Step
from chroma_help.help import help_text


class LNetConfiguration(DeletableStatefulObject):
    states = ["unconfigured", "lnet_unloaded", "lnet_down", "lnet_up"]
    initial_state = "unconfigured"

    host = models.OneToOneField("ManagedHost", related_name="lnet_configuration")

    def get_nids(self):
        return [n.nid_string for n in self.nid_set.all()]

    def __str__(self):
        return "%s LNet configuration" % self.host

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def get_label(self):
        return "lnet configuration"

    def set_state(self, state, intentional=False):
        """
        :param intentional: set to true to silence any alerts generated by this transition
        """
        super(LNetConfiguration, self).set_state(state, intentional)
        if intentional:
            LNetOfflineAlert.notify_warning(self, self.state != "lnet_up")
        else:
            LNetOfflineAlert.notify(self, self.state != "lnet_up")

    reverse_deps = {"ManagedHost": lambda mh: LNetConfiguration.objects.filter(host_id=mh.id)}

    @property
    def is_managed(self):
        """
        :return: True if the lnet_configuration is managed, as a proxy we just use state of the host to decide.
        """
        return self.host.is_managed

    def filter_steps(self, steps):
        """
        Simple helper, if the lnet_configuration is not managed then we never do anything and so no steps.

        This can be used as a safety net as well by using it on steps that should never be requested by for
        monitor mode - just to be sure they are not.

        :param steps: steps to filter
        :return: filtered steps
        """
        return steps if self.is_managed else []


class LNetOfflineAlert(AlertStateBase):
    # LNET being offline is never solely responsible for a filesystem
    # being unavailable: if a target is offline we will get a separate
    # ERROR alert for that.  LNET being offline may indicate a configuration
    # fault, but equally could just indicate that the host hasn't booted up that far yet.
    default_severity = logging.INFO

    def alert_message(self):
        return "LNet offline on server %s" % self.alert_item

    class Meta:
        app_label = "chroma_core"
        db_table = AlertStateBase.table_name

    def end_event(self):
        return AlertEvent(
            message_str="LNet started on server '%s'" % self.alert_item.host,
            alert_item=self.alert_item.host,
            alert=self,
            severity=logging.WARNING,
        )

    @property
    def affected_objects(self):
        """
        :return: A list of objects that are affected by this alert
        """
        return [self.alert_item.host]


class LNetNidsChangedAlert(AlertStateBase):
    # This is WARNING because targets on this host will not work
    # correctly until it is addressed, but the filesystem may still
    # be available if a failover server is not in this condition.
    default_severity = logging.WARNING

    def alert_message(self):
        msg = "NIDs changed on server %s - see manual for details."
        return msg % self.alert_item

    class Meta:
        app_label = "chroma_core"
        db_table = AlertStateBase.table_name

    def end_event(self):
        return AlertEvent(
            message_str="LNet NIDs updated for server %s" % self.alert_item,
            alert_item=self.alert_item,
            alert=self,
            severity=logging.INFO,
        )

    @property
    def affected_objects(self):
        """
        :return: A list of objects that are affected by this alert
        """
        return [self.alert_item.lnet_configuration]


class LNetStateChangeJob(StateChangeJob):
    """
    Simple class to allow us to have one place for the standard parts of LNet StateChangeJobs
    """

    class Meta:
        abstract = True

    @classmethod
    def can_run(cls, lnet_configuration):
        return lnet_configuration.is_managed


class ConfigureLNetStep(Step):
    idempotent = True

    # Truth be told the database acceses should be in the job, but for know they are in the step.
    database = True

    def run(self, kwargs):
        host = kwargs["host"]
        nid_updates = kwargs["config_changes"]["nid_updates"]
        nid_deletes = kwargs["config_changes"]["nid_deletes"]

        modprobe_entries = []
        nid_tuples = []

        network_interfaces = NetworkInterface.objects.filter(host=host)
        lnet_configuration = LNetConfiguration.objects.get(host=host)

        for network_interface in network_interfaces:
            # See if we have deleted the nid for this network interface or
            # see if we have a new configuration for this if we do then it
            # will replace the current configuration.
            #
            # The int will have become a string - we should use a PickledObjectField really.
            if str(network_interface.id) in nid_deletes:
                nid = None
            elif str(network_interface.id) in nid_updates:
                nid = Nid(
                    network_interface=network_interface,
                    lnet_configuration=lnet_configuration,
                    lnd_network=nid_updates[str(network_interface.id)]["lnd_network"],
                    lnd_type=nid_updates[str(network_interface.id)]["lnd_type"],
                )
            else:
                try:
                    nid = Nid.objects.get(network_interface=network_interface)
                except ObjectDoesNotExist:
                    nid = None
                    pass

            if nid is not None:
                modprobe_entries.append(nid.modprobe_entry)
                nid_tuples.append(nid.to_tuple)

        self.invoke_agent_expect_result(
            host,
            "configure_lnet",
            {
                "lnet_configuration": {
                    "state": lnet_configuration.state,
                    "modprobe_entries": modprobe_entries,
                    "network_interfaces": nid_tuples,
                }
            },
        )


class ConfigureLNetJob(Job):
    lnet_configuration = models.ForeignKey(LNetConfiguration)
    config_changes = models.CharField(max_length=4096, help_text="A json string describing the configuration changes")
    requires_confirmation = False
    state_verb = "Configure LNet"

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def create_locks(self):
        return [StateLock(job=self, locked_item=self.lnet_configuration, write=True)]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["configure_lnet"] % stateful_object.host

    def description(self):
        return self.long_description(self.lnet_configuration)

    def get_steps(self):
        # The get_deps means the lnet is always placed into the unloaded state in preparation for the change in
        # configure the next two steps cause lnet to return to the state it was in
        steps = [
            (
                ConfigureLNetStep,
                {"host": self.lnet_configuration.host, "config_changes": json.loads(self.config_changes)},
            )
        ]

        if self.lnet_configuration.state != "lnet_unloaded":
            steps.append((LoadLNetStep, {"host": self.lnet_configuration.host}))

        if self.lnet_configuration.state == "lnet_up":
            steps.append((StartLNetStep, {"host": self.lnet_configuration.host}))

        steps.append((GetLNetStateStep, {"host": self.lnet_configuration.host}))

        return self.lnet_configuration.filter_steps(steps)

    def get_deps(self):
        return DependOn(self.lnet_configuration, "lnet_unloaded")


class UnconfigureLNetStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_agent_expect_result(kwargs["host"], "unconfigure_lnet")


class UnconfigureLNetJob(NullStateChangeJob):
    target_object = models.ForeignKey(LNetConfiguration)
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "lnet_unloaded", "unconfigured")

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def description(self):
        return self.long_description(self.target_object)

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["Change lnet state of %s to unconfigured"] % stateful_object.host
        else:
            return help_text["Stop monitoring lnet on %s"] % stateful_object.host

    def get_steps(self):
        return self.target_object.filter_steps([(UnconfigureLNetStep, {"host": self.target_object.host})])


class EnableLNetJob(NullStateChangeJob):
    target_object = models.ForeignKey(LNetConfiguration)
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "unconfigured", "lnet_unloaded")

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def description(self):
        return self.long_description(self.target_object)

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["Enable LNet on %s"] % stateful_object.host
        else:
            return help_text["Start monitoring LNet on %s"] % stateful_object.host

    def get_deps(self):
        """
        Before LNet operations are possible some dependencies are need, basically the host must have had its packages installed.
        Maybe we need a packages object, but this routine at least keeps the detail in one place.

        Or maybe we need an unacceptable_states lists.
        :return:
        """
        if self.target_object.host.state in ["unconfigured", "undeployed"]:
            return DependOn(self.target_object.host, "packages_installed")
        else:
            return DependAll()


class LoadLNetStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_agent_expect_result(kwargs["host"], "load_lnet")


class LoadLNetJob(LNetStateChangeJob):
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "lnet_unloaded", "lnet_down")
    stateful_object = "lnet_configuration"
    lnet_configuration = models.ForeignKey(LNetConfiguration)
    state_verb = "Load LNet"

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 30

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["load_lnet"]
        else:
            return help_text["Start monitoring LNet on %s"] % stateful_object.host

    def description(self):
        return self.long_description(self.lnet_configuration)

    def get_steps(self):
        return self.lnet_configuration.filter_steps(
            [
                (LoadLNetStep, {"host": self.lnet_configuration.host}),
                (GetLNetStateStep, {"host": self.lnet_configuration.host}),
            ]
        )


class StartLNetStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_agent_expect_result(kwargs["host"], "start_lnet")


class StartLNetJob(LNetStateChangeJob):
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "lnet_down", "lnet_up")
    stateful_object = "lnet_configuration"
    lnet_configuration = models.ForeignKey(LNetConfiguration)
    state_verb = "Start LNet"

    display_group = Job.JOB_GROUPS.COMMON
    display_order = 40

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["start_lnet"]
        else:
            return help_text["Start monitoring LNet on %s"] % stateful_object.host

    def description(self):
        return self.long_description(self.lnet_configuration)

    def get_steps(self):
        return self.lnet_configuration.filter_steps(
            [
                (StartLNetStep, {"host": self.lnet_configuration.host}),
                (GetLNetStateStep, {"host": self.lnet_configuration.host}),
            ]
        )


class StopLNetStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_agent_expect_result(kwargs["host"], "stop_lnet")


class StopLNetJob(LNetStateChangeJob):
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "lnet_up", "lnet_down")
    stateful_object = "lnet_configuration"
    lnet_configuration = models.ForeignKey(LNetConfiguration)
    state_verb = "Stop LNet"

    display_group = Job.JOB_GROUPS.RARE
    display_order = 100

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["stop_lnet"]
        else:
            return help_text["Stop monitoring lnet on %s"] % stateful_object.host

    def description(self):
        return self.long_description(self.lnet_configuration)

    def get_steps(self):
        return self.lnet_configuration.filter_steps(
            [
                (StopLNetStep, {"host": self.lnet_configuration.host}),
                (GetLNetStateStep, {"host": self.lnet_configuration.host}),
            ]
        )


class UnloadLNetStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_agent_expect_result(kwargs["host"], "unload_lnet")


class UnloadLNetJob(LNetStateChangeJob):
    state_transition = StateChangeJob.StateTransition(LNetConfiguration, "lnet_down", "lnet_unloaded")
    stateful_object = "lnet_configuration"
    lnet_configuration = models.ForeignKey(LNetConfiguration)
    state_verb = "Unload LNet"

    display_group = Job.JOB_GROUPS.RARE
    display_order = 110

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        if stateful_object.is_managed:
            return help_text["unload_lnet"]
        else:
            return help_text["Stop monitoring lnet on %s"] % stateful_object.host

    def description(self):
        return self.long_description(self.lnet_configuration)

    def get_steps(self):
        return self.lnet_configuration.filter_steps(
            [
                (UnloadLNetStep, {"host": self.lnet_configuration.host}),
                (GetLNetStateStep, {"host": self.lnet_configuration.host}),
            ]
        )


class GetLNetStateStep(Step):
    idempotent = True

    # Require database to talk to plugin_manager
    database = True

    def run(self, kwargs):
        from chroma_core.services.plugin_runner.agent_daemon_interface import AgentDaemonRpcInterface
        from chroma_core.services.job_scheduler.agent_rpc import AgentException

        host = kwargs["host"]

        try:
            network_data = self.invoke_agent(host, "device_plugin", {"plugin": "linux_network"})["linux_network"]
            AgentDaemonRpcInterface().update_host_resources(host.id, {"linux_network": network_data})
        except TypeError:
            self.log("Data received from old client. Host %s state cannot be updated until agent is updated" % host)
        except AgentException as e:
            self.log("No data for plugin linux_network from host %s due to exception %s" % (host, e))


class GetLNetStateJob(Job):
    """ This is an unused historical class, we can't delete it because old commands may have referenced it
    so it will just sit here for ever more. Maybe someone clever can turn it into a useful class and use it
    for something else.
    """

    host = models.ForeignKey("ManagedHost")

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["lnet_state"]

    def description(self):
        return "Get LNet state for %s" % self.host
