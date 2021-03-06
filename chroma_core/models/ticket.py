# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

from django.db import models
from django.db.models import CASCADE
from chroma_core.lib.job import DependOn, DependAll, Step
from chroma_core.models import DeletableDowncastableMetaclass
from chroma_core.models import StatefulObject, StateChangeJob
from chroma_help.help import help_text


class Ticket(StatefulObject):
    __metaclass__ = DeletableDowncastableMetaclass

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    states = ["revoked", "granted", "forgotten"]
    initial_state = "revoked"

    ha_label = models.CharField(
        max_length=64, null=True, blank=True, help_text="Label used for HA layer; human readable but unique"
    )

    name = models.CharField(
        max_length=64,
        null=False,
        blank=False,
        help_text="Name of ticket",
    )

    resource_controlled = models.BooleanField(
        default=True, help_text="Ticket is controlled by a resources named in `ha_label`"
    )

    cluster_id = models.IntegerField(help_text="The cluster this ticket belongs to", null=True)

    @property
    def ticket(self):
        return Ticket.objects.get(id=self.id)

    def __str__(self):
        return self.name


class MasterTicket(Ticket):
    """
    Ticket that controls all other filesystem tickets and the MGS
    """

    mgs = models.ForeignKey("ManagedMgs", null=False, on_delete=CASCADE)

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def get_host(self):
        return self.mgs.best_available_host()


class FilesystemTicket(Ticket):
    """
    Ticket that controls a named filesystem

    """

    filesystem = models.ForeignKey("ManagedFilesystem", null=False, on_delete=CASCADE)

    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    def get_host(self):
        return self.filesystem.mgs.best_available_host()

    @classmethod
    def filter_by_fs(cls, fs):
        return FilesystemTicket.objects.filter(filesystem=fs)

    reverse_deps = {"ManagedFilesystem": lambda fs: FilesystemTicket.filter_by_fs(fs)}


class GrantRevokedTicketJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Ticket, "revoked", "granted")
    stateful_object = "ticket"
    state_verb = "Grant"

    ticket = models.ForeignKey("Ticket", on_delete=CASCADE)

    def get_steps(self):
        steps = []
        ticket = self.ticket.downcast()
        if ticket.resource_controlled:
            steps.append((StartResourceStep, {"fqdn": ticket.get_host().fqdn, "ha_label": ticket.ha_label}))
        else:
            raise RuntimeError("Ticket `%s' is not resource controlled" % self.ticket.name)

        return steps

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["grant_ticket"]

    def description(self):
        return "Grant ticket %s" % self.ticket.name

    def get_deps(self):
        ticket = self.ticket.downcast()

        if isinstance(ticket, FilesystemTicket):
            mt = MasterTicket.objects.filter(mgs=ticket.filesystem.mgs)[0]
            return DependOn(mt.ticket, "granted")

        return DependAll()


class RevokeGrantedTicketJob(StateChangeJob):
    state_transition = StateChangeJob.StateTransition(Ticket, "granted", "revoked")
    stateful_object = "ticket"
    state_verb = "Revoke"

    ticket = models.ForeignKey("Ticket", on_delete=CASCADE)

    def get_steps(self):
        steps = []
        ticket = self.ticket.downcast()

        if ticket.resource_controlled:
            steps.append((StopResourceStep, {"fqdn": ticket.get_host().fqdn, "ha_label": ticket.ha_label}))
        else:
            raise RuntimeError("Ticket `%s' is not resource controlled" % ticket.name)

        return steps

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["revoke_ticket"]

    def description(self):
        return "Revoke ticket %s" % self.ticket.name


class StartResourceStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_rust_agent_expect_result(kwargs["fqdn"], "ha_resource_start", kwargs["ha_label"])


class StopResourceStep(Step):
    idempotent = True

    def run(self, kwargs):
        self.invoke_rust_agent_expect_result(kwargs["fqdn"], "ha_resource_stop", kwargs["ha_label"])


class ForgetTicketJob(StateChangeJob):
    class Meta:
        app_label = "chroma_core"
        ordering = ["id"]

    state_transition = StateChangeJob.StateTransition(Ticket, ["granted", "revoked"], "forgotten")
    stateful_object = "ticket"
    state_verb = "Forget"
    ticket = models.ForeignKey(Ticket, on_delete=CASCADE)

    @classmethod
    def long_description(cls, stateful_object):
        return help_text["forget_ticket"]

    def description(self):
        return "Forget ticket %s" % self.ticket

    def on_success(self):
        self.ticket.mark_deleted()
        super(ForgetTicketJob, self).on_success()
