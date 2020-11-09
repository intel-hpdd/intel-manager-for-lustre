# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import logging
import operator
from django.db import models, transaction

from polymorphic.models import DowncastManager
from polymorphic.models import PolymorphicMetaclass

from chroma_core.lib.job import Step

#  Convert dict used by models and apis
STR_TO_SEVERITY = dict(
    [
        (logging.getLevelName(level), level)
        for level in [logging.INFO, logging.ERROR, logging.CRITICAL, logging.WARNING, logging.DEBUG]
    ]
)

# Default django max_length for CharField
CHARFIELD_MAX_LENGTH = 1024


class DeletableDowncastableManager(DowncastManager):
    """Filters results to return only not-deleted records"""

    def get_queryset(self):
        return super(DeletableDowncastableManager, self).get_queryset().filter(not_deleted=True)

    def get_query_set_with_deleted(self):
        return super(DeletableDowncastableManager, self).get_queryset()


class DeletableManager(models.Manager):
    """Filters results to return only not-deleted records"""

    def get_queryset(self):
        return super(DeletableManager, self).get_queryset().filter(not_deleted=True)

    def get_query_set_with_deleted(self):
        return super(DeletableManager, self).get_queryset()


def _make_deletable(metaclass, dct):
    def mark_deleted(self):
        self._mark_deleted()

    def _mark_deleted(self):
        """Mark a record as deleted, returns nothing.

        Looks up the model instance by pk, sets the not_deleted attribute
        to None and saves the model instance.

        Additionally marks any AlertStates referring to this item as inactive.

        This is provided as a class method which takes an ID rather than as an
        instance method, in order to use ._base_manager rather than .objects -- this
        allows us to find the object even if it was already deleted, making this
        operation idempotent rather than throwing a DoesNotExist on the second try.
        """
        # Not implemented as an instance method because
        # we will need to use _base_manager to ensure
        # we can get at the object
        from django.db.models import signals

        signals.pre_delete.send(sender=self.__class__, instance=self)

        with transaction.atomic():
            if self.not_deleted:
                self.not_deleted = None
                self.save()

            from chroma_core.lib.job import job_log
            from chroma_core.models.alert import AlertState

            updated = AlertState.filter_by_item_id(self.__class__, self.id).update(active=None)
            job_log.info("Lowered %d alerts while deleting %s %s" % (updated, self.__class__, self.id))

        signals.post_delete.send(sender=self.__class__, instance=self)

    def delete(self):
        raise NotImplementedError("Must use .mark_deleted on Deletable objects")

    dct["objects"] = DeletableManager()
    dct["delete"] = delete
    dct["mark_deleted"] = mark_deleted
    dct["_mark_deleted"] = _mark_deleted
    # Conditional to only create the 'deleted' attribute on the immediate
    # user of the metaclass, not again on subclasses.
    if issubclass(dct.get("__metaclass__", type), metaclass):
        # Please forgive me.  Logically this would be a field called 'deleted' which would
        # be True or False.  Instead, it is a field called 'not_deleted' which can be
        # True or None.  The reason is: unique_together constraints.
        dct["not_deleted"] = models.NullBooleanField(default=True)

    if "Meta" in dct:
        if hasattr(dct["Meta"], "unique_together"):
            if not "not_deleted" in dct["Meta"].unique_together:
                dct["Meta"].unique_together = dct["Meta"].unique_together + ("not_deleted",)


class DeletableMetaclass(models.base.ModelBase):
    """Make a django model 'deletable', such that the default delete() method
    (an SQL DELETE) is replaced with something which keeps the record in the
    database but will hide it from future queries.

    The not_deleted attribute is logically a True/False 'deleted' attribute, but is
    implemented this (admittedly ugly) way in order to work with django's
    unique_together option.  When 'not_deleted' is included in the unique_together
    tuple, the uniqeness constraint is applied only to objects which have not
    been deleted -- e.g. an MGS can only have one filesystem with a given name, but
    once you've deleted that filesystem you should be able to create more with the
    same name.
    """

    def __new__(cls, name, bases, dct):
        _make_deletable(cls, dct)
        return super(DeletableMetaclass, cls).__new__(cls, name, bases, dct)


class DeletableDowncastableMetaclass(PolymorphicMetaclass):
    """Make a django model 'downcastable and 'deletable'.

    This combines the DowncastMetaclass behavior with the not_deleted attribute,
    the delete() method and the DeletableDowncastableManager.

    The not_deleted attribute is logically a True/False 'deleted' attribute, but is
    implemented this (admittedly ugly) way in order to work with django's
    unique_together option.  When 'not_deleted' is included in the unique_together
    tuple, the uniqueness constraint is applied only to objects which have not
    been deleted -- e.g. an MGS can only have one filesystem with a given name, but
    once you've deleted that filesystem you should be able to create more with the
    same name.

    The manager we set is a subclass of polymorphic.models.DowncastManager, so we inherit the
    full DowncastMetaclass behaviour.
    """

    def __new__(cls, name, bases, dct):
        _make_deletable(cls, dct)
        return super(DeletableDowncastableMetaclass, cls).__new__(cls, name, bases, dct)


class Version(tuple):
    "Version string as a comparable tuple, similar to sys.version_info."

    def __new__(cls, version):
        return tuple.__new__(cls, (int(component) for component in (version or "").split(".") if component.isdigit()))

    major, minor = (property(operator.itemgetter(index)) for index in range(2))


def get_all_sub_classes(cls):
    subclasses = set(cls.__subclasses__())

    for c in subclasses:
        subclasses.union(set(get_all_sub_classes(c)))

    return subclasses


class StartResourceStep(Step):
    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        label = kwargs["ha_label"]
        self.invoke_rust_agent_expect_result(host, "ha_resource_start", label)


class StopResourceStep(Step):
    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        label = kwargs["ha_label"]
        self.invoke_rust_agent_expect_result(host, "ha_resource_stop", label)


class CreateSystemdResourceStep(Step):
    """
    Create systemd based pacemaker resource
    Created resource will be "Started" after constraints are added
    Args:
    * host - fqdn to run on
    * unit - systemd unit name
    * ha_label - ha_label for unit
    * start - (optional) start timeout
    * stop - (optional) stop timeout
    * monitor - (optional) monitor interval
    * after - (optional) array of ha_labels to set order contraint to run after
    * with - (optional) array of ha_labels to set colocation constriant
    """

    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        unit = kwargs["unit"]
        ha_label = kwargs["ha_label"]
        start = kwargs.get("start")
        stop = kwargs.get("stop")
        monitor = kwargs.get("monitor")
        after = kwargs.get("after", [])
        coloc = kwargs.get("with", [])

        # c.f. iml-wire-types::ResourceAgentInfo
        agent = {
            "agent": {"standard": "systemd", "ocftype": unit},
            "id": ha_label,
            "args": {},
            "ops": {"start": start, "stop": stop, "monitor": monitor},
        }
        constraints = [{"ORDER": {"id": ha_label + "-after-" + r, "first": r, "then": ha_label}} for r in after]
        constraints.extend(
            [
                {"COLOCATION": {"id": ha_label + "-with-" + r, "rsc": ha_label, "with_rsc": r, "score": "INFINITY"}}
                for r in coloc
            ]
        )
        self.invoke_rust_agent_expect_result(host, "ha_resource_create", [agent, constraints])


class RemoveSystemdResourceStep(Step):
    """
    Create systemd based pacemaker resource
    Created resource will be "Started" after constraints are added
    Args:
    * host - fqdn to run on
    * ha_label - ha_label for unit
    * after - (optional) array of ha_labels to set order contraint to run after
    * with - (optional) array of ha_labels to set colocation constriant
    """

    def run(self, kwargs):
        host = kwargs["host"]
        ha_label = kwargs["ha_label"]
        after = kwargs.get("after", [])
        coloc = kwargs.get("with", [])

        constraints = [ha_label + "-after-" + r for r in after]
        constraints.extend([ha_label + "-with-" + r for r in coloc])

        self.invoke_rust_agent_expect_result(host, "ha_resource_destroy", [ha_label, constraints])


class RemoveResourceStep(Step):
    """
    Remove pacemake resource
    Args:
    * Host - fqdn to run on
    * ha_label - resource to remove
    """

    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        label = kwargs["ha_label"]
        self.invoke_rust_agent_expect_result(host, "ha_resource_destroy", [label, []])


class MountStep(Step):
    """
    Mount and add to fstab
    Args:
    * host - fqdn
    * mountpoint - Mount point
    * spec - Mount Spec
    * auto - (optional def: True) auto mount at boot
    """

    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        persist = kwargs.get("auto", True)
        mp = kwargs["mountpoint"]
        spec = kwargs["spec"]

        # cf. iml-wire-types::client::Mount
        mount = {"persist": persist, "mountspec": spec, "mountpoint": mp}
        self.invoke_rust_agent_expect_result(host, "mount", mount)


class UnmountStep(Step):
    """
    Unmount and remove from fstab
    Args:
    * host - fqdn
    * mountpoint - Mount Point
    * spec - (optional) Mount Spec
    """

    idempotent = True

    def run(self, kwargs):
        host = kwargs["host"]
        mp = kwargs["mountpoint"]
        spec = kwargs.get("spec", "")
        # cf. iml-wire-types::client::Unmount
        unmount = {"mountpoint": mp, "mountspec": spec}

        # unmount call techincally doesn't need the mountspec, and only matches on mountpoint
        self.invoke_rust_agent_expect_result(host, "unmount", unmount)
