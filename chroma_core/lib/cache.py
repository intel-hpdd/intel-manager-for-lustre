# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


from collections import defaultdict
from chroma_core.services import log_register


log = log_register(__name__)


class ObjectCache(object):
    instance = None

    def __init__(self):
        from chroma_core.models import ManagedFilesystem, ManagedHost, LNetConfiguration, LustreClientMount
        from chroma_core.models import PacemakerConfiguration, CorosyncConfiguration, Corosync2Configuration
        from chroma_core.models import ServerProfile
        from chroma_core.models import NTPConfiguration, StratagemConfiguration, Ticket
        from chroma_core.models.target import ManagedTarget
        from chroma_core.models.copytool import Copytool

        self.objects = defaultdict(dict)
        self.filter_args = {
            LNetConfiguration: {"host__not_deleted": True},
        }

        self._cached_models = [
            Copytool,
            Corosync2Configuration,
            CorosyncConfiguration,
            LNetConfiguration,
            LustreClientMount,
            ManagedFilesystem,
            ManagedHost,
            ManagedTarget,
            NTPConfiguration,
            PacemakerConfiguration,
            ServerProfile,
            StratagemConfiguration,
            Ticket,
        ]

        for klass in self._cached_models:
            self._add_to_cache(klass)

    def _add(self, klass, instance):
        assert instance.__class__ in self._cached_models

        log.debug("_add %s %s %s" % (instance.__class__, instance.id, id(instance)))

        self.objects[klass][instance.pk] = instance

    def _add_to_cache(self, klass, args={}):
        filter_args = self.filter_args.get(klass, {}).copy()
        filter_args.update(args)

        for obj in klass.objects.filter(**args):
            self._add(klass, obj)

    @classmethod
    def add(cls, klass, instance):
        cls.getInstance()._add(klass, instance)

    @classmethod
    def get(cls, klass, filter=None):
        assert klass in cls.getInstance()._cached_models
        return [o for o in cls.getInstance().objects[klass].values() if not filter or filter(o)]

    @classmethod
    def get_by_id(cls, klass, instance_id, fill_on_miss=False):
        assert klass in cls.getInstance()._cached_models

        try:
            return cls.getInstance().objects[klass][instance_id]
        except KeyError:
            if not fill_on_miss:
                raise klass.DoesNotExist()
            else:
                cls.getInstance()._add_to_cache(klass, {"id": instance_id})
                return cls.getInstance().objects[klass][instance_id]

    @classmethod
    def get_targets_by_filesystem(cls, filesystem_id):
        return cls.getInstance()._get_targets_by_filesystem(filesystem_id)

    @classmethod
    def fs_targets(cls, fs_id):
        from chroma_core.models import ManagedMgs

        targets = cls.getInstance()._get_targets_by_filesystem(fs_id)
        targets = [t for t in targets if not issubclass(t.downcast_class, ManagedMgs)]
        # log.debug("fs_targets: %s" % targets)
        return targets

    def _get_targets_by_filesystem(self, filesystem_id):
        from chroma_core.models import ManagedTarget, ManagedMdt, ManagedOst, ManagedFilesystem

        # FIXME: This is reasonably efficient but could be improved further by caching the filesystem membership of targets.
        targets = []
        mgs_id = self.get_by_id(ManagedFilesystem, filesystem_id, fill_on_miss=True).mgs_id
        targets.append(self.objects[ManagedTarget][mgs_id])

        targets.extend(
            [
                self.get_by_id(ManagedTarget, mdt["id"], fill_on_miss=True)
                for mdt in ManagedMdt.objects.filter(filesystem=filesystem_id).values("id")
            ]
        )
        targets.extend(
            [
                self.get_by_id(ManagedTarget, ost["id"], fill_on_miss=True)
                for ost in ManagedOst.objects.filter(filesystem=filesystem_id).values("id")
            ]
        )

        return targets

    @classmethod
    def get_one(cls, klass, filter=None, fill_on_miss=False):
        assert klass in cls.getInstance()._cached_models
        r = [o for o in cls.getInstance().objects[klass].values() if not filter or filter(o)]
        if len(r) > 1:
            raise klass.MultipleObjectsReturned
        elif not r:
            if not fill_on_miss:
                raise klass.DoesNotExist
            else:
                cls.getInstance()._add_to_cache(klass)
                return cls.get_one(klass, filter)
        else:
            return r[0]

    @classmethod
    def getInstance(cls):
        if not cls.instance:
            cls.instance = ObjectCache()
        return cls.instance

    @classmethod
    def clear(cls):
        log.info("clear")
        cls.instance = None

    @classmethod
    def client_mount_copytools(cls, cm_id):
        from chroma_core.models.client_mount import LustreClientMount
        from chroma_core.models.copytool import Copytool

        try:
            client_mount = cls.get_one(LustreClientMount, lambda ccm: ccm.id == cm_id)
            return cls.get(
                Copytool, lambda ct: (client_mount.host_id == ct.host_id and ct.mountpoint in client_mount.mountpoints)
            )
        except LustreClientMount.DoesNotExist:
            return []

    @classmethod
    def purge(cls, klass, filter):
        cls.getInstance().objects[klass] = dict(
            [(o.pk, o) for o in cls.getInstance().objects[klass].values() if not filter(o)]
        )

    def _update(self, obj):
        log.debug("update: %s %s" % (obj.__class__, obj.id))
        assert obj.__class__ in self._cached_models
        class_collection = self.objects[obj.__class__]
        if obj.pk in class_collection:
            try:
                fresh_instance = obj.__class__.objects.get(pk=obj.pk)
            except obj.__class__.DoesNotExist:
                return None
            else:
                class_collection[obj.pk] = fresh_instance
            return fresh_instance

    @classmethod
    def update(cls, obj):
        return cls.getInstance()._update(obj)
