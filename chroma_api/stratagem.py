from django.db import models
from django.db.models import Q

from tastypie import fields
from tastypie.resources import Resource
from chroma_api.authentication import AnonymousAuthentication
from chroma_core.services.job_scheduler.job_scheduler_client import JobSchedulerClient
from tastypie.authorization import DjangoAuthorization
from tastypie.validation import Validation
from chroma_api.validation_utils import validate
from chroma_core.models import StratagemConfiguration, ManagedHost, ManagedMdt, ManagedTargetMount, ManagedFilesystem

from chroma_api.chroma_model_resource import ChromaModelResource


class RunStratagemValidation(Validation):
    def is_valid(self, bundle, request=None):
        if "filesystem" not in bundle.data:
            return {"__all__": "filesystem required when running stratagem."}
        else:
            fs_identifier = str(bundle.data.get("filesystem"))

            if not any(
                map(
                    lambda x: str(x.get("id")) == fs_identifier or str(x.get("name")) == fs_identifier,
                    ManagedFilesystem.objects.values("id", "name"),
                )
            ):
                return {"__all__": "Filesystem {} does not exist.".format(bundle.data.get("filesystem_id"))}

            fs_id = filter(
                lambda x: str(x.get("id")) == fs_identifier or str(x.get("name")) == fs_identifier,
                ManagedFilesystem.objects.values("id", "name"),
            )[0].get("id")

            # Each MDT associated with the fielsystem must be installed on a server that has the stratagem profile installed
            target_mount_ids = map(lambda mdt: mdt.active_mount_id, ManagedMdt.objects.filter(filesystem_id=fs_id))
            host_ids = map(
                lambda target_mount: target_mount.host_id, ManagedTargetMount.objects.filter(id__in=target_mount_ids)
            )
            installed_profiles = map(lambda host: host.server_profile_id, ManagedHost.objects.filter(id__in=host_ids))
            if all(map(lambda name: name == "stratagem_server", installed_profiles)):
                return {}
            else:
                return {"__all__": "'stratagem_servers' profile must be installed on all MDT servers."}


class StratagemConfigurationValidation(RunStratagemValidation):
    def is_valid(self, bundle, request=None):
        required_args = [
            "interval",
            "report_duration",
            "report_duration_active",
            "purge_duration",
            "purge_duration_active",
        ]

        difference = set(required_args) - set(bundle.data.keys())

        if len(difference) == 0:
            return super(StratagemConfigurationValidation, self).is_valid(bundle, request)
        else:
            return {"__all__": "Required fields are missing: {}".format(", ".join(difference))}


class StratagemConfigurationResource(ChromaModelResource):
    filesystem = fields.CharField(attribute="filesystem_id", null=False)
    interval = fields.IntegerField(attribute="interval", null=False)
    report_duration = fields.IntegerField(attribute="report_duration", null=False)
    report_duration_active = fields.BooleanField(attribute="report_duration_active", null=False)
    purge_duration = fields.IntegerField(attribute="purge_duration", null=False)
    purge_duration_active = fields.BooleanField(attribute="purge_duration_active", null=False)

    class Meta:
        resource_name = "stratagem_configuration"
        queryset = StratagemConfiguration.objects.all()
        authorization = DjangoAuthorization()
        authentication = AnonymousAuthentication()
        allowed_methods = ["get", "post"]
        validation = StratagemConfigurationValidation()

    @validate
    def obj_create(self, bundle, **kwargs):
        return JobSchedulerClient.configure_stratagem(bundle.data)


class RunStratagemResource(Resource):
    filesystem = fields.CharField(attribute="filesystem_id", null=False)

    class Meta:
        list_allowed_methods = ["post"]
        detail_allowed_methods = []
        resource_name = "run_stratagem"
        authorization = DjangoAuthorization()
        authentication = AnonymousAuthentication()
        object_class = dict
        validation = RunStratagemValidation()

    def get_resource_uri(self, bundle=None, url_name=None):
        return Resource.get_resource_uri(self)

    @validate
    def obj_create(self, bundle, **kwargs):
        fs_identifier = str(bundle.data.get("filesystem"))
        fs_id = filter(
            lambda x: str(x.get("id")) == fs_identifier or str(x.get("name")) == fs_identifier,
            ManagedFilesystem.objects.values("id", "name"),
        )[0].get("id")

        mdts = map(lambda mdt: mdt.id, ManagedMdt.objects.filter(filesystem_id=fs_id))
        return JobSchedulerClient.run_stratagem(mdts)
