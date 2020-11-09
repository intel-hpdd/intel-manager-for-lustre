# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


"""
The service `job_scheduler` handles both RPCs (JobSchedulerRpc) and a queue (NotificationQueue).
The RPCs are used for explicit requests to modify the system or run a particular task, while the queue
is used for updates received from agent reports.  Access to both of these, along with some additional
non-remote functionality is wrapped in JobSchedulerClient.

"""

from django import db

from chroma_core.services import log_register
from chroma_core.services.rpc import ServiceRpcInterface
from chroma_core.models import ManagedHost, Command


log = log_register(__name__)


class JobSchedulerRpc(ServiceRpcInterface):
    methods = [
        "set_state",
        "run_jobs",
        "cancel_job",
        "create_host_ssh",
        "test_host_contact",
        "create_filesystem",
        "create_ostpool",
        "update_ostpool",
        "delete_ostpool",
        "create_client_mount",
        "create_copytool",
        "register_copytool",
        "unregister_copytool",
        "update_nids",
        "trigger_plugin_update",
        "update_lnet_configuration",
        "create_host",
        "create_targets",
        "available_transitions",
        "available_jobs",
        "get_locks",
        "update_corosync_configuration",
        "get_transition_consequences",
        "configure_stratagem",
        "update_stratagem",
        "create_hotpool",
        "remove_hotpool",
    ]


class JobSchedulerClient(object):
    """Because there are some tasks which are the domain of the job scheduler but do not need to
    be run in the context of the service, the RPCs and queue operations are accompanied in this
    class by some operations that run locally.  The local operations are
    read-only operations such as querying what operations are possible for a particular object.

    """

    @classmethod
    def command_run_jobs(cls, job_dicts, message):
        """Create and run some Jobs, within a single Command.

        :param job_dicts: List of 1 or more dicts like {'class_name': 'MyJobClass', 'args': {<dict of arguments to Job constructor>}}
        :param message: User-visible string describing the operation, e.g. "Detecting filesystems"
        :return: The ID of a new Command

        """
        return JobSchedulerRpc().run_jobs(job_dicts, message)

    @classmethod
    def command_set_state(cls, object_ids, message, run=True):
        """Modify the system in whatever way is necessary to reach the state
        specified in `object_ids`.  Creates Jobs under a single Command.  May create
        no Jobs if the system is already in the state, or already scheduled to be
        in that state.  If the system is already scheduled to be in that state, then
        the returned Command will be connected to the existing Jobs which take the system to
        the desired state.

        :param cls:
        :param object_ids: List of three-tuples (natural_key, object_id, new_state)
        :param message: User-visible string describing the operation, e.g. "Starting filesystem X"
        :param run: Test only.  Schedule jobs without starting them.
        :return: The ID of a new Command

        """
        return JobSchedulerRpc().set_state(object_ids, message, run)

    @classmethod
    def available_transitions(cls, object_list):
        """Return the transitions available for each object in list

        See the Job Scheduler method of the same name for details.
        """

        return JobSchedulerRpc().available_transitions(object_list)

    @classmethod
    def available_jobs(cls, object_list):
        """Query which jobs (other than changes to state) can be run on this object.

        See the Job Scheduler method of the same name for details.
        """

        return JobSchedulerRpc().available_jobs(object_list)

    @classmethod
    def get_transition_consequences(cls, stateful_object, new_state):
        """Query what the side effects of a state transition are.  Effectively does
        a dry run of scheduling jobs for the transition.

        The return format is like this:
        ::

            {
                'transition_job': <job dict>,
                'dependency_jobs': [<list of job dicts>]
            }
            # where each job dict is like
            {
                'class': '<job class name>',
                'requires_confirmation': <boolean, whether to prompt for confirmation>,
                'confirmation_prompt': <string, confirmation prompt>,
                'description': <string, description of the job>,
                'stateful_object_id': <ID of the object modified by this job>,
                'stateful_object_content_type_id': <Content type ID of the object modified by this job>
            }

        :param stateful_object: A StatefulObject instance
        :param new_state: Hypothetical new value of the 'state' attribute

        """
        return JobSchedulerRpc().get_transition_consequences(
            stateful_object.__class__.__name__, stateful_object.id, new_state
        )

    @classmethod
    def cancel_job(cls, job_id):
        """Attempt to cancel a job which is already scheduled (and possibly running)

        :param job_id: ID of a Job object
        """
        JobSchedulerRpc().cancel_job(job_id)

    @classmethod
    def create_host_ssh(cls, address, server_profile, root_pw, pkey, pkey_pw):
        """
        Create a host which will be set up using SSH

        :param address: SSH address
        :return: (<ManagedHost instance>, <Command instance>)
        """
        host_id, command_id = JobSchedulerRpc().create_host_ssh(address, server_profile, root_pw, pkey, pkey_pw)
        return (ManagedHost.objects.get(pk=host_id), Command.objects.get(pk=command_id))

    @classmethod
    def test_host_contact(cls, address, root_pw=None, pkey=None, pkey_pw=None):
        command_id = JobSchedulerRpc().test_host_contact(address, root_pw, pkey, pkey_pw)

        return Command.objects.get(pk=command_id)

    @classmethod
    def update_corosync_configuration(cls, corosync_configuration_id, mcast_port, network_interface_ids):
        command_id = JobSchedulerRpc().update_corosync_configuration(
            corosync_configuration_id, mcast_port, network_interface_ids
        )

        return Command.objects.get(pk=command_id)

    @classmethod
    def create_filesystem(cls, fs_data):
        return JobSchedulerRpc().create_filesystem(fs_data)

    @classmethod
    def create_ostpool(cls, pool_data):
        return JobSchedulerRpc().create_ostpool(pool_data)

    @classmethod
    def update_ostpool(cls, pool_data):
        return JobSchedulerRpc().update_ostpool(pool_data)

    @classmethod
    def delete_ostpool(cls, pool):
        return JobSchedulerRpc().delete_ostpool(pool)

    @classmethod
    def update_nids(cls, nid_data):
        return JobSchedulerRpc().update_nids(nid_data)

    @classmethod
    def trigger_plugin_update(cls, include_host_ids, exclude_host_ids, plugin_names):
        """
        Cause the plugins on the hosts passed to send an update irrespective of whether any
        changes have occurred.

        :param include_host_ids: List of host ids to include in the trigger update.
        :param exclude_host_ids: List of host ids to exclude from the include list (makes for usage easy)
        :param plugin_names: list of plugins to trigger update on - empty list means all.
        :return: command id that caused updates to be sent.
        """
        assert isinstance(include_host_ids, list)
        assert isinstance(exclude_host_ids, list)
        assert isinstance(plugin_names, list)

        return JobSchedulerRpc().trigger_plugin_update(include_host_ids, exclude_host_ids, plugin_names)

    @classmethod
    def update_lnet_configuration(cls, lnet_configuration_list):
        return JobSchedulerRpc().update_lnet_configuration(lnet_configuration_list)

    @classmethod
    def create_host(cls, fqdn, nodename, address, server_profile_id):
        # The address of a host isn't something we can learn from it (the
        # address is specifically how the host is to be reached from the manager
        # for outbound connections, not just its FQDN).  If during creation we know
        # the address, then great, accept it.  Else default to FQDN, it's a reasonable guess.
        if address is None:
            address = fqdn

        host_id, command_id = JobSchedulerRpc().create_host(fqdn, nodename, address, server_profile_id)

        return (ManagedHost.objects.get(pk=host_id), Command.objects.get(pk=command_id))

    @classmethod
    def create_targets(cls, targets_data):
        from chroma_core.models import ManagedTarget, Command

        target_ids, command_id = JobSchedulerRpc().create_targets(targets_data)
        return (list(ManagedTarget.objects.filter(id__in=target_ids)), Command.objects.get(pk=command_id))

    @classmethod
    def create_client_mount(cls, host, filesystem_name, mountpoint):
        from chroma_core.models import LustreClientMount

        client_mount_id = JobSchedulerRpc().create_client_mount(host.id, filesystem_name, mountpoint)
        return LustreClientMount.objects.get(id=client_mount_id)

    @classmethod
    def create_copytool(cls, copytool_data):
        from chroma_core.models import Copytool

        copytool_id = JobSchedulerRpc().create_copytool(copytool_data)
        return Copytool.objects.get(id=copytool_id)

    @classmethod
    def register_copytool(cls, copytool_id, uuid):
        JobSchedulerRpc().register_copytool(copytool_id, uuid)

    @classmethod
    def unregister_copytool(cls, copytool_id):
        JobSchedulerRpc().unregister_copytool(copytool_id)

    @classmethod
    def get_locks(cls):
        return JobSchedulerRpc().get_locks()

    @classmethod
    def configure_stratagem(cls, stratagem_data):
        return JobSchedulerRpc().configure_stratagem(stratagem_data)

    @classmethod
    def update_stratagem(cls, stratagem_data):
        return JobSchedulerRpc().update_stratagem(stratagem_data)

    @classmethod
    def create_hotpool(cls, data):
        return JobSchedulerRpc().create_hotpool(data)

    @classmethod
    def remove_hotpool(cls, data):
        return JobSchedulerRpc().remove_hotpool(data)
