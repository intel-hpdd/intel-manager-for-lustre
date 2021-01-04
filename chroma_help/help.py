# -*- coding: utf-8 -*-
# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


help_text = {
    "mount_lustre_filesystems": "Mount all associated Lustre filesystems on this worker.",
    "unmount_lustre_filesystems": "Unmount all associated Lustre filesystems on this worker.",
    "opening_lustre_firewall_port": "Opening Lustre firewall port (988:tcp) on %s",
    "mount_lustre_filesystem": "Mount a Lustre filesystem on this node.",
    "unmount_lustre_filesystem": "Unmount all known clients for the Lustre filesystem on this node.",
    "remove_lustre_client_mount": "Remove this Lustre client entry from the configuration.",
    "configure_stratagem": "Configure Stratagem scanning interval.",
    "configure_stratagem_long": "Configure Stratagem scanning interval.",
    "run_stratagem": "Stratagem: Scanning MDT {}",
    "run_stratagem_for_all": "Stratagem: Scanning all MDT's",
    "hsm_control_enabled": "Enable HSM coordinator for this filesystem.",
    "hsm_control_disabled": "Pause HSM coordinator for this filesystem. New requests will be scheduled when coordinator is enabled.",
    "hsm_control_shutdown": "Stop HSM coordinator for this filesystem. No new requests may be scheduled.",
    "advanced_settings": "<b>Use care when changing these parameters as they can significantly impact functionality or performance.</b> For help with these settings, contact your storage solution provider.",
    "bytes_per_inode": 'File system space (in bytes) per MDS inode. The default is 2048, meaning one MDS inode per each 2048 bytes of file system space. In the "Lustre Operations Manual", see Section 5.3.3: Setting the Number of Inodes for the MDS.',
    "commands": "Shows past and currently running commands that the manager is executing to perform tasks, such as formatting or starting a file system.",
    "command_detail": "View details about this command.",
    "detect_file_systems-dialog": "Ensure that all storage servers for mounted Lustre targets in the file system to be detected are up and running. Then select the storage servers (including passive failover servers) and click <b>Run</b> to scan them for existing Lustre targets.",
    "dismiss_message": "Acknowledge this message and move it to the history view.",
    "goto_dashboard": "Go to the Dashboard",
    "detect_file_systems-tooltip": "Detect an existing file system to be monitored at the manager GUI.",
    "inode_size": 'Size (in bytes) of the inodes used to store Lustre metadata on the MDS for each file. The default is 512 bytes. In the "Lustre Operations Manual", see 5.3.1: Setting the Number of Inodes for the MDS.',
    "force_remove": "<b> WARNING: This command is destructive.</b> This command should only be performed when the Remove command has been unsuccessful. This command will remove this server from the Integrated Manager for Lustre software configuration, but Integrated Manager for Lustre software software will not be removed from this server.  All targets that depend on this server will also be removed without any attempt to unconfigure them. To completely remove the Integrated Manager for Lustre software software from this server (allowing it to be added to another Lustre file system) you must first contact technical support. <b>You should only perform this command if this server is permanently unavailable, or has never been successfully deployed using Integrated Manager for Lustre software.</b>",
    "invoke_agent": "Indicates that the chroma-agent service can be accessed on this server.",
    "nids": "The Lustre network identifier(s) for the LNet network(s) to which this node belongs.",
    "ping": "Indicates if an ICMP ping from the server running Integrated Manager for Lustre, to the server, succeeded.",
    "type": "The type of storage device.",
    "managed_filesystem": "This file system is managed by Integrated Manager for Lustre",
    "monitored_filesystem": "This file system is monitored and may not be modified within Integrated Manager for Lustre",
    "setup_monitored_host": "Setup this monitored host",
    "setup_monitored_host_on": "Setup monitored host %s",
    "setup_managed_host": "Setup this managed host",
    "setup_managed_host_on": "Setup managed host %s",
    "setup_worker_host": "Setup this worker host",
    "setup_worker_host_on": "Setup worker host %s",
    "install_packages_on_host_long": "Install packages on this server",
    "continue_server_configuration": "Continue Server Configuration",
    "install_packages_on_host": "Install packages on server %s",
    "Change lnet state of %s to unconfigured": "Change lnet state of %s to unconfigured",
    "Stop monitoring lnet on %s": "Stop monitoring lnet on %s",
    "Enable LNet on %s": "Enable LNet on %s",
    "Start monitoring LNet on %s": "Start monitoring LNet on %s",
    "reboot_host": "Initiate a reboot on the host. Any HA-capable targets running on the host will be failed over to a peer. Non-HA-capable targets will be unavailable until the host has finished rebooting.",
    "shutdown_host": "Initiate an orderly shutdown on the host. Any HA-capable targets running on the host will be failed over to a peer. Non-HA-capable targets will be unavailable until the host has been restarted.",
    "poweron_host": "Switch on power to this server.",
    "poweroff_host": "Switch off power to this server. Any HA-capable targets running on the server will be failed over to a peer. Non-HA-capable targets will be unavailable until the server is turned on again.",
    "powercycle_host": "Switch the power to this server off and then back on again. Any HA-capable targets running on the server will be failed over to a peer. Non-HA-capable targets will be unavailable until the server has finished booting.",
    "configure_host_fencing": "Configure fencing agent on this host.",
    "remove_configured_server": "Remove this server. Any file systems or targets that rely on this server will also be removed.",
    "remove_monitored_configured_server": "Remove this server.",
    "remove_unconfigured_server": "Remove this unconfigured server.",
    "ssh_authentication_tooltip": "Choose a method to authenticate on the given hostname / PDSH expression.",
    "existing_keys_tooltip": "If the IML Manager servers are configured with a set of SSH keys, those keys will be used to register the new servers.",
    "root_password_tooltip": "Use standard password based auth, if you don't have SSH keys to use.",
    "another_key_tooltip": "A private key that is a pair with a public key on this new server, may be used. A passphrase can be supplied for an encrypted private key.",
    "root_password_input_tooltip": "The password of the root user.",
    "deploying_host": "Deploying agent to host %s",
    "validating_host": "Validation host configuration %s",
    "private_key_textarea_tooltip": "The private key for the public key on the servers.",
    "private_key_input_tooltip": "The passphrase for the private key.",
    "auth": "Indicates if the manager server was able to connect, via SSH, to the storage server using the supplied authentication credentials.",
    "reverse_ping": "Indicates if an ICMP ping from the storage server to manager server succeeded.",
    "resolve": "Indicates if a DNS lookup performed at the manager server, of the fully-qualified domain name (FQDN) of the storage server, succeeded.",
    "reverse_resolve": "Indicates if a DNS lookup by the storage server of the fully-qualified domain name (FQDN) of the manager server succeeded.",
    "hostname_valid": "Indicates if the self-reported hostname of the storage server is valid (resolves to a non-loopback address).",
    "fqdn_resolves": "Indicates if a DNS lookup at the manager server, of the self-reported fully-qualified domain name (FQDN) of the storage server, succeeded.",
    "fqdn_matches": "Indicates if there is match between the DNS lookup at the manager server of the user-supplied hostname, and the DNS lookup at the storage server of the self-reported fully-qualified domain name (FQDN) of the storage server.",
    "yum_valid_repos": "Indicates if the storage server's yum configuration has been found to be free of problematic and unsupported software repositories.",
    "yum_can_update": "Indicates if the storage server is able to access a repository of packages provided by the Linux distribution vendor. This requirement ensures that yum can satisfy vendor-provided dependencies and obtain vendor-provided software updates. Alternately, this check can be skipped if all repositories are disabled and the necessary rpms are already installed.",
    "openssl": "Indicates if OpenSSL is working as expected.",
    "rewrite_target_configuration-dialog": "Select all servers for which the NIDs were re-read by clicking the <strong>Rescan NIDs</strong> button.  Then click <b>Run</b> to rewrite the Lustre target configuration for targets associated with the selected servers.",
    "rewrite_target_configuration-tooltip": "Update each target with the current NID for the server with which it is associated.",
    "remove_target_XXXX_from_filesystem": "Remove target %s from the filesystem\n*** WARNING *** this is destructive to your filesystem and irreversible",
    "install_updates_dialog": "Installs updated software on the selected servers. </br> Select the servers to include in this update operation. Then, click Run to install the updated packages on those servers. <b>Any server that belongs to an active file system is not selectable.</b>",
    "install_updates_configuration-tooltip": "Install updated software on the selected servers.",
    "server_waiting_title": "Add Server - Loading",
    "server_waiting": "Please wait while the server data is processed.",
    "server_waiting_header": "Loading Server Data",
    "server_status_configured": "This server has been configured for use with the manager GUI.",
    "server_status_lnet_down": "The LNet kernel module is loaded, but LNet networking is not currently started on this server.",
    "server_status_lnet_unloaded": "The LNet kernel module is not currently loaded on this server.",
    "server_status_lnet_up": "LNet networking is started on this server.",
    "server_status_unconfigured": "This server has not yet been configured for use with the manager GUI.",
    "pdsh_placeholder": "Enter hostname / hostlist expression.",
    "state_changed": "Time at which the state last changed, either detected or as a result of user action.",
    "set_host_profile_on": "Setting host profile on %s",
    "status": "Indicates the status of high availability (HA) configuration for this volume (ha = available for HA, noha = not configured for HA).",
    "status_light": "Indicates current system health. <br /> Green: The file system is operating normally. <br />  Yellow: The system may be operating in a degraded mode. <br /> Red: This system may be down or is severely degraded. <br /> Click to view all system event and alert status messages. <br /> Count indicates the number of issues causing the status color.",
    "start_file_system": "Start the metadata and object storage targets so the file system can be mounted by clients.,",
    "stop_file_system": "Stop the metadata and object storage targets, thus making the file system unavailable to clients.",
    "remove_file_system": "Remove file system. This file system's contents will remain intact until its volumes are reused in another file system.",
    "lnet_state": "The status of the LNet networking layer on this server.",
    "load_lnet": "Load the LNet kernel modules.",
    "start_lnet": "Start the LNet networking layer.",
    "stop_lnet": "Shut down the LNet networking layer and stop any targets running on this server.",
    "unload_lnet": "If LNet is running, stop LNET and unload the LNet kernel module to ensure that it will be reloaded before any targets are started again.",
    "conflict_diff": "This row has changed from %(initial)s locally and %(remote)s remotely. Click to set value to %(remote)s",
    "local_diff": "This row has changed locally. Click to reset value to %(initial)s",
    "remote_diff": "This row has changed remotely. Click to set value to %(remote)s.",
    "configure_lnet": "Configure LNet for %s",
    "change_host_profile": "Changing host %s to profile %s",
    "change_host_state": "Changing host %s to state %s",
    "configure_lnet_not_allowed": "LNet can only be configured in managed mode.",
    "configure_corosync": "Configure Corosync on this host.",
    "configure_corosync_on": "Configure Corosync on %s.",
    "stop_corosync": "Stop Corosync on this host.",
    "start_corosync": "Start Corosync on this host.",
    "enable_corosync": "Enable Corosync on this host.",
    "unconfigure_corosync": "Unconfiguring Corosync",
    "configure_pacemaker": "Configure Pacemaker on this host.",
    "configure_pacemaker_on": "Configure Pacemaker on %s.",
    "unconfigure_pacemaker": "Unconfigure Pacemaker on this host.",
    "unconfigure_pacemaker_on": "Unconfigure Pacemaker on %s",
    "stop_pacemaker": "Stop Pacemaker on this host.",
    "stop_pacemaker_on": "Stop Pacemaker on %s.",
    "start_pacemaker": "Start Pacemaker on this host.",
    "start_pacemaker_on": "Start Pacemaker on %s.",
    "configure_ntp": "Configured the NTP Client on the host",
    "unconfigure_ntp": "Unconfigured the NTP Client on the host",
    "start_mdt": "Start the metadata target (MDT).",
    "stop_mdt": "Stop the MDT. When an MDT is stopped, the file system becomes unavailable until the MDT is started again. If an object reference is known to a client, the client can continue to access the object in the file system after the MDT is shut down, but will not be able to obtain new object references.",
    "remove_mdt": "Remove the MDT from the file system. This MDT will no longer be seen in the manager GUI. <strong>Caution</strong>: When an MDT is removed, file metadata stored on the MDT will no longer be accessible.<b> To preserve data, manually create a copy of the data elsewhere before removing the MDT.</b>",
    "start_mgt": "Start the management target (MGT).",
    "stop_mgt": "Stop the MGT. When an MGT is stopped, clients are unable to make new connections to file systems using the MGT. However, MDT(s) and OST(s) stay up if they have been started and can be stopped and restarted while the MGT is stopped.",
    "remove_mgt": "Remove this MGT. The contents will remain intact until the volume is reused for a new target.",
    "start_ost": "Start the object storage target (OST).",
    "stop_ost": "Stop the OST. When an OST is stopped, clients are unable to access the files stored on this OST.",
    "remove_ost": "Remove the OST from the file system. This OST will no longer be seen in the manager GUI. <strong>Caution</strong>: When an OST is removed, files stored on the OST will no longer be accessible.<b> To preserve data, manually create a copy of the data elsewhere before removing the OST.</b>",
    "volume_long": "Volumes (also called LUNs or block devices) are the underlying units of storage used to create Lustre file systems.  Each Lustre target corresponds to a single volume. If servers in the volume have been configured for high availability, primary and secondary servers can be designated for a Lustre target. Only volumes that are not already in use as Lustre targets or local file systems are shown. A volume may be accessible on one or more servers via different device nodes, and it may be accessible via multiple device nodes on the same host.",
    "volume_short": "A LUN or block device used as a metadata or object storage target in a Lustre file system.",
    "volume_status_configured-ha": "This volume is ready to be used for a high-availability (HA) Lustre target.",
    "volume_status_configured-noha": "This volume is ready to be used as a Lustre target, but is not configured for high availability.",
    "volume_status_unconfigured": "This volume cannot be used as a Lustre target until a primary server is selected.",
    "pacemaker_configuration_failed": "Pacemaker configuration failed",
    "no_dismiss_message_alert": "This alert relates to an active problem. When the problem is fixed you may dismiss.",
    "no_dismiss_message_command": "This command is incomplete. When it has completed you may dismiss.",
    "copyright_year": "2015",
    "dashboard_filter_type": "Choose to view servers or file systems.",
    "dashboard_filter_fs": "Choose a file system to view. To view a target for a file system, select a file system, and then a target.",
    "dashboard_filter_server": "Choose a server to view. To view a target for a server, select a server, and then a target.",
    "dashboard_filter_target": "Choose a target to view.",
    "update_conf_params": "Update Conf params.",
    "make_file_system_unavailable": "Make this file system unavailable.",
    "deploy_agent": "Deploy agent to host.",
    "deploy_failed_to_register_host": "Failed to register host %s: rc=%s\n'%s'\n'%s'",
    "deployed_agent_failed_to_contact_manager": "Deployed agent on %s failed to contact manager.",
    "detect_targets": "Scan for Lustre targets",
    "discovered_target": "Discovered %s '%s' on server %s",
    "discovered_no_new_target": "Discovered no new %ss",
    "discovered_no_new_filesystem": "Discovered no new filesystems",
    "found_no_primary_mount_point_for_target": "Found no primary mount point for target %s %s",
    "found_not_TYPE_for_filesystem": "Found no %ss for filesystem %s",
    "discovered_filesystem_with_n_MDTs_and_n_OSTs": "Discovered filesystem %s with %s MDTs and %s OSTs",
    "update_devices": "Update device info.",
    "update_packages": "Update packages.",
    "Trigger plugin poll for %s plugins": "Trigger plugin poll for %s plugins",
    "update_nids": "Update NIDs.",
    "configure_target": "Configure target mount points.",
    "remove_target_from_pacemaker_config": "Removing target %s from pacemaker configuration",
    "export_target_from_nodes": "Ensure target %s is available for use",
    "moving_target_to_node": "Moving target %s to node %s",
    "mounting_target_on_node": "Mounting target %s on node %s",
    "add_target_to_pacemaker_config": "Adding target %s to pacemaker configuration",
    "register_target": "Register target.",
    "format_target": "Format target.",
    "migrate_target": "Migrate target.",
    "failover_target": "Migrate target to another host",
    "continue_as_anonymous": "Click this link to continue as an anonymous user. This user has restricted privileges on how they can use the Integrated Manager for Lustre software.",
    "stonith_not_enabled": "stonith-enabled is false on %s. This can cause device corruption. Target creation is forbidden in this state. Ensure that stonith-enabled is set to true.",
    "stonith_enabled": "stonith-enabled set to true on %s",
    "creating_ostpool": "Creating OST Pool",
    "destroying_ostpool": "Destroying OST Pool",
    "updating_ostpool": "Updating OST Pool",
    "grant_ticket": "Granting Ticket",
    "revoke_ticket": "Revoking Ticket",
    "forget_ticket": "Forgetting Ticket",
    "mount_snapshot": "Mounting Snapshot",
    "unmount_snapshot": "Unmounting Snapshot",
    "create_snapshot": "Create snapshot with the given name",
    "destroy_snapshot": "Destroy existing snapshot",
}
