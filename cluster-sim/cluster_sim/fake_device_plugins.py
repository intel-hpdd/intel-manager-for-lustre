# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import logging
import datetime

from chroma_agent.device_plugins.action_runner import ActionRunnerPlugin
from chroma_agent.device_plugins.syslog import MAX_LOG_LINES_PER_MESSAGE
from chroma_agent.plugin_manager import DevicePlugin, DevicePluginMessageCollection, PRIO_LOW
from iml_common.lib.date_time import IMLDateTime
log = logging.getLogger(__name__)


class BaseFakeLinuxPlugin(DevicePlugin):
    _server = None

    def start_session(self):
        return {
            'mpath': {},
            'lvs': {},
            'devs': self._server._devices.get_nodes(self._server.fqdn),
            'local_fs': {},
            'mds': {},
            'vgs': {}
        }


class BaseFakeLinuxNetworkPlugin(DevicePlugin):
    _server = None

    def _lnet_state(self):
        return {(False, False): 'lnet_unloaded',
                (False, True): 'lnet_unloaded',
                (True, False): 'lnet_down',
                (True, True): 'lnet_up'}[(self._server.state['lnet_loaded'], self._server.state['lnet_up'])]

    def start_session(self):
        return self.update_session()

    def update_session(self):
        result = self._get_results()

        return result

    def _get_results(self):
        names = {'tcp': 'eth', 'o2ib': 'ib'}
        interfaces = {}
        nids = {}

        for inet4_address in self._server.network_interfaces.keys():
            interface = self._server.network_interfaces[inet4_address]
            name = '%s%s' % (names[interface['type']], interface['interface_no'])

            interfaces[name] = {'mac_address': '12:34:56:78:90:1%s' % interface['interface_no'],
                                'inet4_address': inet4_address,
                                'inet4_prefix': 24,
                                'inet6_address': 'Need An inet6 Simulated Address',
                                'type': interface['type'],                          # We report LND types for consistency, the real version does as well
                                'rx_bytes': '24400222349',
                                'tx_bytes': '1789870413',
                                'up': True,
                                'slave': False}

            if interface['lnd_network'] is not None:
                nids[name] = {'nid_address': inet4_address,
                              'type': interface['type'],
                              'lnd_network': interface['lnd_network'],
                              'lnd_type': interface['lnd_type']}

        # If lnet is up but no nids are configured then create 1 because lnet always returns 1 nid
        if (self._server.state['lnet_up'] == True) and (nids == {}):
            self._server.network_interfaces[self._server.network_interfaces.keys()[0]]['lnd_network'] = 0
            return self._get_results()

        return {'interfaces': interfaces,
                'lnet': {'state': self._lnet_state(),
                         'nids': nids}}


class BaseFakeSyslogPlugin(DevicePlugin):
    _server = None

    def start_session(self):
        return {
            'log_lines': [
                {
                    'source': 'cluster_sim',
                    'severity': 1,
                    'facility': 1,
                    'message': 'Lustre: Cluster simulator syslog session start %s %s' % (self._server.fqdn, datetime.datetime.now()),
                    'datetime': IMLDateTime.utcnow().isoformat() + 'Z'
                }
            ]
        }

    def update_session(self):
        messages = self._server.pop_log_messages()
        if messages:
            result = DevicePluginMessageCollection([], priority = PRIO_LOW)
            for i in range(0, len(messages), MAX_LOG_LINES_PER_MESSAGE):
                result.append({
                    'log_lines': messages[i:i + MAX_LOG_LINES_PER_MESSAGE]
                })

            return result


class ClusterSimulator(DevicePlugin):
    _server = None

    def start_session(self):
        return {}

    def update_session(self):
        return {}


class BaseFakeLustrePlugin(DevicePlugin):
    _server = None

    def start_session(self):
        return self.update_session(first=True)

    def update_session(self, first=False):
        mounts = []
        for resource in self._server._cluster.get_running_resources(self._server.nodename):
            mounts.append({
                'device': resource['device_path'],
                'fs_uuid': resource['uuid'],
                'mount_point': resource['mount_point'],
                'recovery_status': {}
            })

        if first:
            packages = self._server.scan_packages()
        else:
            packages = None

        return {
            'resource_locations': self._server._cluster.resource_locations(),
            'capabilities': ['manage_targets'],
            'metrics': {
                'raw': {
                    'node': self._server.get_node_stats(),
                    'lustre': self._server.get_lustre_stats(),
                    'lustre_client_mounts': self._server.lustre_client_mounts
                }
            },
            'packages': packages,
            'mounts': mounts,
            "properties": {'zfs_installed': False,
                           'distro': 'CentOS',
                           'distro_version': 6.6,
                           'python_version_major_minor': 2.6,
                           'python_patchlevel': 6,
                           'kernel_version': '2.6.32-504.8.1.el6_lustre.x86_64'},
            'started_at': IMLDateTime.utcnow().isoformat() + 'Z',
            'agent_version': 'dummy'
        }


class BaseFakeCorosyncPlugin(DevicePlugin):

    _server = None

    def get_test_message(self,
                         utc_iso_date_str='2013-01-11T19:04:07+00:00',
                         node_status_list=None):
        """
        Simulate a message from the Corosync agent plugin

        The plugin currently sends datetime in UTC of the nodes localtime.

        TODO:  If that plugin changes format, this must change too.  Consider
        moving this somewhere that is easier to maintain
        e.g. closer to the actual plugin, since the message is initially
        created there based on data reported by corosync.

        TODO: This method is also in tests/unit/services/test_corosync.py.
        Some effort shoudl be considered to consolidate this, so that both
        tests can use the same source.
        """

        # If corosync / pacemaker is not yet running then crm_info should be empty
        crm_info = {}
        if not ((self._server.state['corosync'].state == 'stopped') or
                (self._server.state['pacemaker'].state == 'stopped')):
            nodes = {}
            if node_status_list is not None:
                for hs in node_status_list:
                    node = hs[0]
                    status = hs[1] and 'true' or 'false'
                    node_dict = {node: {
                        'name': node, 'standby': 'false',
                        'standby_onfail': 'false',
                        'expected_up': 'true',
                        'is_dc': 'true', 'shutdown': 'false',
                        'online': status, 'pending': 'false',
                        'type': 'member', 'id': node,
                        'resources_running': '0', 'unclean': 'false'}}
                    nodes.update(node_dict)

            crm_info = {'nodes': nodes,
                        'options': {'stonith_enabled': True},
                        'datetime': utc_iso_date_str}

        #  Second create the message with the nodes and other envelope data.
        message = {'crm_info': crm_info,
                   'state': {'corosync': self._server.state['corosync'].state,
                             'pacemaker': self._server.state['pacemaker'].state}}

        return message

    def start_session(self):

        #  This fake plugin needs to look at it corosync defined peers of
        #  this fake server and determine
        #  which are online.  This happens in production by shelling out the
        #  call crm_mon --one-shot --as-xml

        #  To simulate this, the _server object which is a FakeServer, must
        #  be able to tell this server what it's peers are.

        #  This implementation looks at ALL the servers in the simulator,
        #  and those ones that are also join'ed in the cluster are online.

        log.debug('cluster nodes:  %s' % self._server._cluster.state['nodes'])

        nodes = [(node_dict['nodename'], node_dict['online']) for node_dict
                 in self._server._cluster.state['nodes'].values()]

        log.debug('Nodes and state:  %s' % nodes)

        dt = IMLDateTime.utcnow().isoformat()
        message = self.get_test_message(utc_iso_date_str=dt,
                                        node_status_list=nodes)

        log.debug(message)
        return message

    def update_session(self):
        return self.start_session()


class FakeDevicePlugins():
    '''
    Fake versions of the device plugins, sending monitoring
    information derived from the simulator state (e.g. corosync
    resource locations come from FakeCluster, lustre target
    statistics come from FakeDevices).
    '''
    def __init__(self, server):
        self._server = server

        class FakeLinuxPlugin(BaseFakeLinuxPlugin):
            _server = self._server

        class FakeLinuxNetworkPlugin(BaseFakeLinuxNetworkPlugin):
            _server = self._server

        class FakeLustrePlugin(BaseFakeLustrePlugin):
            _server = self._server

        class FakeSyslogPlugin(BaseFakeSyslogPlugin):
            _server = self._server

        class FakeCorosyncPlugin(BaseFakeCorosyncPlugin):
            _server = self._server

        self._classes = {
            'linux': FakeLinuxPlugin,
            'linux_network': FakeLinuxNetworkPlugin,
            'lustre': FakeLustrePlugin,
            'action_runner': ActionRunnerPlugin,
            'syslog': FakeSyslogPlugin,
            'corosync': FakeCorosyncPlugin,
            'simulator_controller': ClusterSimulator
        }

    def get_plugins(self):
        return self._classes

    def get(self, name):
        return self._classes[name]
