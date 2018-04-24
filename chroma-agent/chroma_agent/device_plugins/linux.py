# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


from chroma_agent.lib.shell import AgentShell
from chroma_agent.plugin_manager import DevicePlugin
from chroma_agent import config
from chroma_agent.device_plugins.linux_components.block_devices import BlockDevices


class LinuxDevicePlugin(DevicePlugin):
    # Some places require that the devices have been scanned before then can operate correctly, this is because the
    # scan creates and stores some information that is use in other places. This is non-optimal because it gives the
    # agent some state which we try and avoid. But this flag does at least allow us to keep it neat.
    devices_scanned = False

    def __init__(self, session):
        super(LinuxDevicePlugin, self).__init__(session)
        self._last_quick_scan_result = ""
        self._last_full_scan_result = None

    def _quick_scan(self):
        """Lightweight enumeration of available block devices"""
        return BlockDevices.quick_scan()

    def _full_scan(self):
        # If we are a worker node then return nothing because our devices are not of interest. This is a short term
        # solution for HYD-3140. This plugin should really be loaded if it is not needed but for now this sorts out
        # and issue with PluginAgentResources being in the linux plugin.
        if config.get('settings', 'profile')['worker']:
            return {}

        # Before we do anything do a partprobe, this will ensure that everything gets an up to date view of the
        # device partitions. partprobe might throw errors so ignore return value
        AgentShell.run(["partprobe"])

        # Map of block devices major:minors to /dev/ path.
        block_devices = BlockDevices()

        # fixme: implement inside block_devices using device_scanner output
        # EMCPower Devices
        # emcpowers = EMCPower(block_devices).all()

        LinuxDevicePlugin.devices_scanned = True

        block_device_dict = {s: getattr(block_devices, s) for s in
                             ['local_fs', 'mds', 'vgs', 'lvs', 'zfspools', 'zfsdatasets', 'zfsvols']}

        block_device_dict['devs'] = block_devices.block_device_nodes

        # block_device_dict['emcpowers'] = emcpowers

        return block_device_dict

    def _scan_devices(self, scan_always):
        full_scan_result = None

        if scan_always or (self._quick_scan() != self._last_quick_scan_result):
            self._last_quick_scan_result = self._quick_scan()
            full_scan_result = self._full_scan()
            self._last_full_scan_result = full_scan_result
        elif self._safety_send < DevicePlugin.FAILSAFEDUPDATE:
            self._safety_send += 1
        else:
            # The purpose of this is to cause the ResourceManager to re-evaluate the device-graph for this
            # host which may lead to different results if the devices reported from other hosts has changed
            # This should not really be required but is a harmless work around while we get the manager code
            # in order
            full_scan_result = self._last_full_scan_result

        if full_scan_result is not None:
            self._safety_send = 0

        return full_scan_result

    def start_session(self):
        return self._scan_devices(True)

    def update_session(self):
        trigger_plugin_update = self.trigger_plugin_update
        self.trigger_plugin_update = False
        scan_result = self._scan_devices(trigger_plugin_update)

        return scan_result
