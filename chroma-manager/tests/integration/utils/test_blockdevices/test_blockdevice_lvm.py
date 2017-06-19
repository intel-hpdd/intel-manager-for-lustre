# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

import re

from tests.integration.utils.test_blockdevices.test_blockdevice import TestBlockDevice


class TestBlockDeviceLvm(TestBlockDevice):
    _supported_device_types = ['lvm']

    def __init__(self, device_type, device_path):
        super(TestBlockDeviceLvm, self).__init__(device_type, device_path)

    @property
    def preferred_fstype(self):
        return 'ldiskfs'

    # Create a lvm on the device.
    @property
    def prepare_device_commands(self):
        return ["vgcreate %s %s; lvcreate --wipesignatures n -l 100%%FREE --name %s %s" % (self.vg_name,
                                                                                           self._device_path,
                                                                                           self.lv_name,
                                                                                           self.vg_name)]

    @property
    def vg_name(self):
        return "vg_%s" % "".join([c for c in self._device_path if re.match(r'\w', c)])

    @property
    def lv_name(self):
        return "lv_%s" % "".join([c for c in self._device_path if re.match(r'\w', c)])

    @property
    def device_path(self):
        return "/dev/%s/%s" % (self.vg_name, self.lv_name)

    @classmethod
    def clear_device_commands(cls, device_paths):
        return ["if vgdisplay %s; then vgremove -f %s; else exit 0; fi" % (TestBlockDeviceLvm('lvm', device_path).vg_name,
                                                                           TestBlockDeviceLvm('lvm', device_path).vg_name) for device_path in device_paths]

    @property
    def install_packages_commands(self):
        return []
