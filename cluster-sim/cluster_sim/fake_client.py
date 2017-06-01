# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


from cluster_sim.log import log
from cluster_sim.utils import Persisted


class FakeClient(Persisted):
    """
    Represents a Lustre client: one or more filesystem mounts, and corresponding
    /proc/ information derived from the filesystem(s).
    """
    default_state = {
        'mounts': []
    }

    def __init__(self, path, address, fake_devices, fake_clusters):
        self.address = address
        self._devices = fake_devices
        self._clusters = fake_clusters

        super(FakeClient, self).__init__(path)

    @property
    def filename(self):
        return "fake_client_%s.json" % self.address

    def mount(self, nids, filesystem_name):
        log.debug("FakeClient.mount %s %s" % (nids, filesystem_name))
        # Look up NIDs to an MGT
        # Check the MGT and targets are really started
        # Add an entry to self.state['mounts']
        if not (nids, filesystem_name) in self.state['mounts']:
            self.state['mounts'].append((nids, filesystem_name))
        self.save()

    def unmount(self, nids, filesystem_name):
        log.debug("FakeClient.unmount %s %s" % (nids, filesystem_name))
        if (nids, filesystem_name) in self.state['mounts']:
            self.state['mounts'].remove((nids, filesystem_name))
        self.save()

    def unmount_all(self):
        self.state['mounts'] = []
        self.save()

    def read_proc(self, path):
        # For each of our mounts, follow it back to an MGT
        # Resolve path to a conf param path

        LLITE_DEFAULTS = {
                'max_cached_mb': "max_cached_mb: 32"
        }

        parts = path.split("/")[1:]
        if parts[3] == 'llite' and parts[4] == "*":
            # /proc/fs/lustre/llite/*/max_cached_mb
            if len(self.state['mounts']) > 1:
                # Assume caller is going to do a llite/*/ query expecting a single filesystem
                raise NotImplementedError("Multiple mounts on %s: %s" % (self.address, self.state['mounts']))

            if len(self.state['mounts']) == 0:
                raise RuntimeError("Tried to read proc on client %s but nothing is mounted" % self.address)

            mgsspec, fsname = self.state['mounts'][0]
            try:
                configs = self._devices.get_conf_params_by_mgsspec(mgsspec)
            except KeyError:
                print "mgsnode %s not found (known mgts are %s)" % (mgsspec, self._devices.state['mgts'].keys())
                raise KeyError("mgsnode %s not found (known mgts are %s)" % (mgsspec, self._devices.state['mgts'].keys()))
            try:
                return configs["%s.llite.%s" % (fsname, parts[5])]
            except KeyError:
                return LLITE_DEFAULTS[parts[5]]

        raise NotImplementedError("Couldn't resolve path %s on client %s" % (path, self.address))
