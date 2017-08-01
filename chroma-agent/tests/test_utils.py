import StringIO

from django.utils import unittest
from chroma_agent.utils import BlkId, Fstab, Mounts
import mock
from iml_common.test.command_capture_testcase import CommandCaptureTestCase


def patch_open(path_to_result):
    """Return a context manager intercepting calls to 'open' so as to return
    a static result string on read()s according to the path"""

    def fake_open(path, *args):
        return StringIO.StringIO(path_to_result[path])
    return mock.patch('__builtin__.open', fake_open, create = True)


class TestBlkId(CommandCaptureTestCase):
    def test_load(self):
        self.add_command(('blkid', '-s', 'UUID', '-s', 'TYPE'), stdout="""/dev/sda1: UUID="d546845f-481f-48f8-a998-8a81adcdb53d" TYPE="ext3"
/dev/sda2: UUID="V229Xn-n1BI-b9J0-tchM-YRfi-9mTz-SMEE5P" TYPE="LVM2_member"
/dev/mapper/LustreVG-root: UUID="9503858f-5ea9-44b6-b690-f473def07a3d" TYPE="ext3"
/dev/mapper/LustreVG-swap: UUID="c334dbab-3121-41c3-ae2b-1d7ab26f5329" TYPE="swap"
/dev/mapper/LustreVG-usr: UUID="9bc9c040-fc00-4cf3-a073-7096c12a8f17" TYPE="ext3"
/dev/mapper/LustreVG-var: TYPE="ext3" UUID="f9093f90-534c-4c61-a49e-b7cadd32fb90"
""")

        expected_result = [{'path': '/dev/sda1', 'uuid': 'd546845f-481f-48f8-a998-8a81adcdb53d', 'type': 'ext3'},
                           {'path': '/dev/sda2', 'uuid': 'V229Xn-n1BI-b9J0-tchM-YRfi-9mTz-SMEE5P', 'type': 'LVM2_member'},
                           {'path': '/dev/mapper/LustreVG-root', 'uuid': '9503858f-5ea9-44b6-b690-f473def07a3d', 'type': 'ext3'},
                           {'path': '/dev/mapper/LustreVG-swap', 'uuid': 'c334dbab-3121-41c3-ae2b-1d7ab26f5329', 'type': 'swap'},
                           {'path': '/dev/mapper/LustreVG-usr', 'uuid': '9bc9c040-fc00-4cf3-a073-7096c12a8f17', 'type': 'ext3'},
                           {'path': '/dev/mapper/LustreVG-var', 'uuid': 'f9093f90-534c-4c61-a49e-b7cadd32fb90', 'type': 'ext3'}]

        result = BlkId().values()
        self.assertListEqual(sorted(expected_result), sorted(result))

    def test_HYD_1958(self):
        """Reproducer for HYD-1958.  Feed BlkId the result from that bug and check it processes it correctly.
        Checks that BlkId copes with presence of filesystems which do not have a UUID."""

        self.add_command((
                "blkid",
                "-s",
                "UUID",
                "-s",
                "TYPE"
            ), stdout="""/dev/mapper/vg_regalmds00-lv_lustre63: UUID="e4b74ffe-0456-4495-91cd-1b38c0fa070c" TYPE="ext4"
/dev/loop0: TYPE="iso9660"
/dev/sda1: UUID="c9e08e31-b3ce-42b4-ba88-c0a8ca3e46ae" TYPE="ext4"
"""
            )

        expected_result = [
            {
                "path": "/dev/mapper/vg_regalmds00-lv_lustre63",
                "type": "ext4",
                "uuid": "e4b74ffe-0456-4495-91cd-1b38c0fa070c"
            },
            {
                "path": "/dev/loop0",
                "type": "iso9660",
                "uuid": None
            },
            {
                "path": "/dev/sda1",
                "type": "ext4",
                "uuid": "c9e08e31-b3ce-42b4-ba88-c0a8ca3e46ae"
            }
        ]

        result = BlkId().values()
        self.assertListEqual(expected_result, result)

    def test_parse_intolerance(self):
        """
        Check that the BlkId parser raises an exception if it sees something it doesn't understand
        """
        self.add_command((
                "blkid",
                "-s",
                "UUID",
                "-s",
                "TYPE"
        ), stdout="""/dev/mapper/vg_regalmds00-lv_lustre63: UUID="e4b74ffe-0456-4495-91cd-1b38c0fa070c" TYPE="ext4"
/dev/loop0: TYPE="iso9660" JUNK="trash"
/dev/sda1: UUID="c9e08e31-b3ce-42b4-ba88-c0a8ca3e46ae" TYPE="ext4"
""")

        with self.assertRaises(RuntimeError):
            BlkId()


class TestFstab(unittest.TestCase):
    def test_load(self):
        path_to_result = {
            '/etc/fstab': """
#
# /etc/fstab
# Created by anaconda on Wed Sep 26 21:03:16 2012
#
# Accessible filesystems, by reference, are maintained under '/dev/disk'
# See man pages fstab(5), findfs(8), mount(8) and/or blkid(8) for more info
#
/dev/mapper/LustreVG-root /                       ext3    defaults        1 1
/dev/mapper/LustreVG-usr /usr                    ext3    defaults        1 2
/dev/mapper/LustreVG-var /var                    ext3    defaults        1 2
/dev/mapper/LustreVG-swap swap                    swap    defaults        0 0
tmpfs                   /dev/shm                tmpfs   defaults        0 0
devpts                  /dev/pts                devpts  gid=5,mode=620  0 0
sysfs                   /sys                    sysfs   defaults        0 0
proc                    /proc                   proc    defaults        0 0
"""
        }

        expected_result = [('/dev/mapper/LustreVG-root', '/', 'ext3'),
                           ('/dev/mapper/LustreVG-usr', '/usr', 'ext3'),
                           ('/dev/mapper/LustreVG-var', '/var', 'ext3'),
                           ('/dev/mapper/LustreVG-swap', 'swap', 'swap'),
                           ('tmpfs', '/dev/shm', 'tmpfs'),
                           ('devpts', '/dev/pts', 'devpts'),
                           ('sysfs', '/sys', 'sysfs'),
                           ('proc', '/proc', 'proc')]

        with patch_open(path_to_result):
            result = Fstab().all()

            self.assertListEqual(result, expected_result)


class TestMounts(unittest.TestCase):
    def test_load(self):
        path_to_result = {'/proc/mounts': """rootfs / rootfs rw 0 0
            proc /proc proc rw,relatime 0 0
            sysfs /sys sysfs rw,relatime 0 0
            devtmpfs /dev devtmpfs rw,relatime,size=475232k,nr_inodes=118808,mode=755 0 0
            devpts /dev/pts devpts rw,relatime,gid=5,mode=620,ptmxmode=000 0 0
            tmpfs /dev/shm tmpfs rw,relatime 0 0
            /dev/mapper/LustreVG-root / ext3 rw,relatime,errors=continue,user_xattr,acl,barrier=1,data=ordered 0 0
            /proc/bus/usb /proc/bus/usb usbfs rw,relatime 0 0
            /dev/sda1 /boot ext3 rw,relatime,errors=continue,user_xattr,acl,barrier=1,data=ordered 0 0
            /dev/mapper/LustreVG-usr /usr ext3 rw,relatime,errors=continue,user_xattr,acl,barrier=1,data=ordered 0 0
            /dev/mapper/LustreVG-var /var ext3 rw,relatime,errors=continue,user_xattr,acl,barrier=1,data=ordered 0 0
            none /proc/sys/fs/binfmt_misc binfmt_misc rw,relatime 0 0
            sunrpc /var/lib/nfs/rpc_pipefs rpc_pipefs rw,relatime 0 0"""}

        expected_result = [('rootfs', '/', 'rootfs'),
                           ('proc', '/proc', 'proc'),
                           ('sysfs', '/sys', 'sysfs'),
                           ('devtmpfs', '/dev', 'devtmpfs'),
                           ('devpts', '/dev/pts', 'devpts'),
                           ('tmpfs', '/dev/shm', 'tmpfs'),
                           ('/dev/mapper/LustreVG-root', '/', 'ext3'),
                           ('/proc/bus/usb', '/proc/bus/usb', 'usbfs'),
                           ('/dev/sda1', '/boot', 'ext3'),
                           ('/dev/mapper/LustreVG-usr', '/usr', 'ext3'),
                           ('/dev/mapper/LustreVG-var', '/var', 'ext3'),
                           ('none', '/proc/sys/fs/binfmt_misc', 'binfmt_misc'),
                           ('sunrpc', '/var/lib/nfs/rpc_pipefs', 'rpc_pipefs')]

        with patch_open(path_to_result):
            result = Mounts().all()
            self.assertListEqual(result, expected_result)


class PatchedContextTestCase(unittest.TestCase):
    def __init__(self, methodName):
        super(PatchedContextTestCase, self).__init__(methodName)
        self._test_root = None

    def _find_subclasses(self, klass):
        """Introspectively find all descendents of a class"""
        subclasses = []
        for subclass in klass.__subclasses__():
            subclasses.append(subclass)
            subclasses.extend(self._find_subclasses(subclass))
        return subclasses

    @property
    def test_root(self):
        return self._test_root

    @test_root.setter
    def test_root(self, value):
        assert self._test_root == None, "test_root can only be set once per test"

        self._test_root = value

        from chroma_agent.device_plugins.audit import BaseAudit
        for subclass in self._find_subclasses(BaseAudit):
            mock.patch.object(subclass, 'fscontext', self._test_root).start()

        # These classes aren't reliably detected for patching.
        from chroma_agent.device_plugins.audit.node import NodeAudit
        mock.patch.object(NodeAudit, 'fscontext', self._test_root).start()
        from chroma_agent.utils import Mounts
        mock.patch.object(Mounts, 'fscontext', self._test_root).start()

        self.addCleanup(mock.patch.stopall)
