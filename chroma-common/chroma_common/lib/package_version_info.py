#
# INTEL CONFIDENTIAL
#
# Copyright 2013-2015 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related
# to the source code ("Material") are owned by Intel Corporation or its
# suppliers or licensors. Title to the Material remains with Intel Corporation
# or its suppliers and licensors. The Material contains trade secrets and
# proprietary and confidential information of Intel or its suppliers and
# licensors. The Material is protected by worldwide copyright and trade secret
# laws and treaty provisions. No part of the Material may be used, copied,
# reproduced, modified, published, uploaded, posted, transmitted, distributed,
# or disclosed in any way without Intel's prior express written permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or delivery
# of the Materials, either expressly, by implication, inducement, estoppel or
# otherwise. Any license under such intellectual property rights must be
# express and approved by Intel in writing.


# import so that this module can be imported in pure python environments as well as on Linux.
# Doing it this way means that rpm_lib can mocked.
try:
    import rpm as rpm_lib
except:
    try:
        # Allow an override in local settings, because I want to get real output I run it on a remote linux node from my mac.
        # I have this in my local_settings.py : Chris
        # class rpm_lib(object):
        #     @classmethod
        #     def labelCompare(cls, a, b):
        #         from subprocess import Popen, PIPE
        #         result = Popen(["ssh", "lotus-33",  "python -c \"import rpm\nimport sys\nsys.stdout.write(str(rpm.labelCompare(('%s', '%s', '%s'), ('%s', '%s', '%s'))))\"" % (a[0], a[1], a[2], b[0], b[1], b[2])], stdout=PIPE).communicate()[0]
        #         return int(result
        from local_settings import rpm_lib
    except:
        class rpm_lib(object):
            @classmethod
            def labelCompare(cls, a, b):
                return cmp(a, b)


class VersionInfo(object):
    """
    A convenient way of storing package version information that can be printed and compared with ease.

    At present this class is not serializable and so cannot be converted easily into json. This highlights
    a limitation of our current use of plain json for messages in that all but the basic types have to be
    discarded during their transportation.
    """
    def __init__(self, epoch, version, release, arch):
        self.epoch = epoch
        self.version = version
        self.release = release
        self.arch = arch

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return "epoch='%s', version='%s', release='%s', arch='%s'" % (self.epoch, self.version, self.release, self.arch)

    def __cmp__(self, other):
        return rpm_lib.labelCompare((self.epoch, self.version, self.release), (other.epoch, other.version, other.release))
