#
# INTEL CONFIDENTIAL
#
# Copyright 2013-2016 Intel Corporation All Rights Reserved.
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


try:
    from scm_version import VERSION, PACKAGE_VERSION, IS_RELEASE, BUILD
    __version__ = VERSION
    __package_version__ = PACKAGE_VERSION
    __build__ = BUILD
    __is_release__ = IS_RELEASE
except ImportError:
    # These are defaults, should loosely track latest dev tag, won't
    # work with packaging but will allow non-packaged installs to work
    # OK.
    __version__ = '1.99.0.0-dev'
    __package_version__ = __version__
    __build__ = 1
    __is_release__ = False


def package_version():
    return __package_version__


def version():
    return __version__
