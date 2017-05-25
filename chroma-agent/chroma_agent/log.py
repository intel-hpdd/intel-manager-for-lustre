#
# INTEL CONFIDENTIAL
#
# Copyright 2013-2017 Intel Corporation All Rights Reserved.
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


import logging
from logging.handlers import SysLogHandler
import os
import sys

# This log is for messages about the internal machinations of our
# daemon and messaging systems, the user would only be interested
# in warnings and errors
daemon_log = logging.getLogger('daemon')

# This log is for copytool monitoring instances. Not particularly interesting
# unless things have gone wrong.
copytool_log = logging.getLogger('copytool')

# This log is for messages about operations invoked at the user's request,
# the user will be interested general breezy chat (INFO) about what we're
# doing for them
console_log = logging.getLogger('console')

logging_in_debug_mode = os.path.exists("/tmp/chroma-agent-debug")

if logging_in_debug_mode or 'nosetests' in sys.argv[0]:
    daemon_log.setLevel(logging.DEBUG)
    copytool_log.setLevel(logging.DEBUG)
    console_log.setLevel(logging.DEBUG)
else:
    daemon_log.setLevel(logging.WARN)
    copytool_log.setLevel(logging.WARN)
    console_log.setLevel(logging.WARN)

agent_loggers = [daemon_log, console_log, copytool_log]


# these are signal handlers used to adjust loglevel at runtime
def increase_loglevel(signal, frame):
    for logger in agent_loggers:
        # impossible to go below 10 -- logging resets to WARN
        logger.setLevel(logger.getEffectiveLevel() - 10)
        logger.critical("Log level set to %s" %
                        logging.getLevelName(logger.getEffectiveLevel()))


def decrease_loglevel(signal, frame):
    for logger in agent_loggers:
        current_level = logger.getEffectiveLevel()
        # No point in setting higher than this
        if current_level >= logging.CRITICAL:
            return
        logger.setLevel(current_level + 10)
        logger.critical("Log level set to %s" %
                        logging.getLevelName(logger.getEffectiveLevel()))


# Not setting up logs at import time because we want to
# set them up after daemonization
def daemon_log_setup():
    handler = logging.FileHandler("/var/log/chroma-agent.log")
    handler.setFormatter(logging.Formatter('[%(asctime)s] daemon %(levelname)s %(message)s', '%d/%b/%Y:%H:%M:%S'))
    daemon_log.addHandler(handler)


class SafeSyslogFormatter(logging.Formatter):
    # Python's logging module has some unicode bugs[1], so to be
    # safe we'll just strip it out.
    #
    # 1. http://bugs.python.org/issue14452
    def format(self, record):
        result = logging.Formatter.format(self, record)
        if isinstance(result, unicode):
            result = result.encode('utf-8', 'replace')
        return result


# Log copytool stuff to syslog because we may have multiple processes running.
def copytool_log_setup():
    handler = SysLogHandler(facility=SysLogHandler.LOG_DAEMON, address="/dev/log")
    # FIXME: define a custom formatter that will handle unicode strings
    handler.setFormatter(SafeSyslogFormatter('[%(asctime)s] copytool %(levelname)s: %(message)s', '%d/%b/%Y:%H:%M:%S'))
    copytool_log.addHandler(handler)

    # Hijack these so that we can reuse code without stomping on other
    # processes' logging.
    console_log.addHandler(handler)
    daemon_log.addHandler(handler)


def console_log_setup():
    handler = logging.FileHandler("/var/log/chroma-agent-console.log")
    handler.setFormatter(logging.Formatter('[%(asctime)s] console %(levelname)s %(message)s', '%d/%b/%Y:%H:%M:%S'))
    console_log.addHandler(handler)
