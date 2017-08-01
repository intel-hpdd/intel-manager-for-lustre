# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import os

from chroma_agent.lib.shell import AgentShell
from chroma_agent.log import console_log
from chroma_agent.device_plugins.action_runner import CallbackAfterResponse
from chroma_agent.lib.pacemaker import PacemakerConfig
from iml_common.blockdevices.blockdevice import BlockDevice
from iml_common.lib import util
from iml_common.lib.agent_rpc import agent_error
from iml_common.lib.agent_rpc import agent_result_ok
from chroma_agent.lib.agent_startup_functions import agent_daemon_startup_function
from chroma_agent.lib.agent_teardown_functions import agent_daemon_teardown_function
from chroma_agent import config


def ssi(runlevel):
    # force a manual failover by failing a node
    AgentShell.try_run(["sync"])
    AgentShell.try_run(["sync"])
    AgentShell.try_run(["init", runlevel])


def fail_node():
    ssi("0")


def stonith(node):
    p_cfg = PacemakerConfig()

    # TODO: signal that manager that a STONITH has been done so that it
    #       doesn't treat it as an AWOL
    console_log.info("Rebooting %s per a STONITH request" % node)

    p_cfg.get_node(node).fence_reboot()


def shutdown_server(halt = True, at_time = "now"):
    def _shutdown():
        console_log.info("Initiating server shutdown per manager request")
        # This will initiate a "nice" shutdown with a wall from root, etc.
        AgentShell.try_run(["shutdown", "-H" if halt else "-h", at_time])

        console_log.info("Terminating")
        os._exit(0)

    raise CallbackAfterResponse(None, _shutdown)


def reboot_server(at_time = "now"):
    def _reboot():
        console_log.info("Initiating server reboot per manager request")
        # reboot(8) just calls shutdown anyhow.
        AgentShell.try_run(["shutdown", "-r", at_time])

        console_log.info("Terminating")
        os._exit(0)

    raise CallbackAfterResponse(None, _reboot)


# When the agent is run we want to allow block devices to do any initialization that they might need, this function
# may also be called by the manager.
@agent_daemon_startup_function()
def initialise_block_device_drivers():
    console_log.info("Initialising drivers for block device types")
    for cls in util.all_subclasses(BlockDevice):
        error = cls.initialise_driver(config.profile_managed)

        if error:
            return agent_error(error)

    return agent_result_ok


# When the agent is stopped we want to allow block devices to do any termination that they might need, this function
# may also be called by the manager.
@agent_daemon_teardown_function()
def terminate_block_device_drivers():
    console_log.info("Terminating drivers for block device types")
    for cls in util.all_subclasses(BlockDevice):
        error = cls.terminate_driver()

        if error:
            return agent_error(error)

    return agent_result_ok

ACTIONS = [reboot_server, shutdown_server, fail_node, stonith, initialise_block_device_drivers]
