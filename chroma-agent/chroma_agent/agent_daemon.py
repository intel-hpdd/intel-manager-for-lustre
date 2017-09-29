# -*- coding: utf-8 -*-
# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import time
import datetime
import errno
import os
import logging
import sys
import traceback
import argparse
import signal
import socket

from daemon.daemon import set_signal_handlers
from daemon import DaemonContext
# pidlockfile was split out of daemon into it's own package in 1.6
try:
    from daemon.pidlockfile import PIDLockFile
    assert PIDLockFile  # Silence Pyflakes
except ImportError:
    from lockfile.pidlockfile import PIDLockFile

from chroma_agent import config
from chroma_agent.crypto import Crypto
from chroma_agent.plugin_manager import ActionPluginManager, DevicePluginManager
from chroma_agent.agent_client import AgentClient
from chroma_agent.log import daemon_log, daemon_log_setup, console_log_setup, increase_loglevel, decrease_loglevel
from chroma_agent.lib.agent_startup_functions import agent_daemon_startup_functions
from chroma_agent.lib.agent_teardown_functions import agent_daemon_teardown_functions


class ServerProperties(object):
    @property
    def fqdn(self):
        return socket.getfqdn()

    @property
    def nodename(self):
        return os.uname()[1]

    @property
    def boot_time(self):
        for line in open("/proc/stat").readlines():
            name, val = line.split(" ", 1)
            if name == 'btime':
                return datetime.datetime.fromtimestamp(int(val))


def kill_orphans_of(parent_pid, timeout = 5):
    """
    If a previous instance of the service left its PID file behind (unclean stop) then
    we will see if there are any subprocesses hanging around, and kill them.  This is done
    to avoid e.g. a mkfs from a previous agent instance still being in progress when we
    start another one (imagine restarting chroma-agent during a long format and then clicking
    the format button again in the GUI).

    :param parent_pid: Find orphans from this PID
    :param timeout: Wait this long for orphan processes to respond to SIGKILL
    :return: None
    """
    victims = []
    for pid in [int(pid) for pid in os.listdir('/proc') if pid.isdigit()]:
        try:
            stat = open("/proc/%s/stat" % pid).read().strip()
            pgid = int(stat.split()[4])
            ppid = int(stat.split()[3])
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise
        else:
            if int(pgid) == int(parent_pid) and ppid == 1:
                sys.stderr.write("Killing orphan process %s from previous instance %s\n" % (pid, parent_pid))
                victims.append(pid)
                os.kill(pid, signal.SIGKILL)

    n = 0
    while True:
        for pid in victims:
            if not os.path.exists("/proc/%s" % pid):
                sys.stderr.write("Process %s terminated successfully\n")
                victims.remove(pid)

        if victims:
            n += 1

            if n > timeout:
                raise RuntimeError("Failed to kill orphan processes %s after %s seconds" % (victims, timeout))

            time.sleep(1)
        else:
            break


def main():
    """Daemonize and handle unexpected exceptions"""
    parser = argparse.ArgumentParser(description="Intel® Manager for Lustre* software Agent")
    parser.add_argument("--foreground", action="store_true")
    parser.add_argument("--publish-zconf", action="store_true")
    parser.add_argument("--pid-file", default = "/var/run/chroma-agent.pid")
    args = parser.parse_args()

    # FIXME: at startup, if there is a PID file hanging around, find any
    # processes which are children of that old PID, and kill them: prevent
    # orphaned processes from an old agent run hanging around where they
    # could cause trouble (think of a 2 hour mkfs)

    if not args.foreground:
        if os.path.exists(args.pid_file):
            pid = None
            try:
                pid = int(open(args.pid_file).read())
                os.kill(pid, 0)
            except (ValueError, OSError, IOError):
                # Not running, delete stale PID file
                sys.stderr.write("Removing stale PID file\n")
                try:
                    os.remove(args.pid_file)
                    os.remove(args.pid_file + ".lock")
                except OSError, e:
                    import errno
                    if e.errno != errno.ENOENT:
                        raise e

                if pid is not None:
                    kill_orphans_of(pid)
            else:
                # Running, we should refuse to run
                raise RuntimeError("Daemon is already running (PID %s)" % pid)
        else:
            if os.path.exists(args.pid_file + ".lock"):
                sys.stderr.write("Removing stale lock file\n")
                os.remove(args.pid_file + ".lock")

        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        context = DaemonContext(pidfile = PIDLockFile(args.pid_file))
        context.open()

        daemon_log_setup()
        console_log_setup()
        daemon_log.info("Starting in the background")
    else:
        context = None
        daemon_log_setup()
        daemon_log.addHandler(logging.StreamHandler())

        console_log_setup()

    try:
        daemon_log.info("Entering main loop")
        try:
            conf = config.get('settings', 'server')
        except (KeyError, TypeError) as e:
            daemon_log.error("No configuration found (must be registered before running the agent service), "
                             "details: %s" % e)
            return

        if config.profile_managed is False:
            # This is kind of terrible. The design of DevicePluginManager is
            # such that it can be called with either class methods or
            # instantiated and then called with instance methods. As such,
            # we can't pass in a list of excluded plugins to the instance
            # constructor. Well, we could, but it would only work some
            # of the time and that would be even more awful.
            import chroma_agent.plugin_manager
            chroma_agent.plugin_manager.EXCLUDED_PLUGINS += ['corosync']

        agent_client = AgentClient(
            conf['url'] + "message/",
            ActionPluginManager(),
            DevicePluginManager(),
            ServerProperties(),
            Crypto(config.path))

        def teardown_callback(*args, **kwargs):
            agent_client.stop()
            agent_client.join()
            [function() for function in agent_daemon_teardown_functions]

        if not args.foreground:
            handlers = {
                signal.SIGTERM: teardown_callback,
                signal.SIGUSR1: decrease_loglevel,
                signal.SIGUSR2: increase_loglevel
            }
            set_signal_handlers(handlers)
        else:
            signal.signal(signal.SIGINT, teardown_callback)
            signal.signal(signal.SIGUSR1, decrease_loglevel)
            signal.signal(signal.SIGUSR2, increase_loglevel)

        # Call any agent daemon startup methods that were registered.
        [function() for function in agent_daemon_startup_functions]

        agent_client.start()
        # Waking-wait to pick up signals
        while not agent_client.stopped.is_set():
            agent_client.stopped.wait(timeout = 10)

        agent_client.join()
    except Exception, e:
        backtrace = '\n'.join(traceback.format_exception(*(sys.exc_info())))
        daemon_log.error("Unhandled exception: %s" % backtrace)

    if context:
        # NB I would rather ensure cleanup by using 'with', but this
        # is python 2.4-compatible code
        context.close()

    # Call any agent daemon teardown methods that were registered.
    [function() for function in agent_daemon_teardown_functions]

    daemon_log.info("Terminating")
