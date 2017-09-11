#!/usr/bin/env python
# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import json
import argparse
import time
from datetime import datetime
import requests

all_queues_to_monitor = ['agent_lustre_rx',
                         'agent_linux_rx',
                         'jobs',
                         'job_scheduler_notifications',
                         'AgentDaemonRpcInterface.requests',
                         'ScanDaemonRpcInterface.requests',
                         'agent_systemd_journal_rx',
                         'agent_linux_network_rx',
                         'periodic',
                         'agent_action_runner_rx',
                         'PowerControlRpc.requests',
                         'HttpAgentRpc.requests',
                         'agent_tx',
                         'JobSchedulerRpc.requests',
                         'agent_corosync_rx',
                         'HttpAgentRpc.responses_eagle-48.eagle.hpdd.intel.com_10442',      # To monitor this queue this line needs to be updated by hand.
                         'stats']

queues_to_monitor = ['agent_lustre_rx',
                     'agent_tx',
                     'stats']

#queues_to_monitor = all_queues_to_monitor


def _authenticated_session(url, username, password):
    session = requests.session()
    session.headers = {"Accept": "application/json",
                       "Content-type": "application/json"}
    session.verify = False
    response = session.get("%s/api/session/" % url)
    if not response.ok:
        raise RuntimeError("Failed to open session")
    session.headers['X-CSRFToken'] = response.cookies['csrftoken']
    session.cookies['csrftoken'] = response.cookies['csrftoken']
    session.cookies['sessionid'] = response.cookies['sessionid']

    response = session.post("%s/api/session/" % url, data = json.dumps({'username': username, 'password': password}))
    if not response.ok:
        raise RuntimeError("Failed to authenticate")

    return session


def get_queues(session, url):
    response = session.get(url + "/api/system_status")
    assert response.ok
    return response.json()['rabbitmq']['queues']


parser = argparse.ArgumentParser(description="IML Queue Monitor")
parser.add_argument('--url', required=False, help="Manager URL", default="https://localhost:8000")
parser.add_argument('--username', required=False, help="REST API username", default='admin')
parser.add_argument('--password', required=False, help="REST API password", default='lustre')
parser.add_argument('--colwidth', required=False, help="Width of output columns", default=20)

args = parser.parse_args()

# Change the url, user and password here.
session = _authenticated_session(args.url, args.username, args.password)

print str(datetime.now()) + ": " + "".join([q.ljust(args.colwidth) for q in queues_to_monitor])

while True:
    ts = time.time()

    samples = dict((queue['name'], queue['messages']) for queue in get_queues(session, args.url) if 'messages' in queue)

    message_counts = [str(samples[sample]).ljust(args.colwidth) for sample in queues_to_monitor]

    print str(datetime.now()) + ": " + "".join(message_counts)

    time.sleep(10)
