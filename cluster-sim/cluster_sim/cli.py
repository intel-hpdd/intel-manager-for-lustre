# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import argparse
import json
import signal
import threading
import os
import sys
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging
import requests


from cluster_sim.fake_power_control import FakePowerControl
from cluster_sim.simulator import ClusterSimulator
from cluster_sim.log import log

from chroma_agent.agent_daemon import daemon_log
from chroma_agent.action_plugins.settings_management import reset_agent_config, set_agent_config

from iml_common.lib.date_time import IMLDateTime


SIMULATOR_PORT = 8743


class RpcThread(threading.Thread):
    def __init__(self, simulator):
        super(RpcThread, self).__init__()
        self.simulator = simulator

    def run(self):
        self.server = SimpleXMLRPCServer(('localhost', SIMULATOR_PORT), allow_none = True)
        self.server.register_instance(self.simulator)

        log.info("Listening on %s" % SIMULATOR_PORT)
        self.server.serve_forever()

    def stop(self):
        self.server.shutdown()


class SimulatorCli(object):
    def __init__(self):
        self._stopping = threading.Event()

    def setup(self, args):
        log.info("Setting up simulator configuration for %s servers in %s/" % (args.server_count, args.config))

        worker_count = int(args.worker_count)

        server_count = int(args.server_count)
        if args.volume_count:
            volume_count = int(args.volume_count)
        else:
            volume_count = server_count * 2

        reset_agent_config()
        set_agent_config('copytool_fifo_directory', '/tmp')

        simulator = ClusterSimulator(args.config, args.url)
        simulator.setup(server_count, worker_count, volume_count, int(args.nid_count), int(args.cluster_size), int(args.psu_count), int(args.su_size))

    def _get_authenticated_session(self, url, username, password):
        session = requests.session()
        session.headers = {"Accept": "application/json"}
        session.verify = False

        response = session.get("%sapi/session/" % url)
        if not response.ok:
            raise RuntimeError("Failed to open session")
        session.headers['X-CSRFToken'] = response.cookies['csrftoken']
        session.cookies['csrftoken'] = response.cookies['csrftoken']
        session.cookies['sessionid'] = response.cookies['sessionid']

        response = session.post("%sapi/session/" % url,
                                data = json.dumps({'username': username, 'password': password}),
                                headers = {"Content-type": "application/json"})
        if not response.ok:
            raise RuntimeError("Failed to authenticate")

        return session

    def _acquire_token(self, url, username, password, credit_count, duration=None, preferred_profile=None):
        """
        Localised use of the REST API to acquire a server registration token.
        """
        session = self._get_authenticated_session(url, username, password)

        # Acquire a server profile
        response = session.get("%sapi/server_profile/" % url)
        if not preferred_profile:
            profile = response.json()['objects'][0]
        else:
            try:
                profile = [p for p in response.json()['objects'] if p['name'] == preferred_profile][0]
            except IndexError:
                raise RuntimeError("No such profile: %s" % preferred_profile)

        args = {
            'credits': credit_count,
            'profile': profile['resource_uri']
        }
        if duration is not None:
            args['expiry'] = (IMLDateTime.utcnow() + duration).isoformat()

        response = session.post("%sapi/registration_token/" % url, data=json.dumps(args))
        assert response.ok
        return response.json()['secret']

    def register(self, args):
        simulator = ClusterSimulator(args.config, args.url)
        server_count = len(simulator.servers)

        if args.secret:
            server_secret = args.secret
            worker_secret = args.secret
        elif args.username and args.password:
            server_secret = self._acquire_token(args.url, args.username, args.password, server_count, preferred_profile='base_managed_rh7')
            worker_secret = self._acquire_token(args.url, args.username, args.password, server_count, preferred_profile='posix_copytool_worker')
        else:
            sys.stderr.write("Must pass either --secret or --username and --password\n")
            sys.exit(-1)

        log.info("Registering %s servers in %s/" % (server_count, args.config))
        register_count = simulator.register_all(server_secret, worker_secret)

        if args.create_pdu_entries and register_count > 0:
            self.create_pdu_entries(simulator, args)

        return simulator

    def create_pdu_entries(self, simulator, args):
        if not (args.username and args.password):
            sys.stderr.write("Username and password required to create PDU entries\n")
            sys.exit(-1)

        session = self._get_authenticated_session(args.url, args.username, args.password)

        log.info("Creating PDU entries and associating PDU outlets with servers...")
        outlet_count = len(simulator.servers)

        if outlet_count < 1:
            log.error("Skipping PDU creation (no servers)")
            return

        # Create a custom type to ensure that it has enough outlets.
        # NB: If more servers are added later this won't work correctly,
        # but it should handle most use cases for simulated clusters.
        response = session.post("%s/api/power_control_type/" % args.url, data = json.dumps({
            'agent': "fence_apc",
            'make': "Fake",
            'model': "PDU",
            'default_username': "apc",
            'default_password': "apc",
            'max_outlets': outlet_count
        }))

        assert 200 <= response.status_code < 300, response.text
        fence_apc = json.loads(response.text)

        log.debug("Created power_control_type: %s" % fence_apc['name'])

        pdu_entries = []
        for pdu_sim in simulator.power.pdu_sims.values():
            response = session.post("%s/api/power_control_device/" % args.url, data = json.dumps({
                'device_type': fence_apc['resource_uri'],
                'name': pdu_sim.name,
                'address': pdu_sim.address,
                'port': pdu_sim.port
            }))

            assert 200 <= response.status_code < 300, response.text
            pdu_entries.append(json.loads(response.text))
            log.debug("Created power_control_device: %s" % pdu_entries[-1]['name'])

        response = session.get("%s/api/host/" % args.url, data = json.dumps({
            'limit': 0
        }))
        assert 200 <= response.status_code < 300, response.text
        servers = [s for s in json.loads(response.text)['objects'] if 'posix_copytool_worker' not in s['server_profile']]

        for i, server in enumerate(sorted(servers, key=lambda server: server['fqdn'])):
            for pdu in pdu_entries:
                outlet = [o for o in pdu['outlets'] if o['identifier'] == str(i + 1)][0]
                response = session.patch("%s/%s" % (args.url, outlet['resource_uri']), data = json.dumps({
                    'host': server['resource_uri']
                }))
                assert 200 <= response.status_code < 300, response.text
                log.debug("Created association %s <=> %s:%s" % (server['fqdn'], pdu['name'], outlet['identifier']))

    def run(self, args):
        simulator = ClusterSimulator(args.config, args.url)
        log.info("Running %s servers in %s/" % (len(simulator.servers), args.config))
        simulator.start_all()

        return simulator

    def stop(self):
        self.simulator.stop()
        self._stopping.set()

    def main(self):
        log.addHandler(logging.StreamHandler())

        daemon_log.addHandler(logging.StreamHandler())
        daemon_log.setLevel(logging.DEBUG)
        handler = logging.FileHandler("chroma-agent.log")
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s', '%d/%b/%Y:%H:%M:%S'))
        daemon_log.addHandler(handler)

        # Usually on our Intel laptops https_proxy is set, and needs to be unset for tests,
        # but let's not completely rule out the possibility that someone might want to run
        # the tests on a remote system using a proxy.
        if 'https_proxy' in os.environ:
            sys.stderr.write("Warning: Using proxy %s from https_proxy" % os.environ['https_proxy'] +
                             " environment variable, you probably don't want that\n")

        parser = argparse.ArgumentParser(description = "Cluster simulator")
        parser.add_argument('--config', required = False, help = "Simulator configuration/state directory", default = "cluster_sim")
        parser.add_argument('--url', required = False, help = "Manager URL", default = "https://localhost:8000/")
        subparsers = parser.add_subparsers()
        setup_parser = subparsers.add_parser("setup")
        setup_parser.add_argument('--su_size', required = False, help = "Servers per SU", default = '0')
        setup_parser.add_argument('--cluster_size', required = False, help = "Number of simulated storage servers", default = '4')
        setup_parser.add_argument('--server_count', required = False, help = "Number of simulated storage servers", default = '8')
        setup_parser.add_argument('--worker_count', required = False, help = "Number of simulated HSM workers", default = '1')
        setup_parser.add_argument('--nid_count', required = False, help = "Number of LNet NIDs per storage server, defaults to 1 per server", default = '1')
        setup_parser.add_argument('--volume_count', required = False, help = "Number of simulated storage devices, defaults to twice the number of servers")
        setup_parser.add_argument('--psu_count', required = False, help = "Number of simulated server Power Supply Units, defaults to one per server", default = '1')
        setup_parser.set_defaults(func = self.setup)

        register_parser = subparsers.add_parser("register", help = "Provide a secret for registration, or provide API credentials for the simulator to acquire a token itself")
        register_parser.add_argument('--secret', required = False, help = "Registration token secret")
        register_parser.add_argument('--username', required = False, help = "API username")
        register_parser.add_argument('--password', required = False, help = "API password")
        register_parser.add_argument('--create_pdu_entries', action='store_true', help = "Create PDU entries on the manager")
        register_parser.set_defaults(func = self.register)

        run_parser = subparsers.add_parser("run")
        run_parser.set_defaults(func = self.run)

        args = parser.parse_args()
        simulator = args.func(args)
        if simulator:
            self.simulator = simulator

            rpc_thread = RpcThread(self.simulator)
            rpc_thread.start()

            # Wake up periodically to handle signals, instead of going straight into join
            while not self._stopping.is_set():
                self._stopping.wait(timeout = 1)
            log.info("Running indefinitely.")

            self.simulator.join()

            rpc_thread.stop()
            rpc_thread.join()


def main():
    cli = SimulatorCli()

    def handler(*args, **kwargs):
        log.info("Stopping...")
        cli.stop()

    signal.signal(signal.SIGINT, handler)

    cli.main()


class PowerControlCli(object):
    def _setup(self):
        # This should all be reasonably thread-safe, since it's just reading
        # the JSON from disk, but talks to the server to make any changes.
        self.power = FakePowerControl(self.args.config, None, None)

    def control_server(self, args):
        self.args = args
        self._setup()

        pdu_clients = []
        for pdu in self.power.pdu_sims.values():
            klassname = "%sClient" % pdu.__class__.__name__
            pdu_clients.append(getattr(__import__("cluster_sim.fake_power_control", fromlist=[klassname]), klassname)(pdu.address, pdu.port))

        if args.fqdn.lower() == "all":
            for outlet in self.power.server_outlet_list:
                for client in pdu_clients:
                    client.perform_outlet_action(outlet, args.action)
            return

        outlet = self.power.server_outlet_number(args.fqdn)

        for client in pdu_clients:
            client.perform_outlet_action(outlet, args.action)

    def control_pdu(self, args):
        self.args = args
        self._setup()

        pdu = self.power.pdu_sims[args.name]
        klassname = "%sClient" % pdu.__class__.__name__
        client = getattr(__import__("cluster_sim.fake_power_control", fromlist=[klassname]), klassname)(pdu.address, pdu.port)
        client.perform_outlet_action(args.outlet, args.action)

    def print_server_status(self, args):
        self.args = args
        self._setup()

        for outlet in self.power.server_outlet_list:
            print self.power.outlet_server_name(outlet)
            for pdu in self.power.pdu_sims.values():
                print "  %s (%s): %s" % (pdu.name, outlet, pdu.outlet_state(outlet))

    def print_pdu_status(self, args):
        self.args = args
        self._setup()

        for pdu in sorted(self.power.pdu_sims.values(), key = lambda x: x.name):
            print "%s:" % pdu.name
            for outlet, state in sorted(pdu.all_outlet_states.items(), key = lambda x: x[0]):
                print "  %s (%s): %s" % \
                        (self.power.outlet_server_name(outlet), outlet, state)

    def main(self):
        parser = argparse.ArgumentParser(description = "Cluster Power Control")
        parser.add_argument('--config', required = False, help = "Simulator configuration/state directory", default = "cluster_sim")
        subparsers = parser.add_subparsers()

        pdu_parser = subparsers.add_parser('pdu')
        pdu_parser.add_argument('name', help = "PDU name")
        pdu_parser.add_argument('outlet', help = "PDU outlet number")
        pdu_parser.add_argument('action', help = "Action to be performed (off|on|reboot)")
        pdu_parser.set_defaults(func = self.control_pdu)

        server_parser = subparsers.add_parser('server')
        server_parser.add_argument('fqdn', help = "Server FQDN")
        server_parser.add_argument('action', help = "Action to be performed (off|on|reboot)")
        server_parser.set_defaults(func = self.control_server)

        status_parser = subparsers.add_parser('status')
        sub_subparsers = status_parser.add_subparsers()
        server_status = sub_subparsers.add_parser('servers')
        server_status.set_defaults(func = self.print_server_status)
        pdu_status = sub_subparsers.add_parser('pdus')
        pdu_status.set_defaults(func = self.print_pdu_status)

        args = parser.parse_args()
        args.func(args)


def power_main():
    cli = PowerControlCli()
    cli.main()
