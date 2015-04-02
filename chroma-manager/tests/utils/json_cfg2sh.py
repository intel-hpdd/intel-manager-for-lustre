import json
import sys

f = open(sys.argv[1])
config = json.load(f)
f.close()

servers = []
workers = []
for server in config["lustre_servers"]:
    if server.get("profile") and \
       server["profile"] == "posix_copytool_worker":
        workers.append(server["address"])
    else:
        servers.append(server["address"])

all_nodes = [config["chroma_managers"][0]["address"]] + servers

print "CHROMA_MANAGER=\"%s\"" % config["chroma_managers"][0]["address"]
print "STORAGE_APPLIANCES=(%s)" % " ".join(servers)
print "WORKERS=(%s)" % " ".join(workers)

if config.get('lustre_clients'):
    print "CLIENT_1=\"%s\"" % config["lustre_clients"][0]['address']
    all_nodes.append(config["lustre_clients"][0]['address'])

if config.get("test_runners"):
    print "TEST_RUNNER=\"%s\"" % config["test_runners"][0]['address']
    all_nodes.append(config["test_runners"][0]['address'])

print "HOST_IP=\"%s\"" % config["hosts"].values()[0]['ip_address']  # This will have to change in case cluster has multiple hosts for VMs, but an adequate placeholder for this version of the prototype.

print "ALL_NODES=\"%s\"" % " ".join(list(set(all_nodes)))

chroma_manager = config["chroma_managers"][0]
user = chroma_manager["users"][0]
print "CHROMA_USER=\"%s\"" % user["username"]
print "CHROMA_PASS=\"%s\"" % user["password"]
if user.get("email"):
    print "CHROMA_EMAIL=\"%s\"" % user["email"]
if chroma_manager.get("ntp_server"):
    print "CHROMA_NTP_SERVER=\"%s\"" % chroma_manager["ntp_server"]
print "INSTALLER_PATH=\"%s\"" % config.get('installer_path', '/tmp')
