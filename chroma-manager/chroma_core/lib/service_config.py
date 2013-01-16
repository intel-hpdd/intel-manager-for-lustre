#
# ========================================================
# Copyright (c) 2012 Whamcloud, Inc.  All rights reserved.
# ========================================================


import logging
import subprocess
import getpass
import socket
import errno
import sys
import os

log = logging.getLogger('installation')
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

from chroma_core.lib.chroma_settings import chroma_settings
settings = chroma_settings()

from django.contrib.auth.models import User, Group
from django.core.management import ManagementUtility


class RsyslogConfig:
    CONFIG_FILE = "/etc/rsyslog.d/chroma-logging.conf"
    SENTINEL = "# Added by chroma-manager\n"

    def __init__(self, config_file = None):
        self.config_file = config_file or self.CONFIG_FILE

    def remove(self):
        """Remove our config section from the rsyslog config file, or do
        nothing if our section is not there"""
        # don't bother with editing if it's a rsyslog.d file
        if self.config_file == self.CONFIG_FILE:
            try:
                os.unlink(self.config_file)
            except OSError:
                pass
            return

        rsyslog_lines = open(self.config_file).readlines()
        try:
            config_start = rsyslog_lines.index(self.SENTINEL)
            rsyslog_lines.remove(self.SENTINEL)
            config_end = rsyslog_lines.index(self.SENTINEL)
            rsyslog_lines.remove(self.SENTINEL)
            rsyslog_lines = rsyslog_lines[:config_start] + rsyslog_lines[config_end:]
            open(self.config_file, 'w').write("".join(rsyslog_lines))
        except ValueError:
            # Missing start or end sentinel, cannot remove, ignore
            pass

    def add(self, database, server, user, port = None, password = None):
        #:ompgsql:database-server,database-name,database-userid,database-password
        config_lines = [
                    self.SENTINEL,
                    "$ModLoad imtcp.so\n",
                    "$InputTCPServerRun 514\n",
                    "$ModLoad ompgsql\n",
                    "$template sqltpl,\"INSERT INTO \\\"SystemEvents\\\" (\\\"Message\\\", \\\"Facility\\\", \\\"FromHost\\\", \\\"Priority\\\", \\\"DeviceReportedTime\\\", \\\"ReceivedAt\\\", \\\"InfoUnitID\\\", \\\"SysLogTag\\\") VALUES ('%msg%', %syslogfacility%, '%HOSTNAME%', %syslogpriority%, (SELECT ('%timereported:::date-pgsql%' AT TIME ZONE '%timereported:R,ERE,0,DFLT:([+-][0-9][0-9]:[0-9][0-9])--end:date-rfc3339%') AT TIME ZONE '+00:00'), (SELECT ('%timegenerated:::date-pgsql%' AT TIME ZONE '%timereported:R,ERE,0,DFLT:([+-][0-9][0-9]:[0-9][0-9])--end:date-rfc3339%') AT TIME ZONE '+00:00'), %iut%, '%syslogtag%')\",SQL\n"]

        action_line = "*.*       :ompgsql:%s,%s,%s," % (server, database, user)
        if password:
            action_line += "%s" % password
        action_line += ";sqltpl\n"
        config_lines.append(action_line)
        config_lines.append(self.SENTINEL)

        if self.config_file == self.CONFIG_FILE:
            with open(self.config_file, 'w') as rsyslog:
                rsyslog.writelines(config_lines)
        else:
            with open(self.config_file, 'a') as rsyslog:
                rsyslog.writelines(config_lines)


class NTPConfig:
    CONFIG_FILE = "/etc/ntp.conf"
    SENTINEL = "# Added by chroma-manager\n"
    COMMENTED = "# Commented by chroma-manager: "

    def __init__(self, config_file = None):
        self.config_file = config_file or self.CONFIG_FILE

    def open_conf_for_edit(self):
        from tempfile import mkstemp
        tmp_f, tmp_name = mkstemp(dir = '/etc')
        f = open('/etc/ntp.conf', 'r')
        return tmp_f, tmp_name, f

    def close_conf(self, tmp_f, tmp_name, f):
        import os
        f.close()
        os.close(tmp_f)
        if not os.path.exists("/etc/ntp.conf.pre-chroma"):
            os.rename("/etc/ntp.conf", "/etc/ntp.conf.pre-chroma")
        os.chmod(tmp_name, 0644)
        os.rename(tmp_name, "/etc/ntp.conf")

    def remove(self):
        import os
        """Remove our config section from the ntp config file, or do
        nothing if our section is not there"""
        tmp_f, tmp_name, f = self.open_conf_for_edit()
        skip = False
        for line in f.readlines():
            if skip:
                if line == self.SENTINEL:
                    skip = False
                continue
            if line == self.SENTINEL:
                skip = True
                continue
            if line.startswith(self.COMMENTED):
                line = line[len(self.COMMENTED):]
            os.write(tmp_f, line)
        self.close_conf(tmp_f, tmp_name, f)

    def add(self, server):
        import os
        tmp_f, tmp_name, f = self.open_conf_for_edit()
        added_server = False
        for line in f.readlines():
            if line.startswith("server "):
                line = "%s%s" % (self.COMMENTED, line)
                if server != "localhost" and not added_server:
                    line = "%sserver %s\n%s%s" % (self.SENTINEL, server, self.SENTINEL, line)
                    added_server = True
            if server == "localhost" and line.startswith("#fudge"):
                line = "%s%sserver  127.127.1.0     # local clock\nfudge   127.127.1.0 stratum 10\n%s" % (line, self.SENTINEL, self.SENTINEL)
            os.write(tmp_f, line)
        self.close_conf(tmp_f, tmp_name, f)


class CommandError(Exception):
    pass


class ServiceConfig:
    def __init__(self):
        self.verbose = False

    def _check_name_resolution(self):
        """
        Check:
         * that the hostname is not localhost
         * that the FQDN can be looked up from hostname
         * that an IP can be looked up from the hostname
         * that reverse lookup on the IP gives the FQDN
         * that the host can connect to its own external IP address

        This check is done ahead of configuring RabbitMQ, which fails unobviously if the
        name resolution is bad.

        :return: True if OK, else False

        """
        try:
            hostname = socket.gethostname()
        except socket.error:
            log.error("Error: Unable to get the servers hostname. Please correct the hostname resolution.")
            return False

        if hostname == "localhost":
            log.error("Error: Currently the hostname is '%s' which is invalid. "
                      "Please correct the hostname resolution.", hostname)
            return False

        try:
            fqdn = socket.getfqdn(hostname)
        except socket.error:
            log.error("Error: Unable to get the FQDN for the server name '%s'. "
                      "Please correct the hostname resolution.", hostname)
            return False

        try:
            ip_address = socket.gethostbyname(hostname)
        except socket.error:
            log.error("Error: Unable to get the ip address for the server name '%s'. "
                      "Please correct the hostname resolution.", hostname)
            return False

        try:
            hostname_from_ip, aliaslist, ipaddr = socket.gethostbyaddr(ip_address)
        except socket.error:
            log.error("Error: Unable to get the host name for ip address: %s. "
                      "Please correct the hostname resolution.", ip_address)
            return False

        if fqdn != hostname_from_ip:
            log.error("Need to correctly setup hostname resolution. Currently the hostname is: "
                      "%s and the ip address hostname is: %s.", fqdn, hostname_from_ip)
            return False

        SSH_PORT = 22

        # Make sure that the ip address is on the machine. Using port 22 to verify since all machines should
        # have sshd running.
        try:
            tst_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tst_socket.bind((ip_address, SSH_PORT))
            tst_socket.close()
        except socket.error, err:
            (err_num, err_msg) = err
            if err_num != errno.EADDRINUSE:
                log.error("Error: Unable to connect to IP address: %s. "
                          "Please correct hostname resolution.", ip_address)
                return False

        return True

    def try_shell(self, cmdline, mystdout = subprocess.PIPE,
                  mystderr = subprocess.PIPE):
        rc, out, err = self.shell(cmdline, mystdout, mystderr)

        if rc != 0:
            log.error("Command failed: %s" % cmdline)
            log.error("returned: %s" % rc)
            log.error("stdout:\n%s" % out)
            log.error("stderr:\n%s" % err)
            raise CommandError("Command failed %s" % cmdline)
        else:
            return rc, out, err

    def shell(self, cmdline, mystdout = subprocess.PIPE,
              mystderr = subprocess.PIPE):
        p = subprocess.Popen(cmdline, stdout = mystdout, stderr = mystderr)
        out, err = p.communicate()
        rc = p.wait()
        return rc, out, err

    def _db_accessible(self):
        """Discover whether we have a working connection to the database"""
        from psycopg2 import OperationalError
        from django.db import connection
        try:
            connection.introspection.table_names()
            return True
        except OperationalError:
            connection._rollback()
            return False

    def _db_populated(self):
        """Discover whether the database has this application's tables"""
        from django.db.utils import DatabaseError
        if not self._db_accessible():
            return False
        try:
            from south.models import MigrationHistory
            MigrationHistory.objects.count()
            return True
        except DatabaseError:
            from django.db import connection
            connection._rollback()
            return False

    def _db_current(self):
        """Discover whether there are any outstanding migrations to be
           applied"""
        if not self._db_populated():
            return False

        from south.models import MigrationHistory
        applied_migrations = MigrationHistory.objects.all().values('app_name', 'migration')
        applied_migrations = [(mh['app_name'], mh['migration']) for mh in applied_migrations]

        from south import migration
        for app_migrations in list(migration.all_migrations()):
            for m in app_migrations:
                if (m.app_label(), m.name()) not in applied_migrations:
                    return False
        return True

    def _users_exist(self):
        """Discover whether any users exist in the database"""
        if not self._db_populated():
            return False

        return bool(User.objects.count() > 0)

    def configured(self):
        """Return True if the system has been configured far enough to present
        a user interface"""
        return self._db_current() and self._users_exist()

    def _setup_rsyslog(self, database):
        log.info("Writing rsyslog configuration")
        rsyslog = RsyslogConfig()
        rsyslog.remove()
        rsyslog.add(database['NAME'],
                    database['HOST'] or 'localhost',
                    database['USER'],
                    database['PORT'] or None,
                    database['PASSWORD'] or None)

    def _start_rsyslog(self):
        log.info("Restarting rsyslog")
        self.try_shell(["chkconfig", "rsyslog", "on"])
        self.try_shell(['service', 'rsyslog', 'restart'])

    def _setup_ntp(self, server = None):
        if not server:
            server = self.get_input(msg = "NTP Server", default = "localhost")
        log.info("Writing ntp configuration")
        ntp = NTPConfig()
        ntp.remove()
        ntp.add(server)
        self._start_ntp()

    def _start_ntp(self):
        log.info("Restarting ntp")
        self.try_shell(["chkconfig", "ntpd", "on"])
        self.try_shell(['service', 'ntpd', 'restart'])

    def _setup_rabbitmq(self):
        RABBITMQ_USER = "chroma"
        RABBITMQ_PASSWORD = "chroma123"
        RABBITMQ_VHOST = "chromavhost"

        log.info("Starting RabbitMQ...")
        self.try_shell(["chkconfig", "rabbitmq-server", "on"])
        # FIXME: there's really no sane reason to have to set the stderr and
        #        stdout to None here except that subprocess.PIPE ends up
        #        blocking subprocess.communicate().
        #        we need to figure out why
        self.try_shell(["service", "rabbitmq-server", "restart"],
                       mystderr = None, mystdout = None)

        rc, out, err = self.try_shell(["rabbitmqctl", "-q", "list_users"])
        users = [line.split()[0] for line in out.split("\n") if len(line)]
        if not RABBITMQ_USER in users:
            log.info("Creating RabbitMQ user...")
            self.try_shell(["rabbitmqctl", "add_user", RABBITMQ_USER, RABBITMQ_PASSWORD])

        rc, out, err = self.try_shell(["rabbitmqctl", "-q", "list_vhosts"])
        vhosts = [line.split()[0] for line in out.split("\n") if len(line)]
        if not RABBITMQ_VHOST in vhosts:
            log.info("Creating RabbitMQ vhost...")
            self.try_shell(["rabbitmqctl", "add_vhost", RABBITMQ_VHOST])

        self.try_shell(["rabbitmqctl", "set_permissions", "-p", RABBITMQ_VHOST, RABBITMQ_USER, ".*", ".*", ".*"])

    CONTROLLED_SERVICES = ['chroma-worker', 'chroma-storage', 'httpd']

    def _enable_services(self):
        log.info("Enabling Chroma daemons")
        for service in self.CONTROLLED_SERVICES:
            self.try_shell(['chkconfig', '--add', service])

    def _start_services(self):
        log.info("Starting Chroma daemons")
        for service in self.CONTROLLED_SERVICES:
            self.try_shell(['service', service, 'start'])

    def _stop_services(self):
        log.info("Stopping Chroma daemons")
        for service in self.CONTROLLED_SERVICES:
            self.try_shell(['service', service, 'stop'])

    def _init_pgsql(self, database):
        rc, out, err = self.shell(["service", "postgresql", "initdb"])
        if rc != 0:
            if 'is not empty' not in out:
                log.error("Failed to initialize postgresql service")
                log.error("stdout:\n%s" % out)
                log.error("stderr:\n%s" % err)
                raise CommandError("service postgresql initdb")
            return
        # Only mess with auth if we've freshly initialized the db
        self._config_pgsql_auth(database)

    def _config_pgsql_auth(self, database):
        auth_cfg_file = "/var/lib/pgsql/data/pg_hba.conf"
        os.rename(auth_cfg_file, "%s.dist" % auth_cfg_file)
        with open(auth_cfg_file, "w") as cfg:
            # Allow our django user to connect with no password
            cfg.write("local\tall\t%s\t\ttrust\n" % database['USER'])
            # Allow rsyslogd to connect
            cfg.write("host\tall\t%s\t127.0.0.1/32\ttrust\n" % database['USER'])
            cfg.write("host\tall\t%s\t::1/128\ttrust\n" % database['USER'])
            # Allow the system superuser (postgres) to connect
            cfg.write("local\tall\tall\t\tident\n")

    def _setup_pgsql(self, database):
        log.info("Setting up PostgreSQL service...")
        self._init_pgsql(database)
        self.try_shell(["service", "postgresql", "restart"])
        self.try_shell(["chkconfig", "postgresql", "on"])

        import time
        tries = 0
        while self.shell(["su", "postgres", "-c", "psql -c '\\d'"])[0] != 0:
            if tries >= 4:
                raise RuntimeError("Timed out waiting for PostgreSQL service to start")
            tries += 1
            time.sleep(1)

        if not self._db_accessible():
            log.info("Creating database owner '%s'...\n" % database['USER'])
            self.try_shell(["su", "postgres", "-c", "psql -c 'CREATE ROLE %s NOSUPERUSER CREATEDB NOCREATEROLE INHERIT LOGIN;'" % database['USER']])

            log.info("Creating database '%s'...\n" % database['NAME'])
            self.try_shell(["su", "postgres", "-c", "createdb -O %s %s;" % (database['USER'], database['NAME'])])

    def get_input(self, msg, empty_allowed = True, password = False, default = ""):
        if msg == "":
            raise RuntimeError("Calling get_input, msg must not be empty")

        if default != "":
            msg = "%s [%s]" % (msg, default)

        msg = "%s: " % msg

        answer = ""
        while answer == "":
            if password:
                answer = getpass.getpass(msg)
            else:
                answer = raw_input(msg)

            if answer == "":
                if not empty_allowed:
                    print "A value is required"
                    continue
                if default != "":
                    answer = default
                break

        return answer

    def get_pass(self, msg = "", empty_allowed = True, confirm_msg = ""):
        while True:
            pass1 = self.get_input(msg = msg, empty_allowed = empty_allowed,
                                   password = True)

            pass2 = self.get_input(msg = confirm_msg,
                                   empty_allowed = empty_allowed,
                                   password = True)

            if pass1 != pass2:
                print "Passwords do not match!"
            else:
                return pass1

    def _user_account_prompt(self):
        log.info("Chroma will now create an initial administrative user using the " +
                 "credentials which you provide.")

        valid_username = False
        while not valid_username:
            username = self.get_input(msg = "Chroma superuser", empty_allowed = False)
            if username.find(" ") > -1:
                print "Username cannot contain spaces"
                continue
            valid_username = True
        email = self.get_input(msg = "Email")
        password = self.get_pass(msg = "Password", empty_allowed = False,
                                     confirm_msg = "Confirm password")

        return username, email, password

    def _setup_database(self, username = None, password = None):
        if not self._db_accessible():
            # For the moment use the builtin configuration
            # TODO: this is where we would establish DB name and credentials
            databases = settings.DATABASES
            self._setup_pgsql(databases['default'])
            self._setup_rsyslog(databases['default'])
        else:
            log.info("DB already accessible")

        if not self._db_current():
            log.info("Creating database tables...")
            args = ['', 'syncdb', '--noinput', '--migrate']
            if not self.verbose:
                args = args + ["--verbosity", "0"]
            ManagementUtility(args).execute()
        else:
            log.info("Database tables already OK")

        # Don't (re-)start rsyslog until AFTER the SystemEvents table exists
        self._start_rsyslog()

        if not self._users_exist():
            if not username:
                username, email, password = self._user_account_prompt()
            else:
                email = ""
            user = User.objects.create_superuser(username, email, password)
            user.groups.add(Group.objects.get(name='superusers'))
            log.info("User '%s' successfully created." % username)
        else:
            log.info("User accounts already created")

        # FIXME: we do this here because running management commands requires a working database,
        # but that shouldn't be so (ideally the /static/ dir would be built into the RPM)
        # (Django ticket #17656)
        log.info("Building static directory...")
        args = ['', 'collectstatic', '--noinput']
        if not self.verbose:
            args = args + ["--verbosity", "0"]
        ManagementUtility(args).execute()

    def setup(self, username = None, password = None, ntp_server = None):
        if not self._check_name_resolution():
            return ["Name resolution is not correctly configured"]

        self._setup_database(username, password)
        self._setup_ntp(ntp_server)
        self._setup_rabbitmq()
        self._enable_services()

        self._start_services()

        return self.validate()

    def start(self):
        if not self._db_current():
            log.error("Cannot start, database not configured")
            return
        self._start_services()

    def stop(self):
        self._stop_services()

    def _service_config(self, interesting_services = None):
        """Interrogate the current status of services"""
        log.info("Checking service configuration...")

        rc, out, err = self.try_shell(['chkconfig', '--list'])
        services = {}
        for line in out.split("\n"):
            if not line:
                continue

            tokens = line.split()
            service_name = tokens[0]
            if interesting_services and service_name not in interesting_services:
                continue

            enabled = (tokens[4][2:] == 'on')

            rc, out, err = self.shell(['service', service_name, 'status'])
            running = (rc == 0)

            services[service_name] = {'enabled': enabled, 'running': running}
        return services

    def validate(self):
        errors = []
        if not self._db_accessible():
            errors.append("Cannot connect to database")
        elif not self._db_current():
            errors.append("Database tables out of date")
        elif not self._users_exist():
            errors.append("No user accounts exist")

        interesting_services = self.CONTROLLED_SERVICES + ['postgresql', 'rsyslog', 'rabbitmq-server']
        service_config = self._service_config(interesting_services)
        for s in interesting_services:
            try:
                service_status = service_config[s]
                if not service_status['enabled']:
                    errors.append("Service %s not set to start at boot" % s)
                if not service_status['running']:
                    errors.append("Service %s is not running" % s)
            except KeyError:
                errors.append("Service %s not found" % s)

        return errors

    def _write_local_settings(self, databases):
        # Build a local_settings file
        import os
        project_dir = os.path.dirname(os.path.realpath(settings.__file__))
        local_settings = os.path.join(project_dir, settings.LOCAL_SETTINGS_FILE)
        local_settings_str = ""
        local_settings_str += "CELERY_RESULT_BACKEND = \"database\"\n"
        local_settings_str += "CELERY_RESULT_DBURI = \"postgresql://%s:%s@%s%s/%s\"\n" % (
                databases['default']['USER'],
                databases['default']['PASSWORD'],
                databases['default']['HOST'] or "localhost",
                ":%d" % databases['default']['PORT'] if databases['default']['PORT'] else "",
                databases['default']['NAME'])

        # Usefully, a JSON dict looks a lot like python
        import json
        local_settings_str += "DATABASES = %s\n" % json.dumps(databases, indent=4).replace("null", "None")

        # Dump local_settings_str to local_settings
        open(local_settings, 'w').write(local_settings_str)

        # TODO: support SERVER_HTTP_URL
        # TODO: support LOG_SERVER_HOSTNAME


def chroma_config():
    """Entry point for chroma-config command line tool.

    Distinction between this and ServiceConfig is that CLI-specific stuff lives here:
    ServiceConfig utility methods don't do sys.exit or parse arguments.

    """
    service_config = ServiceConfig()
    try:
        command = sys.argv[1]
    except IndexError:
        log.error("Usage: %s <setup|validate|start|restart|stop>" % sys.argv[0])
        sys.exit(-1)

    def print_errors(errors):
        if errors:
            log.error("Errors found:")
            for error in errors:
                log.error("  * %s" % error)
        else:
            log.info("OK.")

    if command == 'setup':
        def usage():
            log.error("Usage: setup [-v] [username password ntpserver]")
            sys.exit(-1)

        args = []
        if len(sys.argv) > 2:
            if sys.argv[2] == "-v":
                service_config.verbose = True
                if len(sys.argv) == 6:
                    args = sys.argv[3:6]
                elif len(sys.argv) != 3:
                    usage()
            elif len(sys.argv) == 5:
                args = sys.argv[2:5]
            else:
                usage()

        log.info("\nStarting the installation...\n")
        errors = service_config.setup(*args)
        if errors:
            print_errors(errors)
            sys.exit(-1)
        else:
            log.info("\nThe installation was SUCCESSFUL!\n")
            sys.exit(0)
    elif command == 'validate':
        errors = service_config.validate()
        print_errors(errors)
        if errors:
            sys.exit(1)
        else:
            sys.exit(0)
    elif command == 'stop':
        service_config.stop()
    elif command == 'start':
        service_config.start()
    elif command == 'restart':
        service_config.stop()
        service_config.start()
    else:
        log.error("Invalid command '%s'" % command)
        sys.exit(-1)
