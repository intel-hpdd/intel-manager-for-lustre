import mock

from django.test import TestCase

from chroma_core.models import Command
from chroma_core.services.log import log_register

log = log_register("iml_test_case")


class IMLUnitTestCase(TestCase):
    def setUp(self):
        super(IMLUnitTestCase, self).setUp()

        with open("./migrations/20200817164118_utils.sql", "r") as f:
            sql = f.read()

        with open("./migrations/20200824170157_corosync.sql", "r") as f:
            sql2 = f.read()

        with open("./migrations/20201102184120_lnet.sql", "r") as f:
            sql3 = f.read()

        with open("./migrations/20201028182332_network_interface.sql", "r") as f:
            sql4 = f.read()

        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute(sql)
            cursor.execute(sql2)
            cursor.execute(sql3)
            cursor.execute(sql4)

        mock.patch("chroma_core.services.job_scheduler.job_scheduler.LockQueue.put").start()
        mock.patch("chroma_core.services.dbutils.exit_if_in_transaction").start()

        def get_targets_fn():
            from chroma_core.models import ManagedHost

            ids = [x.id for x in ManagedHost.objects.all()]
            host_id = ids[0]

            return [
                {"name": "MGS", "active_host_id": host_id, "host_ids": ids, "uuid": "uuid_mgt"},
                {"name": "testfs-MDT0000", "active_host_id": host_id, "host_ids": ids, "uuid": "uuid_mdt"},
                {"name": "testfs-OST0000", "active_host_id": host_id, "host_ids": ids, "uuid": "uuid_ost0"},
                {"name": "testfs-OST0001", "active_host_id": host_id, "host_ids": ids, "uuid": "uuid_ost1"},
            ]

        self.get_targets_mock = mock.MagicMock(side_effect=get_targets_fn)
        mock.patch("chroma_core.lib.graphql.get_targets", new=self.get_targets_mock).start()

        self.influx_post_mock = mock.MagicMock()
        mock.patch("chroma_core.lib.influx.requests", new=self.influx_post_mock).start()

    def make_command(self, complete=False, created_at=None, errored=True, message="test"):

        """

        :param complete: Bool True when the command has completed
        :param created_at: DateTime of the creation time
        :param errored: Bool True if the command errored
        :param message: str Message associated with the command
        :return: Command The command created.
        """
        command = Command.objects.create(message=message, complete=complete, errored=errored)

        #  Command.created_at is auto_add_now - so have to update it
        if created_at is not None:
            command.created_at = created_at
            command.save()

        return command
