# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-12-23 16:37
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("chroma_core", "0032_forgetlustreclientjob"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="updatejob",
            name="host",
        ),
        migrations.RemoveField(
            model_name="updatejob",
            name="job_ptr",
        ),
        migrations.DeleteModel(
            name="UpdatesAvailableAlert",
        ),
        migrations.DeleteModel(
            name="UpdateJob",
        ),
    ]
