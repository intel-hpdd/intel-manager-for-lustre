# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-03-10 10:30
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('chroma_core', '0015_device_devicehost'),
    ]

    operations = [
        migrations.DeleteModel(
            name='StorageResourceOffline',
        ),
    ]
