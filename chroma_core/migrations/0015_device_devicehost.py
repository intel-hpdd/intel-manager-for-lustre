# -*- coding: utf-8 -*-
# Generated by Django 1.11.23 on 2020-03-10 10:29
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('chroma_core', '0014_device_devicehost'),
    ]

    operations = [
        migrations.DeleteModel(
            name='StorageResourceAlert',
        ),
    ]
