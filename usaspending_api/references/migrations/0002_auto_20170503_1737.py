# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2017-05-03 17:37
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='agency',
            name='toptier_flag',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterUniqueTogether(
            name='legalentity',
            unique_together=set([]),
        ),
    ]