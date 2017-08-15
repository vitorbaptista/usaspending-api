# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-15 16:11
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0009_merge_20170613_1954'),
    ]

    operations = [
        migrations.AlterField(
            model_name='historicaltransactionassistance',
            name='fain',
            field=models.TextField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='historicaltransactionassistance',
            name='uri',
            field=models.TextField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='historicaltransactioncontract',
            name='piid',
            field=models.TextField(blank=True, db_index=True, help_text='The PIID of this transaction'),
        ),
        migrations.AlterField(
            model_name='transactionassistance',
            name='fain',
            field=models.TextField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='transactionassistance',
            name='uri',
            field=models.TextField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='transactioncontract',
            name='piid',
            field=models.TextField(blank=True, db_index=True, help_text='The PIID of this transaction'),
        ),
    ]
