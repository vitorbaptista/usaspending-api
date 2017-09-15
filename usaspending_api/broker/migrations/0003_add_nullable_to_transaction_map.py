# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-30 16:40
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('broker', '0002_create_transaction'),
    ]

    operations = [
        migrations.AlterField(
            model_name='transactionmap',
            name='transaction_assistance_id',
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='transactionmap',
            name='transaction_contract_id',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
