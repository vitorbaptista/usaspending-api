# -*- coding: utf-8 -*-
# Generated by Django 1.10.7 on 2017-08-14 19:17
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0017_Adding_indeces_for_location_and_legal_entity'),
    ]

    operations = [
        migrations.AddField(
            model_name='legalentity',
            name='test_unique_id',
            field=models.TextField(blank=True, null=True, verbose_name='DUNS Number'),
        ),
    ]