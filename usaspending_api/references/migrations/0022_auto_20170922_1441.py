# -*- coding: utf-8 -*-
# Generated by Django 1.11.4 on 2017-09-22 14:41
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0021_auto_20170907_1421'),
    ]

    operations = [
        migrations.AddField(
            model_name='location',
            name='location_unique',
            field=models.TextField(blank=True, db_index=True, null=True),
        ),
        migrations.AlterField(
            model_name='location',
            name='address_line1',
            field=models.TextField(blank=True, null=True, verbose_name='Address Line 1'),
        ),
        migrations.AlterField(
            model_name='location',
            name='address_line2',
            field=models.TextField(blank=True, null=True, verbose_name='Address Line 2'),
        ),
        migrations.AlterField(
            model_name='location',
            name='address_line3',
            field=models.TextField(blank=True, null=True, verbose_name='Address Line 3'),
        ),
        migrations.AlterField(
            model_name='location',
            name='city_name',
            field=models.TextField(blank=True, null=True, verbose_name='City Name'),
        ),
        migrations.AlterField(
            model_name='location',
            name='congressional_code',
            field=models.TextField(blank=True, null=True, verbose_name='Congressional District Code'),
        ),
        migrations.AlterField(
            model_name='location',
            name='country_name',
            field=models.TextField(blank=True, null=True, verbose_name='Country Name'),
        ),
        migrations.AlterField(
            model_name='location',
            name='county_code',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='location',
            name='county_name',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='location',
            name='state_code',
            field=models.TextField(blank=True, null=True, verbose_name='State Code'),
        ),
        migrations.AlterField(
            model_name='location',
            name='zip4',
            field=models.TextField(blank=True, null=True, verbose_name='ZIP+4'),
        ),
        migrations.AlterField(
            model_name='location',
            name='zip5',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='location',
            name='zip_4a',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='location',
            name='zip_last4',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterUniqueTogether(
            name='location',
            unique_together=set([]),
        ),
    ]