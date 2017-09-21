import datetime
from decimal import Decimal
from django.core.management import call_command, CommandError

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards, TransactionAssistance,
    TransactionContract, Transaction)
from usaspending_api.etl.management.commands.load_submission import get_submission_attributes, get_or_create_program_activity
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import LegalEntity, Location, RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes

import pytest


@pytest.fixture()
def endpoint_data():
    call_command('flush', '--noinput')
    call_command('loaddata', 'endpoint_fixture_db')


@pytest.fixture()
@pytest.mark.django_db
def partially_flushed():
    TransactionContract.objects.all().delete()
    SubmissionAttributes.objects.all().delete()
    AppropriationAccountBalances.objects.all().delete()
    FinancialAccountsByProgramActivityObjectClass.objects.all().delete()
    FinancialAccountsByAwards.objects.all().delete()
    TransactionAssistance.objects.all().delete()
    Transaction.objects.all().delete()
    Location.objects.all().delete()
    LegalEntity.objects.all().delete()
    Award.objects.all().delete()

@pytest.mark.django_db
def test_update_description_fields(endpoint_data, partially_flushed):
    """
    Test the submission loader to validate the ETL process
    """
    # Load the RefObjClass and ProgramActivityCode data
    call_command('load_submission', '-1', '--test')
    ppop_count = Location.objects.filter(place_of_performance_flag=True).count()
    rec_count = Location.objects.filter(recipient_flag=True).count()
    total_location_count = Location.objects.all().count()
    for location in Location.objects.all():
        location.place_of_performance_flag = False
        location.recipient_flag = False
        location.save()
    assert Location.objects.filter(place_of_performance_flag=True).count() == 0
    call_command('update_submission_locations', '-1', '--test')
    assert ppop_count == Location.objects.filter(place_of_performance_flag=True).count()
    assert rec_count == Location.objects.filter(recipient_flag=True).count()
    assert total_location_count == Location.objects.all().count()