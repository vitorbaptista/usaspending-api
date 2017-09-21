from datetime import datetime
import logging
import re
import signal
import sys
import time
from copy import copy

from django.core.management import call_command
from django.db import connections, transaction
from django.db.models import Q
from django.core.cache import caches
import pandas as pd
import numpy as np

from usaspending_api.accounts.models import (
    AppropriationAccountBalances, AppropriationAccountBalancesQuarterly,
    TreasuryAppropriationAccount)
from usaspending_api.awards.models import (
    Award, FinancialAccountsByAwards,
    TransactionAssistance, Transaction)
from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass, TasProgramActivityObjectClassQuarterly)
from usaspending_api.references.models import (
    Agency, LegalEntity, ObjectClass, Cfda, RefProgramActivity, Location)
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.etl.award_helpers import (
    get_award_financial_transaction, get_awarding_agency)
from usaspending_api.etl.helpers import get_fiscal_quarter, get_previous_submission
from usaspending_api.etl.broker_etl_helpers import dictfetchall, PhonyCursor
from usaspending_api.etl.subaward_etl import load_subawards

from usaspending_api.etl.management import load_base
from usaspending_api.etl.management.load_base import format_date, load_data_into_model

# This dictionary will hold a map of tas_id -> treasury_account to ensure we don't
# keep hitting the databroker DB for account data
TAS_ID_TO_ACCOUNT = {}

# Lists to store for update_awards and update_contract_awards
AWARD_UPDATE_ID_LIST = []

awards_cache = caches['awards']
logger = logging.getLogger('console')


class Command(load_base.Command):
    """
    This command will load a single submission from the DATA Act broker. If
    we've already loaded the specified broker submisison, this command
    will remove the existing records before loading them again.
    """
    help = "Loads a single submission from the DATA Act broker. The DATA_BROKER_DATABASE_URL environment variable \
                must set so we can pull submission data from their db."

    def add_arguments(self, parser):
        parser.add_argument('submission_id', nargs=1, help='the data broker submission id to load', type=int)
        super(Command, self).add_arguments(parser)

    @transaction.atomic
    def handle_loading(self, db_cursor, *args, **options):



        def signal_handler(signal, frame):
            transaction.set_rollback(True)
            raise Exception('Received interrupt signal. Aborting...')

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        submission_id = options['submission_id'][0]

        logger.info('Getting submission from broker')
        # Verify the ID exists in the database, and grab the data
        db_cursor.execute('SELECT * FROM submission WHERE submission_id = %s', [submission_id])
        submission_data = dictfetchall(db_cursor)
        logger.info('Finished getting submission from broker')

        if len(submission_data) == 0:
            raise 'Could not find submission with id ' + str(submission_id)
        elif len(submission_data) > 1:
            raise 'Found multiple submissions with id ' + str(submission_id)

        # We have a single submission, which is what we want
        submission_data = submission_data[0]
        broker_submission_id = submission_data['submission_id']
        del submission_data['submission_id']  # To avoid collisions with the newer PK system
        submission_attributes = get_submission_attributes(broker_submission_id, submission_data)

        logger.info('Getting File D2 data')
        # File D2
        db_cursor.execute('SELECT * FROM award_financial_assistance WHERE submission_id = %s', [submission_id])
        award_financial_assistance_data = dictfetchall(db_cursor)
        logger.info('Acquired award financial assistance data for ' + str(submission_id) + ', there are ' + str(
            len(award_financial_assistance_data)) + ' rows.')
        logger.info('Loading File D2 data')
        start_time = datetime.now()

        load_file_d2(submission_attributes, award_financial_assistance_data, db_cursor)
        logger.info('Finished loading File D2 data, took {}'.format(datetime.now() - start_time))

        logger.info('Getting File D1 data')
        # File D1
        db_cursor.execute('SELECT * FROM award_procurement WHERE submission_id = %s', [submission_id])
        procurement_data = dictfetchall(db_cursor)
        logger.info('Acquired award procurement data for ' + str(submission_id) + ', there are ' + str(
            len(procurement_data)) + ' rows.')
        logger.info('Loading File D1 data')
        start_time = datetime.now()
        load_file_d1(submission_attributes, procurement_data, db_cursor)
        logger.info('Finished loading File D1 data, took {}'.format(datetime.now() - start_time))


def load_file_d1(submission_attributes, procurement_data, db_cursor, quick=False):
    """
    Process and load file D1 broker data (contract award txns).
    """

    legal_entity_location_field_map = {
        "address_line1": "legal_entity_address_line1",
        "address_line2": "legal_entity_address_line2",
        "address_line3": "legal_entity_address_line3",
        "location_country_code": "legal_entity_country_code",
        "city_name": "legal_entity_city_name",
        "congressional_code": "legal_entity_congressional",
        "state_code": "legal_entity_state_code",
        "zip4": "legal_entity_zip4"
    }

    place_of_performance_field_map = {
        # not sure place_of_performance_locat maps exactly to city name
        "city_name": "place_of_performance_locat",
        "congressional_code": "place_of_performance_congr",
        "state_code": "place_of_performance_state",
        "zip4": "place_of_performance_zip4a",
        "location_country_code": "place_of_perform_country_c"
    }

    place_of_performance_value_map = {
        "place_of_performance_flag": True
    }

    legal_entity_location_value_map = {
        "recipient_flag": True
    }



    d_start_time = time.time()

    total_rows = len(procurement_data)

    start_time = datetime.now()
    for index, row in enumerate(procurement_data, 1):
        if not (index % 100):
            logger.info('D1 File Load: Loading row {} of {} ({})'.format(str(index),
                                                                         str(total_rows),
                                                                         datetime.now() - start_time))

        legal_entity_location, rec_created = load_base.get_or_create_location(
            legal_entity_location_field_map, row, copy(legal_entity_location_value_map)
        )

        # Create the place of performance location
        pop_location, pop_created = load_base.get_or_create_location(
            place_of_performance_field_map, row, place_of_performance_value_map
        )

        piid = row['piid']
        transaction = None
        if piid:
            transaction = Transaction.objects.filter(contract_data__piid=piid,
                                                     modification_number=row['award_modification_amendme']).first()
        if not transaction:
            logger.warning("The transaction associated with this row {} doesnt exist in the db".format(index+1))
            continue

        if transaction is None:
            logger.warning("The transaction from row {} doesnt exist in the db".format(index+1))
            continue

        old_pop = None
        old_rec = None
        if pop_created:
            old_pop = transaction.place_of_performance
        if rec_created:
            old_rec = transaction.recipient.location

        transaction.place_of_performance = pop_location
        transaction.recipient.location = legal_entity_location

        award = transaction.award
        award_ppop = award.place_of_performance
        award_recipient_location = award.recipient.location

        award = transaction.award
        if not award:
            logger.warning("The award associated from the transaction doesnt exist in the db".format(transaction))
            continue

        if ((award_ppop is None or award.latest_transaction == transaction) and
                    award_ppop != transaction.place_of_performance):
            award.place_of_performance = pop_location

        if ((award_recipient_location is None or award.latest_transaction == transaction) and
                    award_recipient_location != transaction.recipient.location):
            award.recipient.location = legal_entity_location

        if old_pop:
            old_pop.delete()
        if old_rec:
            old_rec.delete()

        transaction.save()
        award.recipient.save()
        award.save()

    logger.info('\n\n\n\nFile D1 time elapsed: {}'.format(time.time() - d_start_time))


def load_file_d2(
        submission_attributes, award_financial_assistance_data, db_cursor, quick=False):
    """
    Process and load file D2 broker data (financial assistance award txns).
    """

    d_start_time = time.time()

    legal_entity_location_field_map = {
        "address_line1": "legal_entity_address_line1",
        "address_line2": "legal_entity_address_line2",
        "address_line3": "legal_entity_address_line3",
        "city_code": "legal_entity_city_code",
        "city_name": "legal_entity_city_name",
        "congressional_code": "legal_entity_congressional",
        "county_code": "legal_entity_county_code",
        "county_name": "legal_entity_county_name",
        "foreign_city_name": "legal_entity_foreign_city",
        "foreign_postal_code": "legal_entity_foreign_posta",
        "foreign_province": "legal_entity_foreign_provi",
        "state_code": "legal_entity_state_code",
        "state_name": "legal_entity_state_name",
        "zip5": "legal_entity_zip5",
        "zip_last4": "legal_entity_zip_last4",
        "location_country_code": "legal_entity_country_code"
    }

    place_of_performance_field_map = {
        "city_name": "place_of_performance_city",
        "performance_code": "place_of_performance_code",
        "congressional_code": "place_of_performance_congr",
        "county_name": "place_of_perform_county_na",
        "foreign_location_description": "place_of_performance_forei",
        "state_name": "place_of_perform_state_nam",
        "zip4": "place_of_performance_zip4a",
        "location_country_code": "place_of_perform_country_c"

    }

    legal_entity_location_value_map = {
        "recipient_flag": True
    }

    place_of_performance_value_map = {
        "place_of_performance_flag": True
    }

    fad_field_map = {
        "type": "assistance_type",
        "description": "award_description",
    }

    total_rows = len(award_financial_assistance_data)

    start_time = datetime.now()
    for index, row in enumerate(award_financial_assistance_data, 1):
        if not (index % 100):
            logger.info('D2 File Load: Loading row {} of {} ({})'.format(str(index),
                                                                         str(total_rows),
                                                                         datetime.now() - start_time))

        legal_entity_location, rec_created = load_base.get_or_create_location(
            legal_entity_location_field_map, row, copy(legal_entity_location_value_map)
        )

        # Create the place of performance location
        pop_location, pop_created = load_base.get_or_create_location(
            place_of_performance_field_map, row, place_of_performance_value_map
        )

        fain = row['fain']
        uri = row['uri']
        transaction = None
        if fain or uri:
            transaction = Transaction.objects.filter(assistance_data__fain=fain, assistance_data__uri=uri,
                                                     modification_number=row['award_modification_amendme']).first()
        if not transaction:
            logger.warning("The transaction associated with this row {} doesnt exist in the db".format(index+1))
            continue

        old_pop = None
        old_rec = None
        if pop_created:
            old_pop = transaction.place_of_performance
        if rec_created:
            old_rec = transaction.recipient.location

        transaction.place_of_performance = pop_location
        transaction.recipient.location = legal_entity_location

        award = transaction.award
        award_ppop = award.place_of_performance
        award_recipient_location = award.recipient.location
        if not award:
            logger.warning("The award associated from the transaction doesnt exist in the db".format(transaction))
            continue

        if ((award_ppop is None or award.latest_transaction == transaction) and
                    award_ppop != transaction.place_of_performance):
            award.place_of_performance = pop_location

        if ((award_recipient_location is None or award.latest_transaction == transaction) and
                    award_recipient_location != transaction.recipient.location):
            award.recipient.location = legal_entity_location

        if old_pop:
            Location.objects.filter(location_id=old_pop.location_id).delete()
        if old_rec:
            Location.objects.filter(location_id=old_rec.location_id).delete()

        transaction.save()
        award.recipient.save()
        award.save()

    logger.info('\n\n\n\nFile D2 time elapsed: {}'.format(time.time() - d_start_time))

def get_submission_attributes(broker_submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending
    submission record or create and return a new one.
    """
    # check if we already have an entry for this broker submission id; if not, create one
    submission_attributes = SubmissionAttributes.objects.get(
        broker_submission_id=broker_submission_id)

    logger.info("Merging CGAC and FREC columns")
    submission_data["cgac_code"] = submission_data["cgac_code"] if submission_data["cgac_code"] else submission_data["frec_code"]

    # Find the previous submission for this CGAC and fiscal year (if there is one)
    previous_submission = get_previous_submission(
        submission_data['cgac_code'],
        submission_data['reporting_fiscal_year'],
        submission_data['reporting_fiscal_period'])

    # Update and save submission attributes
    field_map = {
        'reporting_period_start': 'reporting_start_date',
        'reporting_period_end': 'reporting_end_date',
        'quarter_format_flag': 'is_quarter_format',
    }

    # Create our value map - specific data to load
    value_map = {
        'broker_submission_id': broker_submission_id,
        'reporting_fiscal_quarter': get_fiscal_quarter(
            submission_data['reporting_fiscal_period']),
        'previous_submission': None if previous_submission is None else previous_submission,
        # pull in broker's last update date to use as certified date
        'certified_date': submission_data['updated_at'].date() if type(
            submission_data['updated_at']) == datetime else None,
    }

    return load_data_into_model(
        submission_attributes, submission_data,
        field_map=field_map, value_map=value_map, save=True)