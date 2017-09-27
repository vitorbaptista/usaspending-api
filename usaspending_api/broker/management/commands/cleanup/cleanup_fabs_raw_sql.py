import logging
import timeit

from django.core.management.base import BaseCommand
from django.db import connection, transaction

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.etl.award_helpers import update_awards, update_contract_awards, update_award_categories

# start = timeit.default_timer()
# function_call
# end = timeit.default_timer()
# time elapsed = str(end - start)


logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")


BROKER_RAW_SQL = "UPDATE transaction_fabs AS all_pafa SET is_active=TRUE " \
                 "FROM (SELECT DISTINCT pafa.published_award_financial_assistance_id " \
                 "FROM transaction_fabs AS pafa " \
                 "INNER JOIN (SELECT max(modified_at) AS modified_at, afa_generated_unique " \
                 "FROM transaction_fabs GROUP BY afa_generated_unique) sub_pafa " \
                 "ON pafa.modified_at = sub_pafa.modified_at AND " \
                 "COALESCE(pafa.afa_generated_unique, '') = COALESCE(sub_pafa.afa_generated_unique, '') " \
                 "WHERE COALESCE(UPPER(pafa.correction_late_delete_ind), '') != 'D') AS selected " \
                 "WHERE all_pafa.published_award_financial_assistance_id = " \
                 "selected.published_award_financial_assistance_id"


class Command(BaseCommand):
    help = "Updates FABS data based on the SQL query provided by the DATA Broker"

    def add_arguments(self, parser):
        parser.add_argument(
            '--fiscal_year',
            dest="fiscal_year",
            nargs='+',
            type=int,
            help="Year for which to run the historical load"
        )

        parser.add_argument(
            '--all',
            action='store_true',
            dest='all',
            default=False,
            help='Runs the award updates on all records'
        )

    @transaction.atomic
    def handle(self, *args, **options):
        logger.info('Starting updates to FABS data...')
        start = timeit.default_timer()

        all_records_flag = options.get('all')
        fiscal_year = options.get('fiscal_year')

        db_cursor = connection.cursor()
        extra_where = ''
        arguments = []

        if not all_records_flag:
            if fiscal_year:
                fiscal_year = fiscal_year[0]
            else:
                fiscal_year = 2017
            logger.info('Processing FABS data for Fiscal Year ' + str(fiscal_year))

            fy_begin = '10/01/' + str(fiscal_year - 1)
            fy_end = '09/30/' + str(fiscal_year)

            extra_where = " AND action_date::Date BETWEEN %s AND %s"
            arguments += [fy_begin, fy_end]
        else:
            logger.info('Processing all FABS data...')

        update_query = BROKER_RAW_SQL + extra_where
        logger.info("Executing query on DB => " + update_query % tuple(arguments))
        db_cursor.execute(update_query, arguments)

        delete_query = "DELETE FROM transaction_fabs WHERE is_active=FALSE" + extra_where
        logger.info("Executing query on DB => " + delete_query % tuple(arguments))

        db_cursor.execute(delete_query)

        end = timeit.default_timer()
        logger.info('Finished updating FABS data in ' + str(end - start) + ' seconds')

        # Done!
        logger.info('FINISHED')
