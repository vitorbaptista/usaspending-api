from django.db.models import F, Sum, CharField, Value
from datetime import datetime

from usaspending_api.spending.v2.filters.fy_filter import fy_filter
from usaspending_api.spending.v2.views.recipient import recipient_budget


def object_class_budget(queryset):
    fiscal_year = fy_filter(datetime.now().date())
    # Object Classes Queryset
    object_classes = queryset.annotate(
        id=F('financial_accounts_by_awards_id'),
        type=Value('object_class', output_field=CharField()),
        name=F('object_class__major_object_class_name'),
        code=F('object_class__major_object_class'),
        amount=F('obligations_incurred_total_by_award_cpe')
    ).values(
        'id', 'type', 'name', 'code', 'amount').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    object_classes_total = object_classes.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in object_classes_total.items():
        object_classes_total = value

    # Unpack recipient results
    recipients_results, award_category_results, awards_results, awarding_top_tier_agencies_results,\
        awarding_sub_tier_agencies_results = recipient_budget(queryset)

    object_classes_results = {
        'total': object_classes_total,
        'end_date': fiscal_year,
        'results': object_classes
    }
    results = [
        object_classes_results,
        recipients_results,
        award_category_results,
        awards_results,
        awarding_top_tier_agencies_results,
        awarding_sub_tier_agencies_results
    ]
    return results