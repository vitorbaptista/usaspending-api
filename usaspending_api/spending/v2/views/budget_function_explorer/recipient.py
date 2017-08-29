from django.db.models import F, Sum

from usaspending_api.spending.v2.views.budget_function_explorer.award import award_category


def recipient_budget(queryset, fiscal_year):
    # Recipients Queryset
    recipients = queryset.annotate(
        major_object_class_code=F('object_class__major_object_class'),
        recipient_name=F('award__recipient__recipient_name'),
        recipient_unique_id=F('award__recipient__recipient_unique_id')
    ).values(
        'major_object_class_code', 'recipient_name', 'recipient_unique_id').annotate(
        total=Sum('obligations_incurred_total_by_award_cpe')).order_by('-total')

    recipients_total = recipients.aggregate(Sum('obligations_incurred_total_by_award_cpe'))
    for key, value in recipients_total.items():
        recipients_total = value
    # Unpack award results
    award_category_results = award_category(queryset, fiscal_year)

    recipients_results = {
        'count': recipients.count(),
        'total': recipients_total,
        'end_date': fiscal_year,
        'recipients': recipients,
        'award_category': award_category_results
    }
    results = [
        recipients_results
    ]
    return results