from django.contrib.postgres.search import SearchQuery, SearchRank, SearchVector
from rest_framework.decorators import api_view
from rest_framework.response import Response
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.common.exceptions import status


# SearchRank ranks a non-matching result with 1e-20
SEARCH_RANK_NO_MATCH = 0.00000000000000000001


@api_view(['GET'])
def budget_function_autocomplete_view(request):
    """Return all award spending by award type for a given fiscal year and agency id"""

    json_request = request.query_params

    # retrieve search_text from request
    search_text = json_request.get('search_text', None)

    # required query parameters were not provided
    if not search_text:
        return Response({'message': 'Missing one or more required query parameters: search_text'},
                        status=status.HTTP_400_BAD_REQUEST)

    queryset = TreasuryAppropriationAccount.objects.all()

    # Filter based on search text
    response = {}

    function_vector = SearchVector('budget_function_title')
    subfunction_vector = SearchVector('budget_subfunction_title')
    query = SearchQuery(search_text)

    function_results = queryset.annotate(rank=SearchRank(function_vector, query)).values('budget_function_title').\
        distinct().filter(rank__gt=SEARCH_RANK_NO_MATCH).order_by('-rank')

    subfunction_results = queryset.annotate(rank=SearchRank(subfunction_vector, query)).\
        values('budget_subfunction_title').distinct().filter(rank__gt=SEARCH_RANK_NO_MATCH).order_by('-rank')

    function_titles = function_results.values_list('budget_function_title', flat=True)
    subfunction_titles = subfunction_results.values_list('budget_subfunction_title', flat=True)

    response['results'] = {'budget_function_title': function_titles,
                           'budget_subfunction_title': subfunction_titles}

    response['counts'] = {'budget_function_title': len(function_titles),
                          'budget_subfunction_title': len(subfunction_titles)}

    return Response(response)
