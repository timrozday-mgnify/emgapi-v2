from django.core.validators import EMPTY_VALUES
from django.db.models import Q
from django_admin_inline_paginator.admin import InlinePaginated
from unfold.admin import TabularInline
from unfold.contrib.filters.admin import TextFilter


class TabularInlinePaginatedWithTabSupport(InlinePaginated, TabularInline):
    """
    Unfold tabular inline, plus support for paginating the inline.
    Tab = true gives the inline a whole tab of its own in unfold admin page.
    """

    template = "admin/tabular_inline_paginated_tabbed.html"
    tab = True


class StudyFilter(TextFilter):
    title = "study accession"
    parameter_name = "study_accession"

    study_accession_search_fields = ["ena_study__accession"]

    def queryset(self, request, queryset):
        if self.value() in EMPTY_VALUES:
            return queryset

        filters = Q()
        for field in self.study_accession_search_fields:
            filters |= Q(**{f"{field}__icontains": self.value()})

        return queryset.filter(filters)
