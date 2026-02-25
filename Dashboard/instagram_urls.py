from django.urls import path

from . import api_views

urlpatterns = [
    path('accounts', api_views.instagram_accounts, name='instagram-accounts'),
    path('accounts/', api_views.instagram_accounts, name='instagram-accounts-slash'),
    path('kpis', api_views.instagram_kpis, name='instagram-kpis'),
    path('kpis/', api_views.instagram_kpis, name='instagram-kpis-slash'),
    path('media-table', api_views.instagram_media_table, name='instagram-media-table'),
    path('media-table/', api_views.instagram_media_table, name='instagram-media-table-slash'),
]
