from django.urls import path

from . import api_views

urlpatterns = [
    path('connection-status', api_views.meta_connection_status, name='meta-connection-status'),
    path('connection-status/', api_views.meta_connection_status, name='meta-connection-status-slash'),
    path('filters', api_views.meta_filters, name='meta-filters'),
    path('filters/', api_views.meta_filters, name='meta-filters-slash'),
    path('timeseries', api_views.meta_timeseries, name='meta-timeseries'),
    path('timeseries/', api_views.meta_timeseries, name='meta-timeseries-slash'),
    path('kpis', api_views.meta_kpis, name='meta-kpis'),
    path('kpis/', api_views.meta_kpis, name='meta-kpis-slash'),
    path('sync/start', api_views.meta_sync_start, name='meta-sync-start'),
    path('sync/start/', api_views.meta_sync_start, name='meta-sync-start-slash'),
    path('sync/start/meta', api_views.meta_sync_start_meta, name='meta-sync-start-meta'),
    path('sync/start/meta/', api_views.meta_sync_start_meta, name='meta-sync-start-meta-slash'),
    path('sync/start/instagram', api_views.meta_sync_start_instagram, name='meta-sync-start-instagram'),
    path('sync/start/instagram/', api_views.meta_sync_start_instagram, name='meta-sync-start-instagram-slash'),
    path('sync/start/insights-7d', api_views.meta_sync_start_insights_7d, name='meta-sync-start-insights-7d'),
    path(
        'sync/start/insights-7d/',
        api_views.meta_sync_start_insights_7d,
        name='meta-sync-start-insights-7d-slash',
    ),
    path('sync/<int:sync_run_id>/logs', api_views.meta_sync_logs, name='meta-sync-logs'),
    path('sync/<int:sync_run_id>/logs/', api_views.meta_sync_logs, name='meta-sync-logs-slash'),
]
