from django.urls import path

from . import api_views


urlpatterns = [
    path('analysis', api_views.statistics_analysis, name='statistics-analysis'),
    path('analysis/', api_views.statistics_analysis, name='statistics-analysis-slash'),
    path('clustering', api_views.statistics_clustering, name='statistics-clustering'),
    path('clustering/', api_views.statistics_clustering, name='statistics-clustering-slash'),
]
