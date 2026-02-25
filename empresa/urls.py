from django.urls import path

from . import api_views

urlpatterns = [
    path('clientes', api_views.clientes, name='empresa-clientes'),
    path('clientes/', api_views.clientes, name='empresa-clientes-slash'),
    path('clientes/<int:cliente_id>', api_views.cliente_detail, name='empresa-cliente-detail'),
    path('clientes/<int:cliente_id>/', api_views.cliente_detail, name='empresa-cliente-detail-slash'),
    path('ad-accounts', api_views.empresa_ad_accounts, name='empresa-ad-accounts'),
    path('ad-accounts/', api_views.empresa_ad_accounts, name='empresa-ad-accounts-slash'),
]
