from django.urls import path

from . import views

urlpatterns = [
    path('start', views.facebook_login_start, name='facebook-login-start'),
    path('start/', views.facebook_login_start, name='facebook-login-start-slash'),
    path('callback', views.facebook_login_callback, name='facebook-login-callback'),
    path('callback/', views.facebook_login_callback, name='facebook-login-callback-slash'),
]
