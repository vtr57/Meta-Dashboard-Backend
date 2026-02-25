from django.urls import path

from . import views

urlpatterns = [
    path('login/', views.auth_login, name='auth-login'),
    path('logout/', views.auth_logout, name='auth-logout'),
    path('me/', views.auth_me, name='auth-me'),
]
