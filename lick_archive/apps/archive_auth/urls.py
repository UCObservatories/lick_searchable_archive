from django.urls import path

from . import views

urlpatterns = [
    path(f'api/login', views.login_user),
    path(f'api/logout', views.logout_user),
    path(f'api/get_csrf_token', views.get_csrf_token)
]