from django.urls import path

from . import views

urlpatterns = [
    path(f'login', views.login_user),
    path(f'logout', views.logout_user)
]