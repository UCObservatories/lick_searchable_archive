from django.urls import path
from django.contrib.auth import views as auth_views
from django.conf import settings

from . import views

urlpatterns = [
    path('index.html', views.index),
    path('users/login/', auth_views.LoginView.as_view(template_name="frontend/login.html",next_page=settings.LICK_ARCHIVE_FRONTEND_URL + "/index.html"),name="login"),
    path('users/logout/', views.logout)
]
