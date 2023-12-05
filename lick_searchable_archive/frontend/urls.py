from django.urls import path
from django.contrib.auth import views as auth_views
from lick_archive.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from . import views

urlpatterns = [
    path('index.html', views.index),
    path('users/login/', auth_views.LoginView.as_view(template_name="frontend/login.html",next_page=lick_archive_config.frontend.frontend_url + "/index.html"),name="login"),
    path('users/logout/', views.logout)
]
