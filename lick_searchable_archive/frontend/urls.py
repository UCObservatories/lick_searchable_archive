from django.urls import path
from django.contrib.auth import views as auth_views
from lick_archive.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

from . import data_view
from . import user_views

urlpatterns = [
    path('index.html', data_view.index),
    path('users/login/', auth_views.LoginView.as_view(template_name="frontend/login.html",next_page=str(lick_archive_config.host.frontend_url) + "/index.html"),name="login"),
    path('users/logout/', user_views.logout)
]
