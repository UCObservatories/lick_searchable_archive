"""lick_searchable_archive URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.conf import settings

urlpatterns = [
    path(settings.LICK_ARCHIVE_URL_PATH_PREFIX, include('query.urls')),
#    path('admin/', admin.site.urls),
]

if settings.LICK_ARCHIVE_ALLOW_INGEST:
    urlpatterns.append(path(settings.LICK_ARCHIVE_URL_PATH_PREFIX, include('ingest.urls')))

urlpatterns.append(path(settings.LICK_ARCHIVE_URL_PATH_PREFIX, include('frontend.urls')))

# TODO remove when we get a real web server in front of gunicorn
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
urlpatterns += staticfiles_urlpatterns()
