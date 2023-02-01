from django.urls import path


from . import views
from django.conf import settings

urlpatterns = [
    path(f'ingest_notifications/', views.IngestNotifications.as_view()),
]
