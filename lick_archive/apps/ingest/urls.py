from django.urls import path


from . import views
from django.conf import settings

urlpatterns = [
    path(f'ingest/notifications/', views.IngestNotifications.as_view()),
    path(f'ingest/counts/<path:ingest_path>', views.IngestCounts.as_view()),
]
