from django.urls import path


from . import views
urlpatterns = [
    path('ingest_new_files/', views.IngestNewFiles.as_view()),
    path('sync_query/', views.SyncQuery.as_view()),
]