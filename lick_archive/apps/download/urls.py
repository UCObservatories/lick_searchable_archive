from django.urls import path


from . import views

urlpatterns = [
    path(f'data/<path:file>', views.DownloadSingleView.as_view()),
    path(f'api/download', views.DownloadMultiView.as_view()),
]