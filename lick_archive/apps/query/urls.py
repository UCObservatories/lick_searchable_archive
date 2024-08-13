from django.urls import path
from django.conf import settings


from . import views

urlpatterns = [
    path(f'data/', views.QueryView.as_view()),
    path(f'data/<path:file>/header', views.HeaderView.as_view()),
]