from django.urls import path


from . import views

urlpatterns = [
    path(f'index.html', views.index),
]
