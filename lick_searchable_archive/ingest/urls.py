from django.urls import path


from . import views
urlpatterns = [
    path('notifications/', views.Notifications.as_view()),
]