from django.urls import path


from django.conf import settings
from django.contrib import admin

urlpatterns = [
    path(f'admin/', admin.site.urls),    
]
