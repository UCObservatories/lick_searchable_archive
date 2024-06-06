from django.urls import path


from archive_admin.admin import admin_site

urlpatterns = [
    path(f'admin/', admin_site.urls),    
]
