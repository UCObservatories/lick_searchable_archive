
from django.contrib import admin
from .models import TypeOverride, AccessOverride
from lick_archive.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

# Add extra CSS, JS etc to default admin models
class TypeOverrideAdmin(admin.ModelAdmin):
    class Media:
        css = {"all": ["archive_admin/archive_admin.css"]}

class AccessOverrideAdmin(admin.ModelAdmin):
    class Media:
        css = {"all": ["archive_admin/archive_admin.css"]}

# Register your models here.
admin.site.register(TypeOverride, TypeOverrideAdmin)
admin.site.register(AccessOverride, AccessOverrideAdmin)

# Customize the admin site title
admin.site.site_header = "Mt. Hamilton Data Repository Administration"
admin.site.site_title = "Mt. Hamilton Data Repository Admin"
admin.site.site_url = lick_archive_config.host.frontend_url + "/index.html"

