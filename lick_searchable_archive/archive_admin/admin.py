
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from archive_auth.models import ArchiveUser, TypeOverride, AccessOverride, Ownerhint
from lick_archive.archive_config import ArchiveConfigFile
lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

# Add extra CSS, JS etc to default admin models
class TypeOverrideAdmin(admin.ModelAdmin):
    class Media:
        css = {"all": ["archive_admin/archive_admin.css"]}

class AccessOverrideAdmin(admin.ModelAdmin):
    class Media:
        css = {"all": ["archive_admin/archive_admin.css"]}

class OwnerhintAdmin(admin.ModelAdmin):
    class Media:
        css = {"all": ["archive_admin/archive_admin.css"]}

# Register your models here.
admin.site.register(ArchiveUser, UserAdmin)        
admin.site.register(TypeOverride, TypeOverrideAdmin)
admin.site.register(AccessOverride, AccessOverrideAdmin)
admin.site.register(Ownerhint, OwnerhintAdmin)

# Customize the admin site title
admin.site.site_header = "Mt. Hamilton Data Repository Administration"
admin.site.site_title = "Mt. Hamilton Data Repository Admin"
admin.site.site_url = lick_archive_config.host.frontend_url + "/index.html"

