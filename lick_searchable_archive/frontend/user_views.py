import logging

logger = logging.getLogger(__name__)

from django.http import HttpResponseNotAllowed,HttpResponseRedirect
from django.contrib.auth import logout as auth_logout

from lick_archive.django_utils import log_request_debug
from lick_archive.lick_archive_client import LickArchiveClient
from lick_archive.archive_config import ArchiveConfigFile, ArchiveSiteType

lick_archive_config = ArchiveConfigFile.load_from_standard_inifile().config

def logout(request):
    log_request_debug(request)

    if request.method == 'POST':
        if lick_archive_config.host.type == ArchiveSiteType.FRONTEND:
            logger.info("Performing remote logout...")
            # If our local django session doesn't think we're logged in, there's a case
            # where the remote session might think we are. So try the remote logout regardless of if we're
            # logged in
            archive_client = LickArchiveClient(f"{lick_archive_config.host.api_url}", 1, 30, 5, session=request.session)
            if not archive_client.logout():
                logger.error(f"Failed to perform remote logout.")

        # Now perform the local session logout
        try:
            auth_logout(request)
        except Exception as e:
            logger.error(f"Failed django logout.", exc_info=True)
    
        # Redirect back to the frontend query page, which should reflect the logged out status
        return HttpResponseRedirect(lick_archive_config.host.frontend_url + "/index.html")
        
    else:
        return HttpResponseNotAllowed(['POST'])
