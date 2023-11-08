import logging

from django.contrib.auth.backends import BaseBackend
from django.conf import settings

from lick_archive.lick_archive_client import LickArchiveClient
from django.contrib.auth.models import User

logger = logging.getLogger(__name__)


class RemoteAPIAuthBackend(BaseBackend):

    def authenticate(self, request, username=None, password=None):
        if username is None:
            logger.error(f"None user passed to authenticate")
            return None
        if password is None:
            logger.error(f"None password passed to authenticate")
            return None

        logger.debug(f"Authenticating {username}")

        client = LickArchiveClient(f"{settings.LICK_ARCHIVE_API_URL}", 1, 30, 5,session=request.session)

        if (client.login(username,password)):
            logger.debug("Successfull login returned from backend")
            user_results = User.objects.filter(username=username)
            #user = RemoteAPIUser(username=username)
            user = None
            if len(user_results) == 1:
                user = user_results[0]

            if user is None:
                user = User.objects.create(username=username)
                logger.debug("Created user entry")

        else:
            logger.debug("Failed login returned from backend")
            user = None

        client.persist(request.session)

        return user

    def get_user(self, user_id):
        logger.debug(f"get_user called on {user_id}")
        try:
            result= User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
        #result = super().get_user(user_id)
        logger.debug(f"Result from query {result}")
        return result
    