"""A set of utility functions for use in django apps"""
import logging
import os

logger = logging.getLogger(__name__)

from django.core import validators
from django.core.exceptions import ValidationError
import unicodedata

def validate_username(username):
    """A Django validator for archive user names
    This validator is written to be used in Django forms and models,
    and is consistent with other django `Link validators <https://docs.djangoproject.com/en/4.2/ref/validators/>`_.
    Args:
        username (str): User name to validate
        
    Return:
        None if successfull.
        
    Raises:
        :obj:`django.core.exceptions.ValidationError` Raised if validation failed.
    """
    # First apply django validators
    validators.ProhibitNullCharactersValidator()(username)
    validators.MinLengthValidator(1)(username)
    validators.MaxLengthValidator(150)(username)

    # Now apply our own valiation
    for c in username:
        cat = unicodedata.category(c)
        if cat[0] in "LNP":
            # Allow letters, numbers, punctuation
            continue
        elif cat == "Zs":
            # Allow spaces
            continue
        else:
            # Disallow everything else:
            name = unicodedata.name(c,f"{ord(c):04}")
            raise ValidationError(message=f"Username has invalid character '{c}' ({name})")
    return

def log_request_debug(request):
    if logger.isEnabledFor(logging.DEBUG):
        for key in request.META.keys():
            logger.debug(f"Header key '{key}' value: {request.META[key]}")
        for key in os.environ:
            logger.debug(f"Environment variable '{key}' value: '{os.environ[key]}'")
        if hasattr(request,"session"):
            session = request.session
            if session is None:
                logger.debug("Session is None")
            else:
                logger.debug(f"Session key: {session.session_key}")
                logger.debug(f"Session expiry age: {session.get_expiry_age()}")
                for key, value in session.items():
                    logger.debug(f"Session[{key}] : '{value}'")
        else:
            logger.debug("Request has no session.")
        if hasattr(request, "user"):
            logger.debug(f"Request user: '{request.user.username}'")
        else:
            logger.debug("Request has no user.")
