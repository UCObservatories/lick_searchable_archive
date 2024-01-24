import os
import logging
from http import HTTPStatus

from django.views.decorators.csrf import ensure_csrf_cookie
from django.http import JsonResponse, HttpResponse, HttpResponseNotAllowed
from django.contrib.auth import authenticate,login,logout
from django.middleware.csrf import get_token

from lick_archive.django_utils import validate_username

logger = logging.getLogger(__name__)




@ensure_csrf_cookie
def login_user(request):
    response = {"logged_in": False,
                "user": ""}
    
    try:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Login request method: {request.method}")
            if hasattr(request,"session"):
                session = request.session
                if session is None:
                    logger.debug("Session is None")
                else:
                    logger.debug(f"Session key: {session.session_key}")
                    logger.debug(f"Session expiry age: {session.get_expiry_age()}")
                    for key, value in session.items():
                        logger.debug(f"Session[{key}] : '{value}'")


            for key in request.META.keys():
                logger.debug(f"Header key '{key}' value: {request.META[key]}")
            for key in os.environ:
                logger.debug(f"Environment variable '{key}' value: '{os.environ[key]}'")



        if request.method == "GET":
            response['csrfmiddlewaretoken'] = get_token(request)
            if request.user.is_authenticated:
                response["logged_in"] = True
                response["user"] = request.user.get_username()


        elif request.method == "POST":
            # Validate the username. Presumably authenticate should do it's own validation
            # but we validate it so we can log it later without worrying about
            # logging unvalidated data
            validate_username(request.POST['username'])
            user = authenticate(request=request, username=request.POST['username'],password=request.POST['password'])
            if user is None:
                logger.info(f"Login failed for user '{request.POST['username']}'")
                return HttpResponse(status=HTTPStatus.FORBIDDEN)
            else:
                logger.info(f"Login succeeded for user '{user.get_username()}'")
                login(request,user)
                response["logged_in"] = True
                response["user"] = user.get_username()
                response['csrfmiddlewaretoken'] = get_token(request)
        else:
            return HttpResponseNotAllowed(['GET','POST'])
    except Exception as e:
        logger.error("Login failed with exception", exc_info=True)
        return HttpResponse(status=HTTPStatus.FORBIDDEN)

    return JsonResponse(response)        

@ensure_csrf_cookie
def logout_user(request):
    
    try:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Logout request method: {request.method}")
            if hasattr(request,"session"):
                session = request.session
                if session is None:
                    logger.debug("Session is None")
                else:
                    logger.debug(f"Session key: {session.session_key}")
                    logger.debug(f"Session expiry age: {session.get_expiry_age()}")
                    for key, value in session.items():
                        logger.debug(f"Session[{key}] : '{value}'")


            for key in request.META.keys():
                logger.debug(f"Header key '{key}' value: {request.META[key]}")
            for key in os.environ:
                logger.debug(f"Environment variable '{key}' value: '{os.environ[key]}'")



        if request.method == "POST":
            if request.user.is_authenticated:
                logger.info(f"Logging out user {request.user.get_username()}")
                logout(request)

            return HttpResponse(status = HTTPStatus.NO_CONTENT)

        else:
            return HttpResponseNotAllowed(['POST'])
    except Exception as e:
        logger.error("logout failed with exception", exc_info=True)
        return HttpResponse(status=HTTPStatus.INTERNAL_SERVER_ERROR)

