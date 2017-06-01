# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


import settings

from tastypie.authentication import Authentication
from tastypie.authorization import Authorization, DjangoAuthorization
from django.utils.crypto import constant_time_compare


class CsrfAuthentication(Authentication):
    """Tastypie authentication class for rejecting POSTs
    which do not contain a valid CSRF token.

    Note that unlike many REST APIs, Chroma requires CSRF protection
    because the API is used directly from web browsers.

    Some APIs just check that the call is AJAX and trust XHR protection.
    This was okay until http://gursevkalra.blogspot.com/2011/12/json-csrf-with-parameter-padding.html

    Some APIs assume a request isn't CSRF if its JSON encoded (browsers
    don't let you set enctype to application/json for <form>).  That would
    be true apart from http://gursevkalra.blogspot.com/2011/12/json-csrf-with-parameter-padding.html

    In principle you can argue that any CSRF attacks on JSON APIs are
    the browser's (read: end user's) fault, not ours.  In the wild, that
    is a very unhelpful attitude.

    TODO: it is annoying for non-browser clients to have to jump through the
    CSRF hoops.  We should check if someone is authenticating by key instead
    of username/password, and if so avoid applying the CSRF check.
    """
    def is_authenticated(self, request, object = None):
        if request.method != "POST":
            return True

        request_csrf_token = request.META.get('HTTP_X_CSRFTOKEN', '')
        csrf_token = request.META["CSRF_COOKIE"]

        if not constant_time_compare(csrf_token, request_csrf_token):
            return False
        else:
            return True


class AnonymousAuthentication(CsrfAuthentication):
    """Tastypie authentication class which only allows in
    logged-in users unless settings.ALLOW_ANONYMOUS_READ is true"""
    def is_authenticated(self, request, object = None):
        # If any authentication in the class hierarchy refuses, we refuse
        if not super(AnonymousAuthentication, self).is_authenticated(request, object):
            return False

        return settings.ALLOW_ANONYMOUS_READ or request.user.is_authenticated()

    def get_identifier(self, request):
        if request.user.is_authenticated():
            return request.user.username
        else:
            return "Anonymous user"


class PermissionAuthorization(Authorization):
    """Tastypie authentication class which checks for
    the presence of a single permission for access to
    a resource"""
    def __init__(self, perm_name):
        self.perm_name = perm_name

    def is_authorized(self, request, object = None):
        return request.user.has_perm(self.perm_name)


class PATCHSupportDjangoAuth(DjangoAuthorization):
    """Fixing v0.9.11 tasypie's django auth not handling PATCH

    This is an implementation of this fix:
    https://github.com/toastdriven/django-tastypie/pull/345/files

    When we rev tastypie to >0.9.11, we should try to run without this code.
    There is test covering this to check.  See
    chroma-manager/tests/unit/chroma_api/test_dismissed.py

    The procedure to remove this code is simply to change PATCHSupportDjangoAuth
    to DjangoAuthorization in the 3 resources that define it.  And run the tests.
    See HYD-2354
    """

    def is_authorized(self, request, object=None):
        # GET is always allowed
        if request.method == 'GET':
            return True

        klass = self.resource_meta.object_class

        # cannot check permissions if we don't know the model
        if not klass or not getattr(klass, '_meta', None):
            return True

        permission_codes = {
            'POST': '%s.add_%s',
            'PUT': '%s.change_%s',
            'PATCH': '%s.change_%s',
            'DELETE': '%s.delete_%s',
            }

        # cannot map request method to permission code name
        if request.method not in permission_codes:
            return True

        permission_code = permission_codes[request.method] % (
            klass._meta.app_label,
            klass._meta.module_name)

        # user must be logged in to check permissions
        # authentication backend must set request.user
        if not hasattr(request, 'user'):
            return False

        return request.user.has_perm(permission_code)
