#
# INTEL CONFIDENTIAL
#
# Copyright 2013 Intel Corporation All Rights Reserved.
#
# The source code contained or described herein and all documents related
# to the source code ("Material") are owned by Intel Corporation or its
# suppliers or licensors. Title to the Material remains with Intel Corporation
# or its suppliers and licensors. The Material contains trade secrets and
# proprietary and confidential information of Intel or its suppliers and
# licensors. The Material is protected by worldwide copyright and trade secret
# laws and treaty provisions. No part of the Material may be used, copied,
# reproduced, modified, published, uploaded, posted, transmitted, distributed,
# or disclosed in any way without Intel's prior express written permission.
#
# No license under any patent, copyright, trade secret or other intellectual
# property right is granted to or conferred upon you by disclosure or delivery
# of the Materials, either expressly, by implication, inducement, estoppel or
# otherwise. Any license under such intellectual property rights must be
# express and approved by Intel in writing.


from textwrap import fill


class ApiException(Exception):
    pass


class InvalidApiResource(ApiException):
    def __init__(self, name):
        self.error_str = "Invalid API Resource: %s" % name
        super(InvalidApiResource, self).__init__()

    def __str__(self):
        return self.error_str


class UnsupportedFormat(ApiException):
    pass


class TooManyMatches(ApiException):
    """
    Too many matches returned during a fuzzy-id lookup.
    """
    def __init__(self, msg=None):
        self.msg = msg
        super(TooManyMatches, self).__init__()

    def __str__(self):
        if not self.msg:
            return "The query matched more than one record."
        else:
            return self.msg


class InvalidVolumeNode(ApiException):
    def __init__(self, input):
        self.input = input
        super(InvalidVolumeNode, self).__init__()

    def __str__(self):
        return "Invalid VolumeNode spec: %s (malformed or bad path?)" % self.input


class BadUserInput(ApiException):
    """
    Generic exception for bad user input detected post-argparse.
    """
    pass


class BadRequest(ApiException):
    """
    Represents a failed TastyPie validation or other 400-level error.
    """
    def __init__(self, value):
        self.error_dict = value
        super(BadRequest, self).__init__()

    def __str__(self):
        lines = ["The server rejected the request with the following error(s):"]
        try:
            for field, errors in self.error_dict.items():
                for error in errors:
                    lines.extend(["  %s: %s" % (field, error)])
        except AttributeError:
            # Sometimes what comes back is just a string.
            lines.append(self.error_dict)
        return "\n".join(lines)


class InternalError(ApiException):
    """
    HTTP 500
    """
    def __init__(self, backtrace):
        self.backtrace = backtrace
        super(InternalError, self).__init__()

    def __str__(self):
        return self.backtrace


class NotFound(ApiException):
    """
    HTTP 404
    """
    pass


class UnauthorizedRequest(ApiException):
    """
    HTTP 401
    """
    pass


class AuthenticationFailure(ApiException):
    """
    HTTP 401 after trying to authenticate.
    """
    def __str__(self):
        return "Authentication failed.  Check username/password."


class ApiConnectionError(ApiException):
    def __init__(self, api_url):
        self.api_url = api_url
        super(ApiConnectionError, self).__init__()

    def __str__(self):
        return "Failed to connect to %s (is --api_url correct?)" % self.api_url


class InvalidStateChange(ApiException):
    def __init__(self, requested_state, available_states):
        self.requested_state = requested_state
        self.available_states = available_states

    def __str__(self):
        return "The requested state (%s) is not one of the available states: %s" % (self.requested_state, ", ".join(self.available_states))


class InvalidJobError(ApiException):
    def __init__(self, requested_job, available_jobs):
        self.requested_job = requested_job
        self.available_jobs = available_jobs

    def __str__(self):
        return "The requested job (%s) is not one of the available jobs: %s" % (self.requested_job, ", ".join(self.available_jobs))


class AbnormalCommandCompletion(Exception):
    def __init__(self, command, status):
        self.status = status
        self.command = command
        super(AbnormalCommandCompletion, self).__init__()

    def __str__(self):
        return "Command completed with abnormal status: %s (%s)" % (self.status, self.command['message'])


class UserConfirmationRequired(ApiException):
    def __str__(self):
        return "Confirmation required."


class JobConfirmationRequired(UserConfirmationRequired):
    def __init__(self, verb, subject, confirmation):
        self.verb = verb
        self.subject = subject
        self.confirmation = confirmation

    def __str__(self):
        return "Running %s on %s requires confirmation of the following:\n%s" % (self.verb, self.subject, fill(self.confirmation, initial_indent="    ", subsequent_indent="  "))


class StateChangeConfirmationRequired(UserConfirmationRequired):
    def __init__(self, report):
        self.report = report

    @property
    def consequences(self):
        return sorted([j['description'] for j in self.report['dependency_jobs'] + [self.report['transition_job']]])

    def __str__(self):
        return '''
This action (%s) has the following consequences:
%s
''' % (self.report['transition_job']['description'], "\n".join(["  * %s" % c for c in self.consequences]))
