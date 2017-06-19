# Copyright (c) 2017 Intel Corporation. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.

import os
import platform
import re

SITE_ROOT = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

_settings = {
    'APP_PATH': {
        'dev': SITE_ROOT,
        'prod': '/usr/share/chroma-manager'
    },
    'REPO_PATH': {
        'prod': '/var/lib/chroma/repo'
    },
    'HTTP_FRONTEND_PORT': {
        'dev': 9000,
        'prod': 80
    },
    'HTTPS_FRONTEND_PORT': {
        'dev': 8000,
        'prod': 443
    },
    'HTTP_AGENT_PORT': {
        'all': 8002
    },
    'HTTP_API_PORT': {
        'all': 8001
    },
    'REALTIME_PORT': {
        'all': 8888
    },
    'VIEW_SERVER_PORT': {
      'all': 8889
    },
    'SSL_PATH': {
        'dev': SITE_ROOT,
        'prod': '/var/lib/chroma'
    }
}


def get_settings_for(mode):
    """
    Iterates the _settings dictionary, pulling out items that match the given mode.
    This is basically a reduce operation.

    :param mode: The mode to retrieve settings for. Can be 'dev' or 'prod'
    :return: A dictionary of settings for the given type.
    :rtype: dict
    """
    out = {}

    for key, values in _settings.items():
        value = values.get('all') or values.get(mode)

        if callable(value):
            value = value()

        if value is not None:
            out[key] = value

    return out


def get_production_nginx_settings():
    """
    Gets production nginx settings.

    :return: Production Settings
    :rtype: dict
    """
    return get_settings_for('prod')


def get_dev_nginx_settings():
    """
    Gets development nginx settings.

    :return: Development Settings
    :rtype: dict
    """
    return get_settings_for('dev')
