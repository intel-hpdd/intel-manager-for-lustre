# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Copyright (c) 2020 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


from setuptools import setup, find_packages


excludes = []

setup(
    name="iml-manager",
    version="6.2.0",
    author="whamCloud",
    author_email="iml@whamcloud.com",
    url="https://pypi.python.org/pypi/iml-manager",
    license="MIT",
    description="The Integrated Manager for Lustre software Monitoring and Administration Interface",
    long_description=open("README.txt").read(),
    packages=find_packages(exclude=excludes) + [""],
    package_data={
        "chroma_core": ["fixtures/default_power_types.json"],
        "polymorphic": ["COPYING"],
        "tests": ["integration/run_tests", "integration/*/*.json", "sample_data/*"],
    },
    scripts=[],
    entry_points={
        "console_scripts": [
            "chroma-config = chroma_core.lib.service_config:chroma_config",
        ]
    },
)
