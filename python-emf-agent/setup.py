# -*- coding: utf-8 -*-
#!/usr/bin/env python
# Copyright (c) 2018 DDN. All rights reserved.
# Use of this source code is governed by a MIT-style
# license that can be found in the LICENSE file.


from setuptools import setup, find_packages
from chroma_agent import package_version

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

excludes = ["*tests*"]

setup(
    name="emf-agent",
    version=package_version(),
    author="whamCloud Team",
    author_email="emf@whamcloud.com",
    url="https://pypi.python.org/pypi/emf-agent",
    packages=find_packages(exclude=excludes),
    include_package_data=True,
    license="MIT",
    description="The EMF software Monitoring and Administration Interface Agent",
    long_description=long_description,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
    ],
    keywords="EMF lustre high-availability",
    data_files=[],
    entry_points={
        "console_scripts": [
            "chroma-agent = chroma_agent.cli:main",
            "chroma-agent-daemon = chroma_agent.agent_daemon:main",
            "fence_chroma = chroma_agent.fence_chroma:main",
        ]
    },
)