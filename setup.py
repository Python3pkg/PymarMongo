#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

if sys.version_info[:2] < (2, 7):
    sys.exit("ERROR: Python >= 2.7 required")

if sys.version_info[0] > 2:
    sys.exit("ERROR: Python < 3.0 required")

from distutils.core import setup

setup(
    name='PymarMongo',
    version="0.1.1",
    description="Pymar addition to easily scale your operations with MongoDB collections.",
    url="https://github.com/alexgorin/PymarMongo",
    author="Alexander Gorin",
    author_email='saniagorin@gmail.com',
    license='MIT',
    packages=[
        'pymar.plugins',
        'pymar.plugins.datasources',
    ],
    install_requires=[
          'pymar',
          'pymongo'
    ],
    keywords=[
        'python', 'scale', 'distribute', 'map', 'reduce', 'mongo'
    ]
)
