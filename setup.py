#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function

import codecs
import os
import re

from setuptools import setup

cwd = os.path.abspath(os.path.dirname(__file__))

def read(filename):
    with codecs.open(os.path.join(cwd, filename), 'rb', 'utf-8') as h:
        return h.read()

metadata = read(os.path.join(cwd, 'good_morning', '__init__.py'))


def extract_metaitem(meta):
    # swiped from https://hynek.me 's attr package
    meta_match = re.search(
        r"""^__{meta}__\s+=\s+['\"]([^'\"]*)['\"]""".format(meta=meta),
        metadata, re.MULTILINE)
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError('Unable to find __{meta}__ string.'.format(meta=meta))


setup(
    name='good_morning',
    version=extract_metaitem('version'),
    license=extract_metaitem('license'),
    description=extract_metaitem('description'),
    long_description=(read('README.md') + '\n\n' +
                      read('AUTHORS.rst') + '\n\n' +
                      read('CHANGES')),
    author=extract_metaitem('author'),
    author_email=extract_metaitem('email'),
    maintainer=extract_metaitem('author'),
    maintainer_email=extract_metaitem('email'),
    url=extract_metaitem('url'),
    # download_url=extract_metaitem('download_url'),
    platforms=['Any'],
    packages=['good_morning'],
    install_requires=['numpy', 'pandas', 'pymysql',
                      'python-dateutil','beautifulsoup4',
                      'mock;python_version<"3.3"',
                      "futures; python_version < '3.0'",
                      "futures>=3.0.5; python_version == '2.6' or python_version=='2.7'"
                      ],
    keywords='stocks good_morning financial data historical',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Financial and Insurance Industry',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Office/Business :: Financial',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5'
    ],
)
