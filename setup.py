#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2023 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
vsc-administration distribution setup.py

@author: Andy Georges (Ghent University)
@author: Jens Timmerman (Ghent University)
"""
import sys

from vsc.install import shared_setup
from vsc.install.shared_setup import ag, jt

install_requires = [
    'vsc-accountpage-clients >= 2.1.6',
    'vsc-base >= 3.5.0',
    'vsc-config >= 3.11.0',
    'vsc-filesystems >= 1.3.0',
    'vsc-utils >= 2.0.0',
    'lockfile >= 0.9.1',
]

if sys.version_info > (3, 6) and sys.version_info < (3, 7):
    # Python 3.6 needs extra backports and version limits
    install_requires.extend([
        'dataclasses >= 0.8',
        'isort < 5.11.0',
    ])

PACKAGE = {
    'version': '4.5.0',
    'author': [ag, jt],
    'maintainer': [ag, jt],
    'tests_require': ['mock'],
    'setup_requires': [
        'vsc-install >= 0.15.3',
    ],
    'install_requires': install_requires,
    'extras_require': {
        'oceanstor': ['vsc-filesystem-oceanstor >= 0.6.0'],
    },
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
