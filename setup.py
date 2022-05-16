#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2022 Ghent University
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
    'vsc-base >= 3.0.6',
    'vsc-config >= 3.7.2',
    'vsc-filesystems >= 1.0.1',
    'vsc-utils >= 2.0.0',
    'lockfile >= 0.9.1',
]

if sys.version_info < (3, 0):
    # enum34 is only required with Python 2
    install_requires.append('enum34')
    install_requires.append('python-ldap < 3.3.0')
else:
    install_requires.append('python-ldap')


PACKAGE = {
    'version': '3.8.0',
    'author': [ag, jt],
    'maintainer': [ag, jt],
    'tests_require': ['mock'],
    'setup_requires': [
        'vsc-install >= 0.15.3',
    ],
    'install_requires': install_requires,
    'extras_require': {
        'oceanstor': ['vsc-filesystem-oceanstor >= 0.5.1'],
    },
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
