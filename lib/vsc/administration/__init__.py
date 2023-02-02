# -*- coding: latin-1 -*-
#
# Copyright 2012-2023 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This module contains the utilities for dealing with users on the VSC.

@author Andy Georges

@created Apr 23, 2012
"""
#the vsc namespace is used in different folders allong the system
#so explicitly declare this is also the vsc namespace
import pkg_resources
pkg_resources.declare_namespace(__name__)

__author__ = 'ageorges'
__date__ = 'Apr 24, 2012'


class VscAdminError(Exception):
    pass

class NoSuchUserError(VscAdminError):
    def __init__(self, message):
        pass
