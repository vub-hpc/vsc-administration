#
# Copyright 2018-2023 Ghent University
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
""""""
from vsc.install import commontest

class ImportTest(commontest.CommonTest):
    EXCLUDE_SCRIPTS = [
        r'sync_django_ldap', # requires django
        r'get_overview_users', # requires vsc.pg
    ]
