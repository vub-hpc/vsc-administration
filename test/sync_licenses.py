#
# Copyright 2022-2022 Ghent University
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
Tests for the sync_slurm_external_licenses script
"""

import os
import sys
import logging
logging.basicConfig(level=logging.DEBUG)

from collections import namedtuple
from mock import patch, MagicMock
from vsc.install.testing import TestCase

from sync_slurm_external_licenses import _parse_lmutil

class TestSyncSlurmExtLicenses(TestCase):
    def test_parse_lmstat(self):
        """Test the lmstat parsing"""
        datafn = os.path.dirname(__file__) + '/data/lmutil/flexlm_1'
        output = open(datafn).read()
        res = _parse_lmutil(output)
        self.assertEqual(res, [
            {'in_use': 205, 'name': 'mysoft', 'total': 715},
            {'in_use': 0, 'name': 'supersoft', 'total': 1},
            {'in_use': 10, 'name': 'whatever', 'total': 12},
            {'in_use': 3, 'name': 'is_new', 'total': 6},
            ])
