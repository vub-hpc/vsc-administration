#
# Copyright 2013-2023 Ghent University
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
Tests for the sync_vsc_email_postfix script
"""

import os
import sys
import logging
logging.basicConfig(level=logging.DEBUG)

from collections import namedtuple
from mock import patch, MagicMock
from vsc.install.testing import TestCase
import shutil
from sync_vsc_email_postfix import VscPostfixSync
from vsc.accountpage.sync import MAX_RTT
import time

ETC_TEST = os.path.join(os.path.dirname(__file__), 'testetc')


class TestVscPostfixSync(TestCase):
    def setUp(self):
        super(TestVscPostfixSync, self).setUp()
        os.makedirs(ETC_TEST)

    def tearDown(self):
        shutil.rmtree(ETC_TEST)
        super(TestVscPostfixSync, self).tearDown()

    @patch('vsc.accountpage.sync.ExtendedSimpleOption.prologue')
    def test_sync(self, pl):
        cmfn = os.path.join(ETC_TEST, 'vsc_canonical_test')

        old_mtime = 1234567

        # make mock file
        oldmap = [
            "vsc40001@vscentrum.be foo1@bar.com",
            "vsc40002@vscentrum.be foo2@bar.com",
            "vsc40003@vscentrum.be foo3@bar.com",
            "",
        ]
        with open(cmfn, 'w') as c:
            c.write("\n".join(oldmap))
        os.utime(cmfn, (old_mtime, old_mtime))

        sys.argv = ["", '--postfix_canonical_map=%s' % cmfn]
        vps = VscPostfixSync()
        apc = MagicMock(name='APC')
        vps.apc = apc

        Acct = namedtuple('Acct', ['vsc_id', 'email'])
        apc.get_accounts.return_value = (
            # active
            [ Acct(vsc_id='vsc40001', email='huppel@bar.com')
            , Acct(vsc_id='vsc40002', email='foo2@bar.com')
            ],
            #inactive
            [ Acct(vsc_id='vsc40003', email='willy.wonka@chocolate.factory')
            ]
        )

        vps.do(True)  # dryrun
        with open(cmfn, 'r') as c:
            self.assertEqual(c.read(), "\n".join(oldmap))  # dryrun

        newmap = [
            "vsc40001@vscentrum.be huppel@bar.com\n",
            "vsc40002@vscentrum.be foo2@bar.com\n",
        ]

        vps.do(False)
        with open(cmfn, 'r') as c:
            self.assertEqual(sorted(c.readlines()), newmap)
