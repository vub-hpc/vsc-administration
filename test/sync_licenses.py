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
import re
import sys
import logging
# doesn't seem to work, probably because of the generaloption import
#  see SetUp
logging.basicConfig(level=logging.DEBUG)

from collections import namedtuple
from mock import patch, MagicMock
from vsc.install.testing import TestCase

from sync_slurm_external_licenses import (
    _parse_lmutil, retrieve_license_data, licenses_data,
    update_licenses,
    )

class TestSyncSlurmExtLicenses(TestCase):
    def setUp(self):
        super(TestSyncSlurmExtLicenses, self).setUp()
        logging.getLogger().setLevel(logging.DEBUG)

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

    @patch('sync_slurm_external_licenses._parse_lmutil')
    @patch('sync_slurm_external_licenses.RunNoShell')
    def test_retrieve_license_data(self, mnoshell, mparse):
        mnoshell.run.return_value = (0, 'someoutput')
        mparse.return_value = [
            {'in_use': 10, 'name': 'whatever', 'total': 12},
            {'in_use': 3, 'name': 'is_new', 'total': 6},
        ]

        tool = '/some/path'
        server = 'a.b.c.d'
        port = 7894

        res = retrieve_license_data('NOTSUPPORTED', tool, server, port)
        self.assertTrue(res is None)

        res = retrieve_license_data('flexlm', tool, server, port)
        logging.debug("run calls: %s", mnoshell.run.mock_calls)
        logging.debug("parse calls: %s", mparse.mock_calls)

        self.assertEqual(len(mnoshell.run.mock_calls), 1)
        name, args, kwargs = mnoshell.run.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        # TMPDIR is /tmp? test might fail otherwise
        self.assertTrue(re.search(r'.flexlm_fake_lic$', args[0][-1]))
        args[0][-1] = 'some_tmpfile'
        self.assertEqual(args, (['/some/path', 'lmstat', '-a', '-c', 'some_tmpfile'],))
        self.assertEqual(kwargs, {})

        self.assertEqual(len(mparse.mock_calls), 1)
        name, args, kwargs = mparse.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, ('someoutput',))
        self.assertEqual(kwargs, {})

        self.assertEqual(res, {
            'is_new': {'in_use': 3, 'total': 6},
            'whatever': {'in_use': 10, 'total': 12},
        })

    @patch('sync_slurm_external_licenses.retrieve_license_data')
    def test_licenses_data(self, mretr):
        jsonfn = os.path.dirname(__file__) + '/data/external_licenses.json'

        default_tool = '/some/default'

        mretr.side_effect = [
            {
                'not_so_cool_name': {'in_use': 3, 'total': 6},
            },
            {
                'mysoft': {'in_use': 5, 'total': 7},
                'whatever': {'in_use': 10, 'total': 12},
            },
        ]

        res = licenses_data(jsonfn, default_tool)
        logging.debug("retrieve calls: %s", mretr.mock_calls)

        self.assertEqual(len(mretr.mock_calls), 2)
        name, args, kwargs = mretr.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, ('strange', '/some/path/to/strangetool', 'abc.def', 1234))
        self.assertEqual(kwargs, {})
        name, args, kwargs = mretr.mock_calls[1]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, ('flexlm', '/some/default', 'ghi.jkl', 5678))
        self.assertEqual(kwargs, {})

        self.assertEqual(res, {
            'ano-1@ano-comp1': {'count': 100, 'skip': True, 'extern': 'ano-comp1',
                                'name': 'ano-1', 'type': 'strange'},
            'ano-2@ano-comp1': {'count': 5, 'in_use': 3, 'total': 6, 'extern': 'ano-comp1',
                                'name': 'ano-2', 'type': 'strange'},
            'an-4@ano-comp2': {'count': 200, 'in_use': 5, 'total': 7, 'extern': 'ano-comp2',
                               'name': 'an-4', 'type': 'flexlm'},
            'an-5@ano-comp2': {'count': 7, 'skip': True, 'extern': 'ano-comp2',
                               'name': 'an-5', 'type': 'flexlm'},
        })

    @patch('vsc.administration.slurm.sacctmgr.asyncloop')
    def test_update_licenses(self, masync):
        """Test sacctmgr resource commands: add new, update, skip, dont_update_identical, remove"""

        masync.return_value = (0, """Name|Server|Type|Count|% Allocated|ServerType
comsol|bogus|License|2|0|flexlm
hubba|myserver|NotALicense|10000|52|psssss
an-4|ano-comp2|License|200|3|flexlm
an-5|ano-comp2|License|20|10|flexlm
""")

        licenses = {
            'ano-1@ano-comp1': {'count': 100, 'skip': True, 'extern': 'ano-comp1',
                                'name': 'ano-1', 'type': 'strange'},
            'an-4@ano-comp2': {'count': 200, 'in_use': 5, 'total': 7, 'extern': 'ano-comp2',
                               'name': 'an-4', 'type': 'flexlm'},
            'an-5@ano-comp2': {'count': 7, 'skip': True, 'extern': 'ano-comp2',
                               'name': 'an-5', 'type': 'flexlm'},
        }
        nw_up, rem = update_licenses(licenses, ["clust1", "clust2"], [], False)

        logging.debug("run calls: %s", masync.mock_calls)

        self.assertEqual(len(masync.mock_calls), 1)
        name, args, kwargs = masync.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, (['/usr/bin/sacctmgr', '-s', '-P', 'list', 'resource'],))
        self.assertEqual(kwargs, {})

        logging.debug("new_update %s remove %s", nw_up, rem)
        self.assertEqual(nw_up, [
            ['/usr/bin/sacctmgr', '-i', 'add', 'resource', 'Type=license', 'Name=ano-1', 'Server=ano-comp1', 'ServerType=strange', 'Cluster=clust1,clust2', 'Count=100', 'PercentAllowed=100'],
            ['/usr/bin/sacctmgr', '-i', 'modify', 'resource', 'whereType=license', 'Name=an-5', 'Server=ano-comp2', 'ServerType=flexlm', 'set', 'Cluster=clust1,clust2', 'Count=7', 'PercentAllowed=100'],
        ])
        self.assertEqual(rem, [
            ['/usr/bin/sacctmgr', '-i', 'remove', 'resource', 'where', 'Type=license', 'Name=comsol', 'Server=bogus', 'ServerType=flexlm'],
        ])
