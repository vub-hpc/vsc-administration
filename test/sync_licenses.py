#
# Copyright 2022-2023 Ghent University
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
    update_licenses, update_license_reservations
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
            'not_so_cool_name@ano-comp1': {'count': 5, 'in_use': 3, 'total': 6, 'extern': 'ano-comp1',
                                'name': 'not_so_cool_name', 'type': 'strange'},
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
            'ano-1@ano-comp1': {'count': 100, 'in_use': 1, 'total': 2, 'extern': 'ano-comp1',
                                'name': 'ano-1', 'type': 'strange'},
            'ano-2@ano-comp1': {'count': 100, 'skip': True, 'extern': 'ano-comp1',
                                'name': 'ano-1', 'type': 'strange'},
            'an-4@ano-comp2': {'count': 200, 'in_use': 5, 'total': 7, 'extern': 'ano-comp2',
                               'name': 'an-4', 'type': 'flexlm'},
            'an-5@ano-comp2': {'count': 7, 'in_use': 3, 'total': 4, 'extern': 'ano-comp2',
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
            ['/usr/bin/sacctmgr', '-i', 'modify', 'resource', 'where', 'Name=an-5', 'Server=ano-comp2', 'ServerType=flexlm', 'set', 'Count=7'],
        ])
        self.assertEqual(rem, [
            ['/usr/bin/sacctmgr', '-i', 'remove', 'resource', 'where', 'Type=license', 'Name=comsol', 'Server=bogus', 'ServerType=flexlm'],
        ])


    @patch('vsc.administration.slurm.scontrol.asyncloop')
    def test_update_licenses_reservations(self, masync):

        licenses = {
            'ano-1@ano-comp1': {'count': 100, 'in_use': 20, 'total': 120, 'extern': 'ano-comp1',
                                'name': 'ano-1', 'type': 'strange'},
            'an-4@ano-comp2': {'count': 200, 'in_use': 5, 'total': 7, 'extern': 'ano-comp2',
                               'name': 'an-4', 'type': 'flexlm'},
            'an-5@ano-comp2': {'count': 7, 'skip': True, 'extern': 'ano-comp2',
                               'name': 'an-5', 'type': 'flexlm'},
        }

        scontrol_config = """Configuration data as of 2022-04-27T10:07:02
AccountingStorageBackupHost = (null)
AccountingStorageEnforce = associations
AccountingStorageHost   = mydb
AccountingStorageExternalHost = (null)
ClusterName             = mycluster
SLURM_CONF              = /etc/slurm/slurm.conf
SLURM_VERSION           = 20.11.6
TrackWCKey              = No
TreeWidth               = 50
UsePam                  = No
UnkillableStepProgram   = (null)
UnkillableStepTimeout   = 120 sec
VSizeFactor             = 0 percent
WaitTime                = 0 sec
X11Parameters           = (null)

Cgroup Support Configuration:
AllowedDevicesFile      = (null)
AllowedKmemSpace        = (null)
AllowedRAMSpace         = 100.0%
AllowedSwapSpace        = 0.0%
CgroupAutomount         = no
CgroupMountpoint        = (null)
"""

        scontrol_part = """PartitionName=mypart AllowGroups=gabc,wheel AllowAccounts=ALL AllowQos=ALL AllocNodes=ALL Default=YES QoS=N/A DefaultTime=01:00:00 DisableRootJobs=YES ExclusiveUser=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=3-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED Nodes=node1,node2 PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=32 TotalNodes=2 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=800 MaxMemPerNode=3200 TRESBillingWeights=CPU=1,Mem=1.33G"""

        scontrol_lic = """LicenseName=comsol3@bogus Total=2 Used=0 Free=2 Reserved=0 Remote=yes
LicenseName=comsol3@bogus2 Total=20 Used=0 Free=20 Reserved=4 Remote=yes
LicenseName=ano-1@ano-comp1 Total=120 Used=0 Free=120 Reserved=4 Remote=yes
"""

        scontrol_res = """ReservationName=hpc123 StartTime=2022-03-28T16:05:00 EndTime=2028-05-28T07:59:59 Duration=2252-15:54:59 Nodes=node123,node456 NodeCnt=2 CoreCnt=512 Features=(null) PartitionName=(null) Flags=MAINT,IGNORE_JOBS,SPEC_NODES TRES=cpu=512 Users=vscabc,vscdef Groups=(null) Accounts=(null) Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)
ReservationName=hellohello StartTime=2022-04-19T08:00:00 EndTime=2022-05-19T08:00:00 Duration=30-00:00:00 Nodes=nodeone,nodetwo,nodethree,nodefour NodeCnt=4 CoreCnt=8 Features=(null) PartitionName=party Flags= TRES=cpu=8 Users=(null) Groups=groupies Accounts=myaccount Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)
ReservationName=external_license_comsol3@bogus2 StartTime=2022-04-29T12:01:11 EndTime=2023-04-29T12:01:11 Duration=365-00:00:00 Nodes=(null) NodeCnt=0 CoreCnt=0 Features=(null) PartitionName=cubone Flags=ANY_NODES TRES=(null) Users=root Groups=(null) Accounts=(null) Licenses=comsol3@bogus2:4 State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)
ReservationName=external_license_ano-1@ano-comp1 StartTime=2022-04-29T12:01:11 EndTime=2023-04-29T12:01:11 Duration=365-00:00:00 Nodes=(null) NodeCnt=0 CoreCnt=0 Features=(null) PartitionName=cubone Flags=ANY_NODES TRES=(null) Users=root Groups=(null) Accounts=(null) Licenses=ano-1@ano-comp1:4 State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)
"""

        masync.side_effect = [
            (0, scontrol_config),
            (0, scontrol_part),
            (0, scontrol_lic),
            (0, scontrol_res),
            ]

        nw_up, rem = update_license_reservations(licenses, 'mycluster', 'mypart', [], False)
        logging.debug("run calls: %s", masync.mock_calls)

        logging.debug("new_update %s remove %s", nw_up, rem)
        self.assertEqual(nw_up, [
            ['/usr/bin/scontrol', 'create', 'reservation', 'ReservationName=external_license_an-4@ano-comp2', 'Duration=7300-0:0:0', 'Flags=LICENSE_ONLY', 'Licenses=an-4@ano-comp2:5', 'NodeCnt=0', 'Partition=mypart', 'Start=now', 'User=root'],
            ['/usr/bin/scontrol', 'update', 'reservation', 'ReservationName=external_license_ano-1@ano-comp1', 'Licenses=ano-1@ano-comp1:20'],
        ])
        self.assertEqual(rem, [
            ['/usr/bin/scontrol', 'delete', 'reservation', 'ReservationName=external_license_comsol3@bogus2'],
        ])
