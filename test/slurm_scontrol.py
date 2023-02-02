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
Tests for vsc.administration.slurm.*
"""

import logging
# doesn't seem to work, probably because of the generaloption import
#  see SetUp
logging.basicConfig(level=logging.DEBUG)

from mock import patch, MagicMock
from vsc.install.testing import TestCase

from vsc.administration.slurm.scontrol import (
    parse_scontrol_dump, get_scontrol_info, get_scontrol_config,
    ScontrolTypes, SlurmReservation, SlurmLicense, SlurmConfig, SlurmPartition,
    )


class SlurmScontrolTest(TestCase):
    def setUp(self):
        super(SlurmScontrolTest, self).setUp()
        logging.getLogger().setLevel(logging.DEBUG)

    def test_parse_scontrol_dump(self):
        """Test that the scontrol output is correctly processed."""

        # Or eg 'No licenses configured in Slurm.' (slurm consistency yay)
        scontrol_output = ["No reservations in the system"]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.reservation)

        self.assertEqual(info, set())

        # test reservation output
        scontrol_output = [
            "ReservationName=hpc123 StartTime=2022-03-28T16:05:00 EndTime=2028-05-28T07:59:59 Duration=2252-15:54:59 Nodes=node123,node456 NodeCnt=2 CoreCnt=512 Features=(null) PartitionName=(null) Flags=MAINT,IGNORE_JOBS,SPEC_NODES TRES=cpu=512 Users=vscabc,vscdef Groups=(null) Accounts=(null) Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)",
            "ReservationName=hellohello StartTime=2022-04-19T08:00:00 EndTime=2022-05-19T08:00:00 Duration=30-00:00:00 Nodes=nodeone,nodetwo,nodethree,nodefour NodeCnt=4 CoreCnt=8 Features=(null) PartitionName=party Flags= TRES=cpu=8 Users=(null) Groups=groupies Accounts=myaccount Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)",
        ]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.reservation)

        self.assertEqual(info, set([
            SlurmReservation(ReservationName='hpc123', StartTime='2022-03-28T16:05:00', EndTime='2028-05-28T07:59:59', Duration='2252-15:54:59', Nodes='node123,node456', NodeCnt='2', CoreCnt='512', Features=None, PartitionName=None, Flags='MAINT,IGNORE_JOBS,SPEC_NODES', TRES='cpu=512', Users='vscabc,vscdef', Groups=None, Accounts=None, Licenses=None, State='ACTIVE', BurstBuffer=None, Watts='n/a', MaxStartDelay=None),
            SlurmReservation(ReservationName='hellohello', StartTime='2022-04-19T08:00:00', EndTime='2022-05-19T08:00:00', Duration='30-00:00:00', Nodes='nodeone,nodetwo,nodethree,nodefour', NodeCnt='4', CoreCnt='8', Features=None, PartitionName='party', Flags='', TRES='cpu=8', Users=None, Groups='groupies', Accounts='myaccount', Licenses=None, State='ACTIVE', BurstBuffer=None, Watts='n/a', MaxStartDelay=None),
        ]))

        # test license output
        scontrol_output = [
            'LicenseName=comsol3@bogus Total=2 Used=0 Free=2 Reserved=0 Remote=yes',
            'LicenseName=comsol3@bogus2 Total=4 Used=1 Free=3 Reserved=0 Remote=yes',
            ]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.license)

        self.assertEqual(info, set([
            SlurmLicense(LicenseName='comsol3@bogus', Total=2, Used=0, Free=2, Reserved=0, Remote='yes'),
            SlurmLicense(LicenseName='comsol3@bogus2', Total=4, Used=1, Free=3, Reserved=0, Remote='yes'),
        ]))

        # test config output (this is re-formatted oneliner output)
        scontrol_output = ['ClusterName="mycluster" AccountingStorageHost="mydb" NotRelevant="abc def" SLURM_CONF="/etc/slurm/slurm.conf" SLURM_VERSION="20.11.6"']
        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.config)
        self.assertEqual(info, set([
            SlurmConfig(ClusterName='mycluster', AccountingStorageHost='mydb', SLURM_CONF='/etc/slurm/slurm.conf',
                        SLURM_VERSION='20.11.6',
                        )
        ]))

        # test partition output
        scontrol_output = [
            'PartitionName=mypart AllowGroups=gabc,wheel AllowAccounts=ALL AllowQos=ALL AllocNodes=ALL Default=YES QoS=N/A DefaultTime=01:00:00 DisableRootJobs=YES ExclusiveUser=NO GraceTime=0 Hidden=NO MaxNodes=UNLIMITED MaxTime=3-00:00:00 MinNodes=0 LLN=NO MaxCPUsPerNode=UNLIMITED Nodes=node1,node2 PriorityJobFactor=1 PriorityTier=1 RootOnly=NO ReqResv=NO OverSubscribe=NO OverTimeLimit=NONE PreemptMode=OFF State=UP TotalCPUs=32 TotalNodes=2 SelectTypeParameters=NONE JobDefaults=(null) DefMemPerCPU=800 MaxMemPerNode=3200 TRESBillingWeights=CPU=1,Mem=1.33G',
        ]
        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.partition)
        self.assertEqual(info, set([
            SlurmPartition(PartitionName='mypart', AllowGroups='gabc,wheel', AllowAccounts='ALL', AllowQos='ALL', AllocNodes='ALL', Default='YES', QoS='N/A', DefaultTime='01:00:00', DisableRootJobs='YES', ExclusiveUser='NO', GraceTime='0', Hidden='NO', MaxNodes='UNLIMITED', MaxTime='3-00:00:00', MinNodes='0', LLN='NO', MaxCPUsPerNode='UNLIMITED', Nodes='node1,node2', PriorityJobFactor='1', PriorityTier='1', RootOnly='NO', ReqResv='NO', OverSubscribe='NO', OverTimeLimit='NONE', PreemptMode='OFF', State='UP', TotalCPUs=32, TotalNodes=2, SelectTypeParameters='NONE', JobDefaults=None, DefMemPerCPU=800, MaxMemPerNode=3200, TRESBillingWeights='CPU=1,Mem=1.33G'),
        ]))

    @patch('vsc.administration.slurm.scontrol.asyncloop')
    def test_get_scontrol_info(self, masync):

        masync.return_value = (0, """Configuration data as of 2022-04-27T10:07:02
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
""")

        info = get_scontrol_info(ScontrolTypes.config, as_dict=False)

        logging.debug("run calls: %s", masync.mock_calls)

        self.assertEqual(len(masync.mock_calls), 1)
        name, args, kwargs = masync.mock_calls[0]
        logging.debug("%s %s %s", name, args, kwargs)
        self.assertEqual(name, '')
        self.assertEqual(args, (['/usr/bin/scontrol', 'show', 'config', '--detail', '--oneliner'],))
        self.assertEqual(kwargs, {})

        self.assertEqual(info, set([
            SlurmConfig(ClusterName='mycluster', AccountingStorageHost='mydb', SLURM_CONF='/etc/slurm/slurm.conf',
                        SLURM_VERSION='20.11.6',
                        )
        ]))

        config = get_scontrol_config()
        self.assertEqual(info, set([config]))
