#
# Copyright 2015-2021 Ghent University
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

@author: Andy Georges (Ghent University)
"""
import shlex

from collections import namedtuple

from vsc.install.testing import TestCase

from vsc.administration.slurm.sync import slurm_vo_accounts, slurm_user_accounts, parse_slurm_acct_dump
from vsc.administration.slurm.sync import SyncTypes, SlurmAccount, SlurmUser


VO = namedtuple("VO", ["vsc_id", "institute"])


class SlurmSyncTestGent(TestCase):
    """Test for the slurm account sync in Gent"""

    def test_slurm_vo_accounts(self):
        """Test that the commands to create accounts are correctly generated."""

        vos = [
            VO(vsc_id="gvo00001", institute={"name": "gent"}),
            VO(vsc_id="gvo00002", institute={"name": "gent"}),
            VO(vsc_id="gvo00012", institute={"name": "gent"}),
            VO(vsc_id="gvo00016", institute={"name": "gent"}),
            VO(vsc_id="gvo00017", institute={"name": "gent"}),
            VO(vsc_id="gvo00018", institute={"name": "gent"}),
        ]

        commands = slurm_vo_accounts(vos, [], ["mycluster"], 'gent')

        self.assertEqual([tuple(x) for x in commands], [tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account gvo00001 Parent=gent Organization=ugent Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00002 Parent=gent Organization=ugent Cluster=mycluster")
        ]])

    def test_slurm_user_accounts(self):
        """Test that the commands to create, change and remove users are correctly generated."""
        vo_members = {
            "vo1": (set(["user1", "user2", "user3"]), VO(vsc_id="vo1", institute={"name": "gent"})),
            "vo2": (set(["user4", "user5", "user6"]), VO(vsc_id="vo2", institute={"name": "gent"})),
        }

        active_accounts = set(["user1", "user3", "user4", "user5", "user6", "user7"])
        slurm_user_info = [
            SlurmUser(User='user1', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user2', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user3', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user4', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user5', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
        ]

        commands = slurm_user_accounts(vo_members, active_accounts, slurm_user_info, ["banette"])

        self.assertEqual(set([tuple(x) for x in commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add user user6 Account=vo2 DefaultAccount=vo2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i delete user name=user2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i add user user3 Account=vo1 DefaultAccount=vo1 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i delete user name=user3 Account=vo2 where Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i add user user4 Account=vo2 DefaultAccount=vo2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i delete user name=user4 Account=vo1 where Cluster=banette"),
        ]]))


    def test_parse_slurmm_acct_dump(self):
        """Test that the sacctmgr output is correctly processed."""

        sacctmgr_account_output = [
            "Account|Descr|Org|Cluster|Par Name|User|Share|GrpJobs|GrpNodes|GrpCPUs|GrpMem|GrpSubmit|GrpWall|GrpCPUMins|MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS",
            "antwerpen|antwerpen|uantwerpen|banette|root||1||||||||||||||normal|",
            "brussel|brussel|vub|banette|root||1||||||||||||||normal|",
            "gent|gent|ugent|banette|root||1||||||||||||||normal|",
            "vo1|vo1|ugent|banette|gent||1||||||||||||||normal|",
            "vo2|vo2|ugent|banette|gent||1||||||||||||||normal|",
            "vo2|vo2|gvo00002|banette||someuser|1||||||||||||||normal|",
        ]

        info = parse_slurm_acct_dump(sacctmgr_account_output, SyncTypes.accounts)

        self.assertEqual(set(info), set([
            SlurmAccount(Account='brussel', Descr='brussel', Org='vub', Cluster='banette', Par_Name='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='gent', Descr='gent', Org='ugent', Cluster='banette', Par_Name='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='vo2', Descr='vo2', Org='ugent', Cluster='banette', Par_Name='gent', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='antwerpen', Descr='antwerpen', Org='uantwerpen', Cluster='banette', Par_Name='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='vo1', Descr='vo1', Org='ugent', Cluster='banette', Par_Name='gent', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS='')
        ]))

        sacctmgr_user_output = [
            "User|Def Acct|Admin|Cluster|Account|Partition|Share|MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS",
            "root|root|Administrator|banette|root||1|||||||normal|",
            "root|root|Administrator|banette2|root||1|||||||normal|",
            "root|root|Administrator|banette3|root||1|||||||normal|",
            "account1|vo1|None|banette|vo1||1|||||||normal|",
            "account2|vo1|None|banette|vo1||1|||||||normal|",
            "account3|vo2|None|banette|vo2||1|||||||normal|",
        ]

        info = parse_slurm_acct_dump(sacctmgr_user_output, SyncTypes.users)

        self.assertEqual(set(info), set([
            SlurmUser(User='account1', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='account2', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='account3', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
        ]))


class SlurmSyncTestBrussel(TestCase):
    """Test for the slurm account sync in Brussel."""

    def test_slurm_vo_accounts(self):
        """Test that the commands to create accounts are correctly generated."""

        vos = [
            VO(vsc_id="bvo00001", institute={"name": "brussel"}),
            VO(vsc_id="bvo00002", institute={"name": "brussel"}),
            VO(vsc_id="bvo00003", institute={"name": "brussel"}),
            VO(vsc_id="bvo00004", institute={"name": "brussel"}),
            VO(vsc_id="bvo00005", institute={"name": "brussel"}),
            VO(vsc_id="bvo00006", institute={"name": "brussel"}),
        ]

        commands = slurm_vo_accounts(vos, [], ["mycluster"], 'brussel')

        self.assertEqual([tuple(x) for x in commands], [tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account bvo00005 Parent=brussel Organization=vub Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i add account bvo00006 Parent=brussel Organization=vub Cluster=mycluster")
        ]])

