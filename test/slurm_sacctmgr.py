#
# Copyright 2015-2023 Ghent University
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

from vsc.install.testing import TestCase

from vsc.administration.slurm.sacctmgr import (
    parse_slurm_sacct_dump,
    SacctMgrTypes, SlurmAccount, SlurmUser,
    )


class SlurmSacctmgrTest(TestCase):
    def test_parse_slurmm_sacct_dump(self):
        """Test that the sacctmgr output is correctly processed."""

        sacctmgr_account_output = [
            "Account|Descr|Org|Cluster|ParentName|User|Share|GrpJobs|GrpNodes|GrpCPUs|GrpMem|GrpSubmit|GrpWall|GrpCPUMins|MaxJobs|MaxNodes|MaxCPUs|MaxSubmit|MaxWall|MaxCPUMins|QOS|Def QOS",
            "antwerpen|antwerpen|uantwerpen|banette|root||1||||||||||||||normal|",
            "brussel|brussel|vub|banette|root||1||||||||||||||normal|",
            "gent|gent|gent|banette|root||1||||||||||||||normal|",
            "vo1|vo1|gent|banette|gent||1||||||||||||||normal|",
            "vo2|vo2|gent|banette|gent||1||||||||||||||normal|",
            "vo2|vo2|gvo00002|banette||someuser|1||||||||||||||normal|",
        ]

        info = parse_slurm_sacct_dump(sacctmgr_account_output, SacctMgrTypes.accounts)

        self.assertEqual(set(info), set([
            SlurmAccount(Account='brussel', Descr='brussel', Org='vub', Cluster='banette', ParentName='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='gent', Descr='gent', Org='gent', Cluster='banette', ParentName='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='vo2', Descr='vo2', Org='gent', Cluster='banette', ParentName='gent', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='antwerpen', Descr='antwerpen', Org='uantwerpen', Cluster='banette', ParentName='root', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmAccount(Account='vo1', Descr='vo1', Org='gent', Cluster='banette', ParentName='gent', User='', Share='1', GrpJobs='', GrpNodes='', GrpCPUs='', GrpMem='', GrpSubmit='', GrpWall='', GrpCPUMins='', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS='')
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

        info = parse_slurm_sacct_dump(sacctmgr_user_output, SacctMgrTypes.users)

        self.assertEqual(set(info), set([
            SlurmUser(User='account1', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='account2', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='account3', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
        ]))


