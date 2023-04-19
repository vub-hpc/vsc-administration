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
import shlex

from collections import namedtuple

from vsc.install.testing import TestCase

from vsc.administration.slurm.sacctmgr import SacctMgrTypes, SlurmUser

from vsc.administration.slurm.sync import (
    slurm_vo_accounts, slurm_user_accounts,
    slurm_institute_accounts, slurm_project_accounts, slurm_project_users_accounts,
    slurm_project_qos,
)


VO = namedtuple("VO", ["vsc_id", "institute", "fairshare", "qos"])
VO.__new__.__defaults__ = (None,) * len(VO._fields)

Project = namedtuple("Project", ["name", "members"])
Project.__new__.__defaults__ = (None,) * len(Project._fields)


class SlurmSyncTestGent(TestCase):
    """Test for the slurm account sync in Gent"""

    def test_slurm_vo_accounts(self):
        """Test that the commands to create accounts are correctly generated."""

        vos = [
            VO(vsc_id="gvo00001", institute={"name": "gent"}, fairshare=10),
            VO(vsc_id="gvo00002", institute={"name": "gent"}, fairshare=20),
            VO(vsc_id="gvo00012", institute={"name": "gent"}, fairshare=100),
            VO(vsc_id="gvo00016", institute={"name": "gent"}, fairshare=10),
            VO(vsc_id="gvo00017", institute={"name": "gent"}, fairshare=10),
            VO(vsc_id="gvo00018", institute={"name": "gent"}, fairshare=10),
        ]

        commands = slurm_vo_accounts(vos, [], ["mycluster"], 'gent')

        self.assertEqual([tuple(x) for x in commands], [tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account gvo00001 Parent=gent Organization=ugent Cluster=mycluster Fairshare=10"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00002 Parent=gent Organization=ugent Cluster=mycluster Fairshare=20")
        ]])


    def test_slurm_project_accounts(self):
        """
        Test the creation of command required to sync projects with slurm accounts.
        """

        RAP = namedtuple("RAP", ["name"])

        resource_app_projects = [
            RAP(name="gpr_compute_project1"),
            RAP(name="gpr_compute_project2"),
            RAP(name="gpr_compute_project3"),
            RAP(name="gpr_compute_project4"),
        ]

        SAI = namedtuple("SAI", ["Account", "Share", "Cluster"])

        slurm_account_info = [
            SAI(Account="gpr_compute_project1", Share=1, Cluster="mycluster"),
            SAI(Account="gpr_compute_project2", Share=1, Cluster="mycluster"),
            SAI(Account="gpr_compute_project5", Share=1, Cluster="mycluster"),
            SAI(Account="gpr_compute_project6", Share=1, Cluster="other_cluster"),
            SAI(Account="gpr_compute_project7", Share=1, Cluster="mycluster"),
            SAI(Account="some_project", Share=1, Cluster="mycluster"),
        ]

        commands = slurm_project_accounts(resource_app_projects, slurm_account_info, ["mycluster"], ["some_project"], ["qosforall"])

        self.assertEqual(set([tuple(x) for x in commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account gpr_compute_project3 Parent=projects Organization=ugent Cluster=mycluster Qos=mycluster-gpr_compute_project3,qosforall"),
            shlex.split("/usr/bin/sacctmgr -i add account gpr_compute_project4 Parent=projects Organization=ugent Cluster=mycluster Qos=mycluster-gpr_compute_project4,qosforall"),
            shlex.split("/usr/bin/scancel --cluster=mycluster --account=gpr_compute_project5 --state=PENDING"),
            shlex.split("/usr/bin/scancel --cluster=mycluster --account=gpr_compute_project5 --state=SUSPENDED"),
            shlex.split("/usr/bin/scancel --cluster=mycluster --account=gpr_compute_project7 --state=PENDING"),
            shlex.split("/usr/bin/scancel --cluster=mycluster --account=gpr_compute_project7 --state=SUSPENDED"),
            shlex.split("/usr/bin/sacctmgr -i remove account Name=gpr_compute_project5 Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i remove account Name=gpr_compute_project7 Cluster=mycluster"),
        ]]))

    def test_slurm_project_qos(self):

        PR = namedtuple("PR", ["name", "cpu_hours", "gpu_hours"])

        projects = [
            PR(name="gpr_compute_project1", cpu_hours=2, gpu_hours=3),
            PR(name="gpr_compute_project2", cpu_hours=5, gpu_hours=0),
            PR(name="gpr_compute_project3", cpu_hours=4, gpu_hours=0),
        ]
        SQI = namedtuple("SQI", ["Name"])

        slurm_qos_info = [
            SQI(Name="mycluster-gpr_compute_project3"),
            SQI(Name="mycluster-gpr_compute_project4"),
            SQI(Name="other-cluster-some-project"),
            SQI(Name="protected_qos"),
        ]

        commands = slurm_project_qos(projects, slurm_qos_info, ["mycluster"], ["protected_qos"], qos_cleanup=True)

        self.assertEqual(set([tuple(x) for x in commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add qos Name=mycluster-gpr_compute_project1"),
            shlex.split("/usr/bin/sacctmgr -i add qos Name=mycluster-gpr_compute_project2"),
            shlex.split("/usr/bin/sacctmgr -i modify qos mycluster-gpr_compute_project1 set flags=NoDecay,DenyOnLimit GRPTRESMins=billing=2280,cpu=2280,gres/gpu=180"),
            shlex.split("/usr/bin/sacctmgr -i modify qos mycluster-gpr_compute_project2 set flags=NoDecay,DenyOnLimit GRPTRESMins=billing=300,cpu=300,gres/gpu=1"),
            shlex.split("/usr/bin/sacctmgr -i modify qos mycluster-gpr_compute_project3 set flags=NoDecay,DenyOnLimit GRPTRESMins=billing=240,cpu=240,gres/gpu=1"),
            shlex.split("/usr/bin/sacctmgr -i remove qos where Name=mycluster-gpr_compute_project4"),
        ]]))


    def test_slurm_project_users_accounts(self):
        project_members = [
            (set(["user1", "user2", "user3"]), "gpr_compute_project1"),
            (set(["user4", "user5", "user6"]), "gpr_compute_project2"),
        ]

        active_accounts = set(["user1", "user3", "user4", "user5", "user6", "user7"])
        slurm_user_info = [
            SlurmUser(User='user1', Def_Acct='default_account', Admin='None', Cluster='mycluster', Account='gpr_compute_project1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user1', Def_Acct='default_account', Admin='None', Cluster='mycluster', Account='default_account', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user2', Def_Acct='gpr_compute_project1', Admin='None', Cluster='mycluster', Account='gpr_compute_project1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user2', Def_Acct='default_account', Admin='None', Cluster='mycluster', Account='default_account', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user4', Def_Acct='gpr_compute_project1', Admin='None', Cluster='mycluster', Account='gpr_compute_project1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user3', Def_Acct='gpr_compute_project2', Admin='None', Cluster='mycluster', Account='gpr_compute_project2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user5', Def_Acct='gpr_compute_project2', Admin='None', Cluster='mycluster', Account='gpr_compute_project2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
        ]

        commands = slurm_project_users_accounts(
            project_members,
            active_accounts,
            slurm_user_info,
            ["mycluster"],
            default_account="default_account",
            protected_accounts=("protected_account1", "protected_acocunt2")
        )

        self.assertEqual(set([tuple(x) for x in commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add user user4 Account=default_account Cluster=mycluster DefaultAccount=default_account"),
            shlex.split("/usr/bin/sacctmgr -i add user user6 Account=default_account Cluster=mycluster DefaultAccount=default_account"),
            shlex.split("/usr/bin/sacctmgr -i add user user3 Account=default_account Cluster=mycluster DefaultAccount=default_account"),
            shlex.split("/usr/bin/sacctmgr -i add user user4 Account=gpr_compute_project2 Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i add user user6 Account=gpr_compute_project2 Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i add user user3 Account=gpr_compute_project1 Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i remove user Name=user3 Account=gpr_compute_project2 Cluster=mycluster"),
            shlex.split("/usr/bin/sacctmgr -i remove user Name=user4 Account=gpr_compute_project1 Cluster=mycluster"),
        ]]))


    def test_slurm_institute_accounts(self):

        institute_vos = dict([
            ("gvo00012", VO(vsc_id="gvo00012", institute={"name": "gent"}, fairshare=100)),
            ("gvo00016", VO(vsc_id="gvo00016", institute={"name": "brussel"}, fairshare=10)),
            ("gvo00017", VO(vsc_id="gvo00017", institute={"name": "antwerpen"}, fairshare=30)),
            ("gvo00018", VO(vsc_id="gvo00018", institute={"name": "leuven"}, fairshare=20)),
        ])

        commands = slurm_institute_accounts([], ["mycluster"], "gent", institute_vos)

        self.assertEqual([tuple(x) for x in commands], [tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account antwerpen Parent=root Organization=uantwerpen Cluster=mycluster Fairshare=500"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00017 Parent=antwerpen Organization=uantwerpen Cluster=mycluster Fairshare=30"),
            shlex.split("/usr/bin/sacctmgr -i add account brussel Parent=root Organization=vub Cluster=mycluster Fairshare=500"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00016 Parent=brussel Organization=vub Cluster=mycluster Fairshare=10"),
            shlex.split("/usr/bin/sacctmgr -i add account gent Parent=root Organization=ugent Cluster=mycluster Fairshare=8500"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00012 Parent=gent Organization=ugent Cluster=mycluster Fairshare=100"),
            shlex.split("/usr/bin/sacctmgr -i add account leuven Parent=root Organization=kuleuven Cluster=mycluster Fairshare=500"),
            shlex.split("/usr/bin/sacctmgr -i add account gvo00018 Parent=leuven Organization=kuleuven Cluster=mycluster Fairshare=20"),
        ]])


    def test_slurm_user_accounts(self):
        """Test that the commands to create, change and remove users are correctly generated."""
        vo_members = {
            "vo1": (set(["user1", "user2", "user3"]), VO(vsc_id="vo1", institute={"name": "gent"}, fairshare=10)),
            "vo2": (set(["user4", "user5", "user6"]), VO(vsc_id="vo2", institute={"name": "gent"}, fairshare=11)),
        }

        active_accounts = set(["user1", "user3", "user4", "user5", "user6", "user7"])
        slurm_user_info = [
            SlurmUser(User='user1', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user2', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user3', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user4', Def_Acct='vo1', Admin='None', Cluster='banette', Account='vo1', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
            SlurmUser(User='user5', Def_Acct='vo2', Admin='None', Cluster='banette', Account='vo2', Partition='', Share='1', MaxJobs='', MaxNodes='', MaxCPUs='', MaxSubmit='', MaxWall='', MaxCPUMins='', QOS='normal', Def_QOS=''),
        ]

        (job_cancel_commands, commands, remove_user_commands) = slurm_user_accounts(vo_members, active_accounts, slurm_user_info, ["banette"])

        self.assertEqual(set([tuple(x) for x in commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add user user6 Account=vo2 Cluster=banette DefaultAccount=vo2"),
            shlex.split("/usr/bin/sacctmgr -i add user user3 Account=vo1 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i add user user4 Account=vo2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i modify user Name=user3 Cluster=banette set DefaultAccount=vo1"),
            shlex.split("/usr/bin/sacctmgr -i modify user Name=user4 Cluster=banette set DefaultAccount=vo2"),
        ]]))
        self.assertEqual(set([tuple(x) for x in remove_user_commands]), set([tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i remove user Name=user2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i remove user Name=user3 Account=vo2 Cluster=banette"),
            shlex.split("/usr/bin/sacctmgr -i remove user Name=user4 Account=vo1 Cluster=banette"),
        ]]))

        self.assertEqual(set([tuple(x) for c in job_cancel_commands.values() for x in c]), set([tuple(x) for x in [
            shlex.split("/usr/bin/scancel --cluster=banette --user=user2"),
            shlex.split("/usr/bin/scancel --cluster=banette --user=user3 --account=vo2"),
            shlex.split("/usr/bin/scancel --cluster=banette --user=user4 --account=vo1"),
        ]]))


class SlurmSyncTestBrussel(TestCase):
    """Test for the slurm account sync in Brussel."""

    def test_slurm_vo_accounts(self):
        """Test that the commands to create accounts are correctly generated."""

        vos = [
            VO(vsc_id="bvo00001", institute={"name": "brussel"}, fairshare=18),
            VO(vsc_id="bvo00002", institute={"name": "brussel"}, fairshare=17),
            VO(vsc_id="bvo00003", institute={"name": "brussel"}, fairshare=16),
            VO(vsc_id="bvo00004", institute={"name": "brussel"}, fairshare=15),
            VO(vsc_id="bvo00005", institute={"name": "brussel"}, fairshare=14),
            VO(vsc_id="bvo00006", institute={"name": "brussel"}, fairshare=13),
        ]

        commands = slurm_vo_accounts(vos, [], ["mycluster"], 'brussel')

        self.assertEqual([tuple(x) for x in commands], [tuple(x) for x in [
            shlex.split("/usr/bin/sacctmgr -i add account bvo00005 Parent=brussel Organization=vub Cluster=mycluster Fairshare=14"),
            shlex.split("/usr/bin/sacctmgr -i add account bvo00006 Parent=brussel Organization=vub Cluster=mycluster Fairshare=13")
        ]])
