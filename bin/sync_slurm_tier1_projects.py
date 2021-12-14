#!/usr/bin/env python
#
# Copyright 2021-2021 Ghent University
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
This script synchronises the users and projects from the Resource App to the Slurm database.

The script must result in an idempotent execution, to ensure nothing breaks.
"""
from collections import namedtuple

import logging

from configparser import ConfigParser
from datetime import datetime

from vsc.accountpage.sync import Sync
from vsc.administration.slurm.sync import (
    get_slurm_acct_info, SyncTypes, SacctMgrException,
    slurm_project_accounts, slurm_project_users_accounts,
    slurm_project_qos
)
from vsc.utils.py2vs3 import HTTPError
from vsc.utils.run import RunNoShell

VSC_ADMIN_GROUPS = ("astaff", "badmin", "l_sysadmin", "gt1_dodrio_vscadmins")

TIER1_PROTECTED_ACCOUNTS = ("root", "projects", "gadminforever", "gt1_default")

def execute_commands(commands):
    """Run the specified commands"""

    for command in commands:
        logging.info("Running command: %s", command)

        # if one fails, we simply fail the script and should get notified
        (ec, _) = RunNoShell.run(command)
        if ec != 0:
            raise SacctMgrException("Command failed: {0}".format(command))


ProjectIniConfig = namedtuple("ProjectIniConfig",
    ["name", "group", "end_date", "members", "cpu_hours", "gpu_hours"]
)


def over_and_done(datestamp):
    """
    Is the datestamp past the current day?
    """
    end = datetime.strptime(datestamp, "%Y%m%d").date()
    return end < datetime.today().date()


def get_projects(projects_ini):
    """
    This reads the ini file which contains information on the projects

    [project_name]
    end_date: YYYYMMDD
    members: comma-separated list of VSC IDs of project members
    CPUhours: int
    GPUhours: int

    """

    projects_config = ConfigParser()
    with open(projects_ini) as pini:
        projects_config.read_file(pini)

    active_projects = []
    past_projects = []

    for section in projects_config.sections():
        if section in TIER1_PROTECTED_ACCOUNTS:
            continue

        end_date = projects_config.get(section, "end_date", fallback="20380101")

        if over_and_done(end_date):
            logging.debug("Project %s past end date %s", section, end_date)
            projects = past_projects
        else:
            projects = active_projects

        logging.info("processing section %s", section)
        if section in VSC_ADMIN_GROUPS:
            projects.append(ProjectIniConfig(
                name=section,
                group=section,
                end_date=projects_config.get(section, "end_date"),
                members=[m.strip() for m in projects_config.get(section, "members").split(",")],
                cpu_hours=int(projects_config.get(section, "CPUhours", fallback=0)),
                gpu_hours=int(projects_config.get(section, "GPUhours", fallback=0)),
            ))

        elif section.startswith('gpr_compute'):
            projects.append(ProjectIniConfig(
                name=section.replace("gpr_compute_", ""),
                group=section,
                end_date=projects_config.get(section, "end_date"),
                members=[m.strip() for m in projects_config.get(section, "members").split(",")],
                cpu_hours=int(projects_config.get(section, "CPUhours", fallback=0)),
                gpu_hours=int(projects_config.get(section, "GPUhours", fallback=0)),
            ))

    return (active_projects, past_projects)

class Tier1SlurmProjectSync(Sync):

    CLI_OPTIONS = {
        "clusters": (
            "Cluster(s) (comma-separated) to sync for. ",
            "strlist",
            "store",
            ["dodrio"],
        ),
        'project_ini': ('Ini file with projects information', str, 'store', None),
    }

    def update_project(self, project):
        """
        Retrieves project information from the AP -- if any -- and updates the projects' members.
        """

        try:
            (_, group) = self.apc.group[project.group].get()
        except HTTPError as _:
            logging.error("Could not get project group %s data", project.group)
            return project

        logging.debug("Current group members: %s", project.members)
        project = project._replace(members=group["members"])
        logging.debug("Updated group members: %s", project.members)

        return project

    def do(self, dryrun):

        (active_projects, _) = get_projects(self.options.project_ini)
        # update project memberships from the AP if needed
        projects = [self.update_project(p) for p in active_projects]

        projects_members = [(set(p.members), p.name) for p in projects]  # TODO: verify enddates

        # fetch slurm dbd information on accounts (projects), users and qos
        slurm_account_info = get_slurm_acct_info(SyncTypes.accounts, exclude_accounts=TIER1_PROTECTED_ACCOUNTS)
        slurm_user_info = get_slurm_acct_info(SyncTypes.users)
        slurm_qos_info = get_slurm_acct_info(SyncTypes.qos)

        # The projects do not track active state of users, so we need to fetch all accounts as well
        # we cannot use the self.get_accounts, as this implies using a timestamp for a modification date
        active_accounts = set([a["vsc_id"] for a in self.apc.account.get()[1] if a["isactive"] == True])

        logging.debug("%d accounts found", len(slurm_account_info))
        logging.debug("%d users found", len(slurm_user_info))

        sacctmgr_commands = []

        # process projects
        # add the QoS
        sacctmgr_commands += slurm_project_qos(projects, slurm_qos_info, self.options.clusters)

        sacctmgr_commands += slurm_project_accounts(
            projects,
            slurm_account_info,
            self.options.clusters,
            TIER1_PROTECTED_ACCOUNTS,
        )

        # process project members
        sacctmgr_commands += slurm_project_users_accounts(
            projects_members,
            active_accounts,  # active VSC accounts
            slurm_user_info,
            self.options.clusters,
        )

        logging.info("Executing %d commands", len(sacctmgr_commands))

        if dryrun:
            logging.info("Commands to be executed:\n")
            logging.info("\n".join([" ".join(c) for c in sacctmgr_commands]))
        else:
            execute_commands(sacctmgr_commands)


if __name__ == "__main__":
    Tier1SlurmProjectSync().main()
