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
This script synchronises the users and projects from the Resource App to the Ac

FIXME: For now, it takes its data fom an ini file.

The script must result in an idempotent execution, to ensure nothing breaks.
"""
from collections import namedtuple

import logging
import sys

from configparser import ConfigParser

from vsc.accountpage.sync import Sync
from vsc.config.base import GENT, VSC_SLURM_CLUSTERS, PRODUCTION, PILOT


VSC_ADMIN_GROUPS = ("gadminforever", "badmin", "l_sysadmin", "gt1_dodrio_vscadmins")

AP_ACTIVE_USERS_AUTOGROUP = "gt1_dodrio_activeusers"


ProjectIniConfig = namedtuple("ProjectIniConfig",
    ["name", "description", "end_date", "members", "moderators"]
)

def get_projects(projects_ini):
    """
    This reads the ini file which contains information on the projects

    [project_name]
    end_date: YYYYMMDD
    members: comma-separated list of VSC IDs of project members

    """

    projects_config = ConfigParser()
    with open(projects_ini) as pini:
        projects_config.read_file(pini)

    projects = []
    other_sources = []

    for section in projects_config.sections():
        if section in VSC_ADMIN_GROUPS:
            other_sources.append(section)

        if section.startswith('gpr_compute'):
            projects.append(ProjectIniConfig(
                name=section,
                description=projects_config.get(section, "description", fallback=section),
                end_date=projects_config.get(section, "end_date"),
                members=set([m.strip() for m in projects_config.get(section, "members").split(",")]),
                moderators=set([m.strip() for m in projects_config.get(section, "moderators").split(",")]),
            ))

    return (projects, other_sources)

class Tier1APProjectSync(Sync):
    CLI_OPTIONS = {
        'project_ini': ('Ini file with projects information', str, 'store', None),
    }


    def do(self, dryrun):

        # TODO: take end_date into account

        (projects, other_sources) = get_projects(self.options.project_ini)
        projects_members = [(set(p.members), p.name) for p in projects]  # TODO: verify enddates

        logging.debug("Current projects: %s", [p.name for p in projects])
        logging.debug("Current other sources: %s", other_sources)

        # get all the groups that corresponds to projects
        active_groups, _ = self.get_groups(modified_since="20211101")
        active_group_names = set([g.vsc_id for g in active_groups])
        active_accounts, inactive_accounts = self.get_accounts()

        active_users_autogroup_sources = self.apc.autogroup[AP_ACTIVE_USERS_AUTOGROUP].get()[1]["sources"]

        logging.debug("Current %s sources: %s", AP_ACTIVE_USERS_AUTOGROUP, active_users_autogroup_sources)

        # create the projects groups in the AP
        for source in other_sources:
            if source not in active_users_autogroup_sources:
                if dryrun:
                    logging.info("Calling apc.autogroup[%s].source[%s].add.post()", AP_ACTIVE_USERS_AUTOGROUP, source)
                else:
                    self.apc.autogroup[AP_ACTIVE_USERS_AUTOGROUP].source[source].add.post()

        for project in projects:
            if project.name not in active_group_names:
                data = {
                    "name": project.name.replace("gpr_compute_", ""),  # this will be regenerated, the RA should only have the suffix
                    "members": [
                        { "vsc_id": m, "moderator": m in project.moderators }
                        for m in project.members.union(project.moderators)
                    ],
                    "description": project.description,
                    "metadata": {
                        "institute": "gent",
                        "label": project.name.split("_")[1] # gpr_compute_2021_042 -> compute
                    }
                }
                if dryrun:
                    logging.info("Calling apc.ragroup.post() with body %s", data)
                else:
                    _, _ = self.apc.ragroup.post(body=data)

            if project.name not in active_users_autogroup_sources:
                if dryrun:
                    logging.info("Calling apc.autogroup[%s].source[%s].add.post()", AP_ACTIVE_USERS_AUTOGROUP, project.name)
                else:
                    _, _ = self.apc.autogroup[AP_ACTIVE_USERS_AUTOGROUP].source[project.name].add.post()




if __name__ == '__main__':
    Tier1APProjectSync().main()
