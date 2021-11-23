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
from vsc.accountpage.client import AccountpageClient
from vsc.config.base import GENT, VSC_SLURM_CLUSTERS, PRODUCTION, PILOT
from vsc.utils.missing import nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.run import RunNoShell
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.utils.timestamp import convert_timestamp, write_timestamp, retrieve_timestamp_with_default

NAGIOS_HEADER = "sync_ap_tier1_projects"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)


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

    for section in projects_config.sections():
        if not section.startswith('gpr_compute'):
            continue

        projects.append(ProjectIniConfig(
            name=section,
            description=projects_config.get(section, "description", fallback=section),
            end_date=projects_config.get(section, "end_date"),
            members=set([m.strip() for m in projects_config.get(section, "members").split(",")]),
            moderators=set([m.strip() for m in projects_config.get(section, "moderators").split(",")]),
        ))

    return projects


class Tier1APProjectSync(Sync):
    CLI_OPTIONS = {
        "clusters": (
            "Cluster(s) (comma-separated) to sync for. "
            "Overrides <host_institute>_SLURM_COMPUTE_CLUSTERS that are in production.",
            "strlist",
            "store",
            [],
        ),
        'start_timestamp': ('Timestamp to start the sync from', str, 'store', None),
        'cluster_classes': (
            'Classes of clusters that should be synced, comma-separated',
            "strlist",
            'store',
            [PRODUCTION, PILOT]
        ),
        'project_ini': ('Ini file with projects information', str, 'store', None),
    }


    def do(self, dryrun):

        # TODO: take end_date into account

        projects = get_projects(self.options.project_ini)
        projects_members = [(set(p.members), p.name) for p in projects]  # TODO: verify enddates

        # get all the groups that corresponds to projects
        active_groups, inactive_groups = self.get_groups(modified_since="20211101")
        active_group_names = set([g.vsc_id for g in active_groups])
        active_accounts, inactive_accounts = self.get_accounts()

        if self.options.clusters:
            clusters = self.options.clusters
        else:
            clusters = [cs
                for p in self.options.cluster_classes
                for cs in VSC_SLURM_CLUSTERS[host_institute][p]
            ]

        # create groups in the AP and set the sources for the
        # is done in another script

        # create the projects groups in the AP
        for project in projects:
            if project.name in active_group_names and False:
                # update the members if needed
                # this should no longer be needed, once people can populate the AP groups
                (_, project_group) = self.apc.group[project.name].get()

                current_members = set(project_group["members"])
                current_moderators = set(project_group["moderators"])

                for member in project.members - current_members:
                    logging.debug("Add member %s to group %s", member, project.name)
                    self.apc.group[project.name].member[member].post()

                for member in current_members - project.members:
                    logging.debug("Delete members %s from group %s", member, project.name)
                    self.apc.group[project.name].member[member].delete()

                #for moderator in project.moderators - current_moderators:
                    #logging.debug("Set moderator status for member %s of group %s", moderator, project. name)
                    #self.apc.group[project.name].member[moderator].add.moderator["true"].patch()

                #for moderator in current_moderators - project.moderators:
                    #logging.debug("Removing moderator status for member %s of group %s", moderator, project. name)
                    #self.apc.group[project.name].member[moderator].add.moderator["false"].patch()

            else:
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
                _, _ = self.apc.ragroup.post(body=data)
                _, _ = self.apc.autogroup["gt1_dodrio_activeusers"].source[project.name].add.post()




if __name__ == '__main__':
    Tier1APProjectSync().main()
