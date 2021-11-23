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
from __future__ import print_function
from collections import namedtuple

import logging
import sys

from configparser import ConfigParser

from vsc.accountpage.client import AccountpageClient
from vsc.administration.slurm.sync import (
    get_slurm_acct_info, SyncTypes, SacctMgrException,
    slurm_project_accounts, slurm_project_users_accounts,
    slurm_project_qos
)
from vsc.config.base import GENT, VSC_SLURM_CLUSTERS, PRODUCTION, PILOT
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.py2vs3 import HTTPError
from vsc.utils.run import RunNoShell
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.utils.timestamp import convert_timestamp, write_timestamp, retrieve_timestamp_with_default

NAGIOS_HEADER = "sync_slurm_tier1_projects"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)


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
            name=section.replace("gpr_compute_", ""),
            group=section,
            end_date=projects_config.get(section, "end_date"),
            members=[m.strip() for m in projects_config.get(section, "members").split(",")],
#            moderators=[m.strip() for m in projects_config.get(section, "moderators").split(",")],
            cpu_hours=int(projects_config.get(section, "CPUhours", fallback=0)),
            gpu_hours=int(projects_config.get(section, "GPUhours", fallback=0)),
        ))

    return projects

def update_project(client, project):
    """
    Retrieves project information from the AP -- if any -- and updates the projects' members.
    """

    try:
        (_, group) = client.group[project.group].get()
    except HTTPError as _:
        return project

    project.members = group["members"]

    return project


def main():
    """
    Main script. The usual.
    """

    options = {
        "nagios-check-interval-threshold": NAGIOS_CHECK_INTERVAL_THRESHOLD,
        "access_token": ("OAuth2 token to access the account page REST API", None, "store", None),
        "account_page_url": (
            "URL of the account page where we can find the REST API",
            str,
            "store",
            "https://apivsc.ugent.be/django",
        ),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
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

    opts = ExtendedSimpleOption(options)
    stats = {}

    (last_timestamp, start_time) = retrieve_timestamp_with_default(
        SYNC_TIMESTAMP_FILENAME,
        start_timestamp=opts.options.start_timestamp)
    logging.info("Using timestamp %s", last_timestamp)
    logging.info("Using startime %s", start_time)

    try:
        client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + "/api/")
        host_institute = opts.options.host_institute

        projects = get_projects(opts.options.project_ini)
        # update project memberships from the AP if needed
        projects = [update_project(client, p) for p in projects]

        projects_members = [(set(p.members), p.name) for p in projects]  # TODO: verify enddates


        # fetch slurm dbd information on accounts (projects), users and qos
        slurm_account_info = get_slurm_acct_info(SyncTypes.accounts)
        slurm_user_info = get_slurm_acct_info(SyncTypes.users)
        slurm_qos_info = get_slurm_acct_info(SyncTypes.qos)

        # The projects do not track active state of users, so we need to fetch all accounts as well
        active_accounts = set([a["vsc_id"] for a in client.account.get()[1] if a["isactive"]])

        logging.debug("%d accounts found", len(slurm_account_info))
        logging.debug("%d users found", len(slurm_user_info))

        if opts.options.clusters:
            clusters = opts.options.clusters
        else:
            clusters = [cs
                for p in opts.options.cluster_classes
                for cs in VSC_SLURM_CLUSTERS[host_institute][p]
            ]

        sacctmgr_commands = []

        # create groups in the AP and set the sources for the
        # is done in another script

        # process projects
        # add the QoS
        sacctmgr_commands += slurm_project_qos(projects, slurm_qos_info, clusters)

        sacctmgr_commands += slurm_project_accounts(projects, slurm_account_info, clusters)

        # process project members
        sacctmgr_commands += slurm_project_users_accounts(
            projects_members,
            active_accounts,  # active VSC accounts
            slurm_user_info,
            clusters,
        )

        logging.info("Executing %d commands", len(sacctmgr_commands))

        if opts.options.dry_run:
            print("Commands to be executed:\n")
            print("\n".join([" ".join(c) for c in sacctmgr_commands]))
        else:
            execute_commands(sacctmgr_commands)

        if not opts.options.dry_run:
            (_, ldap_timestamp) = convert_timestamp(start_time)
            write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
            opts.epilogue("Accounts synced to slurm", stats)
        else:
            logging.info("Dry run done")

    except Exception as err:
        logging.exception("critical exception caught: %s", err)
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)


if __name__ == "__main__":
    main()
