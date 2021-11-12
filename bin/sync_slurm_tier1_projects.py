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
This script synchronises the users and VO's from the HPC account page to the Slurm database.

The script must result in an idempotent execution, to ensure nothing breaks.
"""
from __future__ import print_function
from collections import namedtuple

import logging
import sys

from configparser import ConfigParser

from vsc.accountpage.client import AccountpageClient
from vsc.administration.slurm.sync import get_slurm_acct_info, SyncTypes, SacctMgrException
from vsc.administration.slurm.sync import slurm_project_accounts, slurm_project_users_accounts
from vsc.config.base import GENT, VSC_SLURM_CLUSTERS, PRODUCTION, PILOT
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.run import RunNoShell
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.utils.timestamp import convert_timestamp, write_timestamp, retrieve_timestamp_with_default

NAGIOS_HEADER = "sync_slurm_acct"
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


ProjectIniConfig = namedtuple("ProjectIniConfig", ["name", "end_date", "members"])

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
        projects.append(ProjectIniConfig(
            name=section,
            end_date=projects_config.get(section, "end_date"),
            members=projects_config.get("gpr_compute_testproject1", "members").split(",")
        ))

    return projects


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

        slurm_account_info = get_slurm_acct_info(SyncTypes.accounts)
        slurm_user_info = get_slurm_acct_info(SyncTypes.users)

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

        projects = get_projects(opts.options.project_ini)

        projects_members = [(set(p.members), p.name) for p in projects]  # TODO: verify enddates

        # process projects
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
