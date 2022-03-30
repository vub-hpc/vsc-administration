#!/usr/bin/env python
#
# Copyright 2013-2022 Ghent University
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
This script synchronises external license server information with slurm license tracking.

The script must result in an idempotent execution, to ensure nothing breaks.
"""

# See example imnplementation of https://gitlab.com/ggeurts/slurm-license_monitor/-/tree/master/
#   However that is not driven by config file, and is not pseudonymous
# Current main difference: this code is to be run as cron; the other code is a daemon with possibly higher frequency

from __future__ import print_function

import logging
import sys

from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.config.base import GENT, VSC_SLURM_CLUSTERS, INSTITUTE_VOS_BY_INSTITUTE, PRODUCTION, PILOT
from vsc.utils.run import RunNoShell
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_slurm_external_licenses"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

def execute_commands(commands):
    """Run the specified commands"""

    for command in commands:
        logging.info("Running command: %s", command)

        # if one fails, we simply fail the script and should get notified
        (ec, _) = RunNoShell.run(command)
        if ec != 0:
            raise SacctMgrException("Command failed: {0}".format(command))

def get_licenses():
    # parse config file, to be read from remainder of generloption default config
    #    need following data
    #    name -> section, some pseudonymous name
    #    server
    #    type (default flex?)
    #    tool = path to eg lmutil
    #    count = number of known licenses

def update_licenses(licenses):
    # Create/update the license sacctmghr resource data

    #  don't use server for server, but pseudonymous name as well
    #sacctmgr add resource name=comsol count=2 server=1718@flexlm-server servertype=flexlm type=license

    # Cleanup licenses
    return new_update, remove

def update_license_reservations(licenses, clusters):
    # Check for each licnese the number of licenses in use
    #    optionally find total number avail, and compare with count (and report some error/warning if this goes out of sync)

    # Create/update the license reservation data
    #    For which clusters? only one or all of them?

    # Cleanup reservations

    return new_update, remove


def main():
    """
    Main script. The usual.
    """

    options = {
        "clusters": (
            "Cluster(s) (comma-separated) to sync for. "
            "Overrides <host_institute>_SLURM_COMPUTE_CLUSTERS that are in production.",
            "strlist",
            "store",
            [],
        ),
        'cluster_classes': (
            'Classes of clusters that should be synced, comma-separated',
            "strlist",
            'store',
            [PRODUCTION, PILOT]
        ),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        if opts.options.clusters:
            clusters = opts.options.clusters
        else:
            clusters = [cs
                for p in opts.options.cluster_classes
                for cs in VSC_SLURM_CLUSTERS[host_institute][p]
            ]

        licenses = get_licenses()

        sacct_new_update, sacct_remove = update_licenses(licenses)

        scontrol_new_update, scontrol_remove = update_license_reservations(licenses, clusters)

        # remove is in reverse order
        all_commands = sacct_new_update + scontrol_new_update + scontrol_remove + sacct_remove
        if opts.options.dry_run:
            print("Commands to be executed:\n")
            print("\n".join([" ".join(c) for c in commands]))
        else:
            logging.info("Executing %d commands", len(commands))
            execute_commands(commands)

    except Exception as err:
        logging.exception("critical exception caught: %s", err)
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)


if __name__ == "__main__":
    main()
