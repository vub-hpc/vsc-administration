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
import os
import re
import sys
import tempfile

from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.administration.slurm.sync import execute_commands

NAGIOS_HEADER = "sync_slurm_external_licenses"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

FLEXLM = 'flexlm'

LMUTIL_LMSTAT_REGEXP = re.compile("""
    Users\s+of\s+(?P<name>\w+):
    \s+\(
        Total\s+of\s+(?P<total>\d+)\s+licenses?\s+issued;\s+
        Total\s+of\s+(?P<in_use>\d+)\s+licenses?\s+in\s+use
    \)""", re.VERBOSE)


def _parse_lmutil(output):
    """Parse the lmutil output"""
    res = []

    for match in LMUTIL_LMSTAT_REGEXP.finditer(output):
        matches = match.groupdict()
        for key in ['total', 'in_use']:
            matches[key] = int(matches[key])
        res.append(matches)

    return res


def retrieve_license_data(license_type, tool, server, port):
    """
    Run tool to retrieve all license data from server/port.
    Return dict with key the toolname and value another dict with total and in_use as keys
    """

    res = None

    if license_type == FLEXLM:
        # make tempfile file 'SERVER hostname AABBCCDDEEFF port' (yes, with fake MAC)
        (fd, fn) = tempfile.mkstemp(suffix='.flexlm_fake_lic')
        try:
            with os.fdopen(fd, 'w') as fh:
                fh.write('SERVER %s AABBCCDDEEFF %s\n' % (server, port))
            #lmutil lmstat -a -c tmpfile
            (ec, output) = RunNoShell.run([tool, 'lmstat', '-a', '-c', fn])
            # parse output
            parsed = _parse_lmutil(output)
            #  For every toolname, add total and in_use
        finally:
            os.unlink(fn)
    else:
        logging.error("Unsupported license_type %s for server %s", license_type, server)

    return res


def licenses_data(config_filename):
    """
    Read license JSON file, add some default values, retrieve license server data and add it
    Return dict: key = full pseudonymous name combo (name + sofwtare_name), value another dict with count, in_use
    """

    data = {}

    # parse config file in JSON
    #    need following data
    #      name: key of dict, prefix for software name
    #        server
    #        port
    #        license_type (default flexlm?)
    #        tool = path to eg lmutil if not default
    #        software: list of dicts
    #          name: pseudonymous name, to be used by users in jobs
    #          toolname: name reported by tool
    #          count: number of licenses avail

    # for each name, retrieve data from server and augment software count with total and in_use data
    #    compare with total count (and report some error/warning if this goes out of sync)
    #       if server is unreachable, set number in_use equal to count: i.e. all is in use


    # licnese in_use reported by server should be corrected by license known by slurm to be allocated to jobs
    #    problem is: license known by slurm in use doesn't mean in_use by server
    #         eg job is started, be application not yet started, or not using as much

    return data


def update_licenses(licenses, ignore_resources):
    """
    Create/update the license sacctmgr resource data
    """
    # Get all existing license resources
    #   only license resrouces
    #   remove the ignore_resources also

    new_update = []
    remove = []

    #  don't use server for server, but pseudonymous name+software name as well
    #sacctmgr add resource name=comsol count=2 server= servertype=flexlm type=license

    # Cleanup licenses

    return new_update, remove


def update_license_reservations(licenses, cluster, partition, ignore_reservations):
    """
    Create/update the license reservations for each cluster
    """
    # Get all existing license reservations
    #    only license reservations
    #       remove the ignore_reservations also

    new_update = []
    remove = []

    # Create/update the license reservation data
    #    What endtime? Save thing is to block the jobs from executing, so make a never ending reservation?
    #CMD="scontrol update reservation Reservation=external_"+lic+"@"+server+"
    #  Licenses="+lic+"@"+server+":"+str(difference)+"
    #  EndTime="+datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(minutes=1),"%Y-%m-%dT%H:%M:%S")
    #CMD="scontrol create reservation Reservation=external_"+lic+"@"+server+"
    #  Licenses="+lic+"@"+server+":"+str(difference)+"
    #  StartTime="+datetime.datetime.strftime(datetime.datetime.now(),"%Y-%m-%dT%H:%M:%S")+"
    #  duration=00:"+str(sched_interval/2)+"
    #  partition=cluster
    #  user=root flags=LICENSE_ONLY partition=cluster"

    # Cleanup reservations

    return new_update, remove


def main():
    """
    Main script. The usual.
    """

    options = {
        "licenses": ('JSON file with required license information', None, 'store', "/etc/%s.json" % NAGIOS_HEADER),
        "ignore-resources": ('List of license resources to ignore', "strlist", 'store', []),
        "ignore-reservations": ('List of license reservations to ignore', "strlist", 'store', []),
        "cluster": ("Cluster that will hold the reservation", None, 'store', None),
        "partition": ("Partition on cluster that will hold the reservation", None, 'store', None),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        if not opts.options.cluster:
            raise Exception("Missing cluster option")
        if not opts.options.partition:
            raise Exception("Missing partition option")

        licenses = licenses_data(opts.options.licenses)

        sacct_new_update, sacct_remove = update_licenses(licenses, opts.options.ignore_resources)

        scontrol_new_update, scontrol_remove = update_license_reservations(
            licenses, opts.options.cluster, opts.options.partition, opts.options.ignore_reservations)

        # remove is in reverse order
        all_commands = sacct_new_update + scontrol_new_update + scontrol_remove + sacct_remove
        if opts.options.dry_run:
            print("Commands to be executed:\n")
            print("\n".join([" ".join(c) for c in all_commands]))
        else:
            logging.info("Executing %d commands", len(all_commands))
            execute_commands(all_commands)

    except Exception as err:
        logging.exception("critical exception caught: %s", err)
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)


if __name__ == "__main__":
    main()
