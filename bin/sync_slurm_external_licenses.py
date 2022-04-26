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

import datetime
import json
import logging
import os
import re
import sys
import tempfile

from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.run import RunNoShell
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.administration.slurm.sync import execute_commands
from vsc.administration.slurm.sacctmgr import (
    get_slurm_sacct_info, SacctMgrTypes,
    create_add_resource_license_command,
    create_remove_resource_license_command,
    create_modify_resource_license_command,
)
from vsc.config.base import VSC_SLURM_CLUSTERS, PRODUCTION, PILOT, GENT


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

    res = {}

    if license_type == FLEXLM:
        # make tempfile file 'SERVER hostname AABBCCDDEEFF port' (yes, with fake MAC)
        (fd, fn) = tempfile.mkstemp(suffix='.flexlm_fake_lic')
        try:
            with os.fdopen(fd, 'w') as fh:
                fh.write('SERVER %s AABBCCDDEEFF %s\n' % (server, port))
            # lmutil lmstat -a -c tmpfile
            (ec, output) = RunNoShell.run([tool, 'lmstat', '-a', '-c', fn])
            if ec != 0:
                raise Exception("Failed to run flexlm tool")
        finally:
            os.unlink(fn)

        # parse output
        parsed = _parse_lmutil(output)

        #  For every toolname, add total and in_use
        for data in parsed:
            name = data.pop('name')
            res[name] = data
    else:
        res = None
        logging.error("Unsupported license_type %s for server %s", license_type, server)

    logging.debug("license_type %s for server %s port %s returned %s", license_type, server, port, res)

    return res


def licenses_data(config_filename, default_tool):
    """
    Read license JSON file, add some default values, retrieve license server data and add it
    Return dict: key = full pseudonymous name combo (software_name@name), value another dict with count, in_use
    """

    res = {}
    # parse config file in JSON
    #    need following data
    #      key of dict, used as license server for software name (eg pseudonymous name for company X)
    #        server
    #        port
    #        license_type: default flexlm
    #        tool = path to eg lmutil: default from options
    #        software: key of dict is name reported by tool
    #          name: pseudonymous name, to be used by users in jobs
    #          count: number of licenses avail
    with open(config_filename) as fh:
        all_extern_data = json.load(fh)

    all_externs = sorted(all_extern_data.keys())  # sorted for reproducible tests
    for extern in all_externs:
        edata = all_extern_data[extern]

        if 'license_type' not in edata:
            edata['license_type'] = FLEXLM
        if 'tool' not in edata:
            edata['tool'] = default_tool
        # for each name, retrieve data from server and augment software count with total and in_use data
        #    compare with total count (and report some error/warning if this goes out of sync)
        #       if server is unreachable, set number in_use equal to count: i.e. all is in use
        lics = retrieve_license_data(edata['license_type'], edata['tool'], edata['server'], edata['port'])

        eknown = set(lics.keys())

        software = edata['software']
        econfig = set(software.keys())

        missing = econfig - eknown
        if missing:
            logging.error("Configured licenses for extern %s for software %s are not reported back", extern, missing)

        for soft in missing:
            # Add it to results, so we can keep any existing resource/reservation
            software[soft]['skip'] = True
        for soft in econfig - missing:
            software[soft].update(lics[soft])

        for soft, sdata in software.items():
            sdata['extern'] = extern
            sdata['type'] = edata['license_type']
            res["%s@%s" % (sdata['name'], extern)] = sdata

    return res


def update_licenses(licenses, clusters, ignore_resources, force_update):
    """
    Create/update the license sacctmgr commands for resources
    """

    # Get all existing license resources
    #   only license resrouces
    #   remove the ignore_resources also

    info = get_slurm_sacct_info(SacctMgrTypes.resource)
    logging.debug("%d unfiltered resources found: %s", len(info), info)

    info = [resc for resc in info if resc and resc.Type == 'License' and resc.Name not in ignore_resources]
    logging.debug("%d license resources found: %s", len(info), info)

    info = dict([("%s@%s" % (resc.Name, resc.Server), resc) for resc in info])

    known = set(list(info.keys()))
    config = set(list(licenses.keys()))

    remove = known - config
    new = config - known
    update = config & known

    new_update_cmds = []
    for name in new:
        lic = licenses[name]
        new_update_cmds.append(create_add_resource_license_command(
            lic['name'], lic['extern'], lic['type'], clusters, lic['count']))

    for name in update:
        lic = licenses[name]

        # The info command does not use the "withclusters" option, so no cluster configuration details are shown
        #    In case of new clusters, run with --force_update

        # Default supported modification is updated count
        if force_update or lic['count'] != info[name].Count:
            new_update_cmds.append(create_modify_resource_license_command(
                lic['name'], lic['extern'], lic['type'], clusters, lic['count']))

    # Cleanup licenses
    remove_cmds = []
    for name in remove:
        lic = info[name]
        remove_cmds.append(create_remove_resource_license_command(lic.Name, lic.Server, lic.ServerType))

    return new_update_cmds, remove_cmds


def update_license_reservations(licenses, cluster, partition, ignore_reservations, force_update):
    """
    Create/update the license reservations for each cluster
    """
    # Get all existing license reservations
    #    only license reservations
    #       remove the ignore_reservations also
    #[root@master39 ~]# scontrol show lic  --oneliner
    #LicenseName=comsol3@bogus Total=2 Used=0 Free=2 Reserved=0 Remote=yes


    # license in_use reported by server should be corrected by license known by slurm to be allocated to jobs
    #    problem is: license known by slurm in use doesn't mean in_use by server
    #         eg job is started, be application not yet started, or not using as much

    new_update = []
    remove = []

    # Create/update the license reservation data
    #    What endtime? Safe thing is to block the jobs from executing, so make a never ending reservation?
    start = datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(seconds=10), "%Y-%m-%dT%H:%M:%S")
    #CMD="scontrol update reservation Reservation=external_"+lic
    #  Licenses="+lic+"@"+server+":"+str(difference)+"
    #CMD="scontrol create reservation Reservation=external_"+lic+"@"+server+"
    #  Licenses="+lic+"@"+server+":"+str(difference)+"
    #  StartTime=start
    #  duration="infinite"
    #  partition=cluster
    #  user="root" flags=["LICENSE_ONLY"] partition=partition

    # Cleanup reservations

    return new_update, remove


def main():
    """
    Main script. The usual.
    """

    options = {
        "licenses": ('JSON file with required license information', None, 'store', "/etc/%s.json" % NAGIOS_HEADER),
        "force_update": ('No compare logic, update all found license resources and/or reservations',
                         None, 'store_true', False),
        "tool": ('Default license tool path', None, 'store', None),
        "ignore_resources": ('List of license resources to ignore', "strlist", 'store', []),
        "ignore_reservations": ('List of license reservations to ignore', "strlist", 'store', []),
        "reservation_cluster": ("Cluster that will hold the reservation", None, 'store', None),
        "reservation_partition": ("Partition on cluster that will hold the reservation", None, 'store', None),
        "host_institute": ('Name of the institute where this script is being run', str, 'store', GENT),
        "resource_clusters": ("Cluster(s) that have access to the license resources", "strlist", "store", []),
        "resource_classes": ("Classes of clusters that have access to the license resources",
                             "strlist", "store", [PRODUCTION, PILOT]),
    }

    extopts = ExtendedSimpleOption(options)
    opts = extopts.options

    if opts.resource_clusters:
        resource_clusters = opts.resource_clusters
    else:
        resource_clusters = [
            cs
            for p in opts.resource_classes
            for cs in VSC_SLURM_CLUSTERS[opts.host_institute][p]
        ]

    try:

        if not opts.reservation_cluster:
            raise Exception("Missing reservation_cluster option")
        if opts.reservation_cluster not in resource_clusters:
            logging.error("reservation_cluster %s option must be a in resource_clusters %s",
                          opts.reservation_cluster, resource_clusters)
            raise Exception("reservation_cluster option must be a in resource_clusters")
        if not opts.reservation_partition:
            raise Exception("Missing reservation_partition option")

        licenses = licenses_data(opts.licenses, opts.tool)

        sacct_new_update, sacct_remove = update_licenses(
            licenses, resource_clusters, opts.ignore_resources, opts.force_update)

        scontrol_new_update, scontrol_remove = update_license_reservations(
            licenses, opts.reservation_cluster, opts.reservation_partition, opts.ignore_reservations, opts.force_update)

        # remove is in reverse order
        all_commands = sacct_new_update + scontrol_new_update + scontrol_remove + sacct_remove
        if opts.dry_run:
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
