#!/usr/bin/env python
#
# Copyright 2013-2023 Ghent University
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

See also https://bugs.schedmd.com/show_bug.cgi?id=2231#c9

A mechanism that I have heard of being used is to:
1. Configure in Slurm the total number of licenses of each type which exist (e.g. "FOO=10")
2. Using some script to poll FlexLM in order to determine how many licenses have been claimed
   by anyone on any system (e.g. "FOO=5")
3. That same script polls the Slurm system to determine how many licenses that Slurm system
   has allocated (e.g. "FOO=3")
4. We now know how many licenses were consumed outside of that one Slurm system (e.g. 5 - 3 = 2).
   Create or modify an advanced reservation that exists forever and reserves those licenses (e.g. "FOO=2").
5. Sleep  for a while
6. Go to step 2.

"""

# See example imnplementation of https://gitlab.com/ggeurts/slurm-license_monitor/-/tree/master/
#   However that is not driven by config file, and is not pseudonymous
# Current main difference: this code is to be run as cron; the other code is a daemon with possibly higher frequency

from __future__ import print_function

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
from vsc.administration.slurm.scontrol import (
    get_scontrol_info, ScontrolTypes,
    get_scontrol_config, LICENSE_RESERVATION_PREFIX,
    make_license_reservation_name, create_create_license_reservation,
    create_update_license_reservation, create_delete_reservation,
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
    #          name: pseudonymous name, to be used by users in jobs (optional: use software/key otherwise)
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
            if 'name' not in sdata:
                sdata['name'] = soft
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

    skip = set([x for x in licenses.keys() if licenses[x].get('skip', False)])
    if skip:
        logging.warning("License resources to skip: %s", skip)
        known = known - skip
        config = config - skip

    remove = sorted(list(known - config))
    new = sorted(list(config - known))
    update = sorted(list(config & known))

    new_update_cmds = []
    for name in new:
        lic = licenses[name]
        logging.debug("Command to add new license resource %s", lic)
        new_update_cmds.append(create_add_resource_license_command(
            lic['name'], lic['extern'], lic['type'], clusters, lic['count']))

    for name in update:
        lic = licenses[name]
        logging.debug("Command to update license resource %s", lic)

        # The info command does not use the "withclusters" option, so no cluster configuration details are shown
        #    In case of new clusters, run with --force_update

        # Default supported modification is updated count
        if force_update or lic['count'] != info[name].Count:
            new_update_cmds.append(create_modify_resource_license_command(
                lic['name'], lic['extern'], lic['type'], lic['count']))

    # Cleanup licenses
    remove_cmds = []
    for name in remove:
        lic = info[name]
        logging.debug("Command to remove license resource %s", lic)
        remove_cmds.append(create_remove_resource_license_command(lic.Name, lic.Server, lic.ServerType))

    return new_update_cmds, remove_cmds


def update_license_reservations(licenses, cluster, partition, ignore_reservations, force_update):
    """
    Create/update the license reservations for each cluster
    """
    # convert licenses to dict with reservation names
    rlicenses = {}
    for licname, lic in licenses.items():
        lic['fullname'] = licname
        rlicenses[make_license_reservation_name(licname)] = lic

    # Check this is the correct cluster
    #   in theory, we can also issue all scontrol commands with extra "cluster name_of_cluster" args
    slurm_config = get_scontrol_config()
    if cluster != slurm_config.ClusterName:
        logging.error("Expected cluster %s, got %s (%s)", cluster, slurm_config.ClusterName, slurm_config)
        raise Exception("Wrong cluster")

    partitions = get_scontrol_info(ScontrolTypes.partition)
    if partition not in partitions:
        logging.error("Expected partiton %s, only have %s", partition, partitions)
        raise Exception("Wrong partition")


    # Get the licenses
    #    This cluster should see all licenses, incl their usage
    # Convert to dict with reservation names
    lics = dict([(make_license_reservation_name(k), v) for k, v in get_scontrol_info(ScontrolTypes.license).items()])
    logging.debug("Existing licenses %s", lics)

    # Get all existing license reservations
    #    only license reservations
    #       remove the ignore_reservations also
    # The LICENSE_ONLY flag does not show up in flags
    ress = dict([(k, v) for k, v in get_scontrol_info(ScontrolTypes.reservation).items()
                 if v.Licenses is not None
                 and v.ReservationName.startswith(LICENSE_RESERVATION_PREFIX)
                 and k not in ignore_reservations
                 ])
    logging.debug("Existing license reservations %s", ress)

    known = set(list(ress.keys()))
    config = set(list(rlicenses.keys()))

    skip = set([x for x in rlicenses.keys() if rlicenses[x].get('skip', False)])
    if skip:
        logging.warning("License reservations to skip: %s", skip)
        known = known - skip
        config = config - skip


    remove = sorted(list(known - config))
    new = sorted(list(config - known))
    update = sorted(list(config & known))


    new_update_cmds = []

    for res in new:
        lic = rlicenses[res]
        logging.debug("Command to add new license reservation %s", lic)
        # no reservation yet, in_use is the starting value
        new_update_cmds.append(create_create_license_reservation(lic['fullname'], lic['in_use'], partition))

    for res in update:
        lic = rlicenses[res]
        logging.debug("Command to update license reservation %s", lic)

        current_value = lics[res].Reserved

        # license in_use reported by server should be corrected by license known by slurm to be allocated to jobs
        #    problem is: license known by slurm in use doesn't mean in_use by server
        #         eg job is started, be application not yet started, or not using as much
        # value is difference between seen as Used by slurm and reported in_use
        #     The externally used licenses remain as Free according to slurm, but are not usable by slurm.
        #     They are reserved for user root.
        used = lics[res].Used
        in_use = lic['in_use']
        if used > in_use:
            # jobs claiming to use/need more licenses?
            logging.debug("More %s licenses used according to slurm %s than reported by server %s", res, used, in_use)
            value = 0
        else:
            value = in_use - used

        if force_update or value != current_value:
            new_update_cmds.append(create_update_license_reservation(lic['fullname'], value))

    # Cleanup reservations
    remove_cmds = []
    for res in remove:
        logging.debug("Command to remove license reservation %s", res)
        remove_cmds.append(create_delete_reservation(res))

    return new_update_cmds, remove_cmds


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
        extopts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    extopts.epilogue("external licenses sync complete", None)


if __name__ == "__main__":
    main()
