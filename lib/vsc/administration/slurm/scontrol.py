#
# Copyright 2022-2022 Ghent University
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
scontrol commands
"""
import logging
import re
import shlex

from enum import Enum

from vsc.accountpage.wrappers import mkNamedTupleInstance
from vsc.utils.missing import namedtuple_with_defaults
from vsc.utils.run import asyncloop


SLURM_SCONTROL = "/usr/bin/scontrol"

SLURM_SCONTROL_CONFIG_REGEX = re.compile("^(.*\S)\s+=\s+(\S.*)$")


class ScontrolTypes(Enum):
    reservation = "reservation"
    license = "license"
    config = "config"
    partition = "partition"


ScontrolReservationFields = [
    'ReservationName', 'StartTime', 'EndTime', 'Duration', 'Nodes', 'NodeCnt', 'CoreCnt',
    'Features', 'PartitionName', 'Flags', 'TRES', 'Users', 'Groups', 'Accounts', 'Licenses',
    'State', 'BurstBuffer', 'Watts', 'MaxStartDelay',
]

ScontrolLicenseFields = [
    'LicenseName', 'Total', 'Used', 'Free', 'Reserved', 'Remote',
]

# Obviously, there are plenty more
ScontrolConfigFields = [
    'ClusterName', 'AccountingStorageHost',
]

ScontrolPartitionFields = [
    'PartitionName', 'AllowGroups', 'AllowAccounts', 'AllowQos', 'AllocNodes', 'Default', 'QoS', 'DefaultTime',
    'DisableRootJobs', 'ExclusiveUser', 'GraceTime', 'Hidden', 'MaxNodes', 'MaxTime', 'MinNodes', 'LLN',
    'MaxCPUsPerNode', 'Nodes', 'PriorityJobFactor', 'PriorityTier', 'RootOnly', 'ReqResv', 'OverSubscribe',
    'OverTimeLimit', 'PreemptMode', 'State', 'TotalCPUs', 'TotalNodes', 'SelectTypeParameters', 'JobDefaults',
    'DefMemPerCPU', 'MaxMemPerNode', 'TRESBillingWeights',
]

SlurmReservation = namedtuple_with_defaults('SlurmReservation', ScontrolReservationFields)
SlurmLicense = namedtuple_with_defaults('SlurmLicense', ScontrolLicenseFields)
SlurmConfig = namedtuple_with_defaults('SlurmConfig', ScontrolConfigFields)
SlurmPartition = namedtuple_with_defaults('SlurmPartition', ScontrolPartitionFields)


def mkSlurmReservation(fields):
    """Make a named tuple from the given fields"""
    reservation = mkNamedTupleInstance(fields, SlurmReservation)
    return reservation


def mkSlurmLicense(fields):
    """Make a named tuple from the given fields"""
    for key in ['Total', 'Used', 'Free', 'Reserved']:
        fields[key] = int(fields[key])
    lic = mkNamedTupleInstance(fields, SlurmLicense)
    return lic


def mkSlurmConfig(fields):
    """Make a named tuple from the given fields"""
    filtered = dict([(k, v) for k,v in fields.items() if k in ScontrolConfigFields])
    config = mkNamedTupleInstance(filtered, SlurmConfig)
    return config


def mkSlurmPartition(fields):
    """Make a named tuple from the given fields"""
    for key in ['TotalCPUs', 'TotalNodes', 'DefMemPerCPU', 'MaxMemPerNode']:
        fields[key] = int(fields[key])
    lic = mkNamedTupleInstance(fields, SlurmPartition)
    return lic


def mkscontrol(mode):
    """Decorator to prefix common sacctmgr code for mode"""
    def decorator(function):
        def wrapper(*args, **kwargs):
            prefix = [SLURM_SCONTROL,  mode]
            return prefix + function(*args, **kwargs)
        return wrapper
    return decorator


def parse_scontrol_line(line, info_type):
    """Parse the line into the correct data type."""
    # output should have eg 'Flags=' or 'Account=(null)'
    fields = dict([x.split("=", 1) for x in shlex.split(line)])

    # convert all null to None
    for key in fields.keys():
        if fields[key] == '(null)':
            fields[key] = None

    # sanity check for keys vs the fields?

    if info_type == ScontrolTypes.license:
        creator = mkSlurmLicense
    elif info_type == ScontrolTypes.reservation:
        creator = mkSlurmReservation
    elif info_type == ScontrolTypes.config:
        creator = mkSlurmConfig
    elif info_type == ScontrolTypes.partition:
        creator = mkSlurmPartition
    else:
        return None

    return creator(fields)


def parse_scontrol_dump(lines, info_type):
    """Parse the scontrol dump from the listing."""
    info = set()

    if len(lines) == 1 and lines[0].startswith('No '):
        logging.warning("Output indicates there was no result for type %s: '%s'", info_type, lines[0])
    else:
        for line in lines:
            logging.debug("line %s", line)
            line = line.rstrip()
            try:
                parsed = parse_scontrol_line(line, info_type)
            except Exception as err:
                logging.exception("Slurm scontrol parse dump: could not process line %s [%s]", line, err)
                raise

            if parsed:
                info.add(parsed)

    return info


def get_scontrol_info(info_type):
    """Get slurm info for the given clusterself.

    @param info_type: ScontrolTypes
    """
    (exitcode, contents) = asyncloop([
        SLURM_SCONTROL,
        "show",
        info_type.value,
        "--detail",
        "--oneliner",
    ])
    if exitcode != 0:
        raise Exception("Cannot run scontrol")

    lines = contents.splitlines()
    if info_type == ScontrolTypes.config:
        # there is one config, so convert the multilines in single line
        fields = []
        for line in lines:
            if SLURM_SCONTROL_CONFIG_REGEX.match(line):
                fields.append(SLURM_SCONTROL_CONFIG_REGEX.sub(r'\1="\2"', line))
        lines = [" ".join(fields)]

    info = parse_scontrol_dump(lines, info_type)

    return info
