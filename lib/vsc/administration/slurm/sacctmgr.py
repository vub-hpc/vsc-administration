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
sacctmgr commands
"""
import logging
from enum import Enum

from vsc.accountpage.wrappers import mkNamedTupleInstance
from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.utils.missing import namedtuple_with_defaults
from vsc.utils.run import asyncloop

from vsc.administration.slurm.scancel import create_remove_user_jobs_command

SLURM_SACCT_MGR = "/usr/bin/sacctmgr"

SLURM_ORGANISATIONS = {
    ANTWERPEN: 'uantwerpen',
    BRUSSEL: 'vub',
    GENT: 'ugent',
    LEUVEN: 'kuleuven',
}


class SacctMgrException(Exception):
    pass


class SacctMgrTypes(Enum):
    accounts = "accounts"
    users = "users"
    qos = "qos"
    resource = "resource"


# Fields for Slurm 21.08.
# FIXME: at some point this should be versioned

SacctUserFields = [
    "User", "Def_Acct", "Admin", "Cluster", "Account", "Partition", "Share",
    "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
    "QOS", "Def_QOS"
]

SacctAccountFields = [
    "Account", "Descr", "Org", "Cluster", "ParentName", "User", "Share",
    "GrpJobs", "GrpNodes", "GrpCPUs", "GrpMem", "GrpSubmit", "GrpWall", "GrpCPUMins",
    "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
    "QOS", "Def_QOS"
]

SacctQosFields = [
    "Name", "Priority", "GraceTime", "Preempt", "PreemptExemptTime", "PreemptMode",
    "Flags", "UsageThres", "UsageFactor", "GrpTRES", "GrpTRESMins", "GrpTRESRunMins",
    "GrpJobs", "GrpSubmit", "GrpWall", "MaxTRES", "MaxTRESPerNode", "MaxTRESMins",
    "MaxWall", "MaxTRESPU", "MaxJobsPU", "MaxSubmitPU", "MaxTRESPA", "MaxJobsPA",
    "MaxSubmitPA", "MinTRES"
]

SacctResourceFields = [
    "Name", "Server", "Type", "Count", "PCT__Allocated", "ServerType",
]

IGNORE_USERS = ["root"]
IGNORE_ACCOUNTS = ["root"]
IGNORE_QOS = ["normal"]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)
SlurmQos = namedtuple_with_defaults('SlurmQos', SacctQosFields)
SlurmResource = namedtuple_with_defaults('SlurmResource', SacctResourceFields)


def mkSlurmAccount(fields):
    """Make a named tuple from the given fields."""
    account = mkNamedTupleInstance(fields, SlurmAccount)
    if account.Account in IGNORE_ACCOUNTS:
        return None
    return account


def mkSlurmUser(fields):
    """Make a named tuple from the given fields."""
    user = mkNamedTupleInstance(fields, SlurmUser)
    if user.User in IGNORE_USERS:
        return None
    return user


def mkSlurmQos(fields):
    """Make a named tuple from the given fields"""
    qos = mkNamedTupleInstance(fields, SlurmQos)
    return qos


def mkSlurmResource(fields):
    """Make a named tuple from the given fields"""
    fields['Count'] = int(fields['Count'])
    resource = mkNamedTupleInstance(fields, SlurmResource)
    return resource


def mksacctmgr(mode):
    """Decorator to prefix common sacctmgr code for mode"""
    def decorator(function):
        def wrapper(*args, **kwargs):
            prefix = [SLURM_SACCT_MGR, "-i", mode]
            return prefix + function(*args, **kwargs)
        return wrapper
    return decorator


def parse_slurm_sacct_line(header, line, info_type, user_field_number, account_field_number, exclude=None):
    """Parse the line into the correct data type."""
    fields = line.split("|")

    if info_type == SacctMgrTypes.accounts:
        if exclude and fields[account_field_number] in exclude:
            return None
        if fields[user_field_number]:
            # association information for a user. Users are processed later.
            return None
        creator = mkSlurmAccount
    elif info_type == SacctMgrTypes.users:
        creator = mkSlurmUser
    elif info_type == SacctMgrTypes.qos:
        creator = mkSlurmQos
    elif info_type == SacctMgrTypes.resource:
        creator = mkSlurmResource
    else:
        return None

    return creator(dict(zip(header, fields)))


def parse_slurm_sacct_dump(lines, info_type, exclude=None):
    """Parse the sacctmgr dump from the listing."""
    acct_info = set()

    header = [w.replace(' ', '_').replace('%', 'PCT_') for w in lines[0].rstrip().split("|")]
    header_names = [h.lower() for h in header]

    if info_type == SacctMgrTypes.accounts:
        user_field_number = header_names.index("user")
        account_field_number = header_names.index("account")
    else:
        user_field_number = None
        account_field_number = None

    for line in lines[1:]:
        logging.debug("line %s", line)
        line = line.rstrip()
        try:
            info = parse_slurm_sacct_line(
                header, line, info_type, user_field_number, account_field_number, exclude=exclude
            )
        except Exception as err:
            logging.exception("Slurm sacct parse dump: could not process line %s [%s]", line, err)
            raise
        # This fails when we get e.g., the users and look at the account lines.
        # We should them just skip that line instead of raising an exception
        if info:
            acct_info.add(info)

    return acct_info


def get_slurm_sacct_info(info_type, exclude=None):
    """Get slurm info for the given clusterself.

    @param info_type: SacctMgrTypes
    """
    (exitcode, contents) = asyncloop([
        SLURM_SACCT_MGR,
        "-s",
        "-P",
        "list",
        info_type.value,
    ])
    if exitcode != 0:
        raise SacctMgrException("Cannot run sacctmgr")

    info = parse_slurm_sacct_dump(contents.splitlines(), info_type, exclude=exclude)

    return info


@mksacctmgr('add')
def create_add_account_command(account, parent, organisation, cluster, fairshare=None, qos=None):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    command = [
        "account",
        account,
        "Parent={0}".format(parent or "root"),
        "Organization={0}".format(SLURM_ORGANISATIONS[organisation]),
        "Cluster={0}".format(cluster)
    ]

    if fairshare is not None:
        command.append("Fairshare={0}".format(fairshare))
    if qos is not None:
        command.append("Qos={0}".format(qos))

    logging.debug(
        "Adding command to add account %s with Parent=%s Cluster=%s Organization=%s",
        account,
        parent,
        cluster,
        organisation,
        )

    return command


@mksacctmgr('modify')
def create_default_account_command(user, account, cluster):
    """Creates the command the set a default account for a user.

    @param user: the user name in Slurm
    @param accont: the account name in Slurm
    @param cluster: cluster for which the user sets a default account
    """
    command = [
        "user",
        "Name={0}".format(user),
        "Cluster={0}".format(cluster),
        "set",
        "DefaultAccount={0}".format(account),
    ]
    logging.debug(
        "Creating command to set default account to %s for %s on cluster %s",
        account,
        user,
        cluster)

    return command


@mksacctmgr('modify')
def create_change_account_fairshare_command(account, cluster, fairshare):
    command = [
        "account",
        "name={0}".format(account),
        "cluster={0}".format(cluster),
        "set",
        "fairshare={0}".format(fairshare),
    ]
    logging.debug(
        "Adding command to change fairshare for account %s on cluster %s to %d",
        account,
        cluster,
        fairshare,
    )

    return command


@mksacctmgr('add')
def create_add_user_command(user, account, cluster, default_account=None):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    command = [
        "user",
        user,
        "Account={0}".format(account),
        "Cluster={0}".format(cluster)
    ]
    if default_account is not None:
        command.append(
            "DefaultAccount={0}".format(account),
        )
    logging.debug(
        "Adding command to add user %s with Account=%s Cluster=%s",
        user,
        account,
        cluster,
        )

    return command


def create_change_user_command(user, current_vo_id, new_vo_id, cluster):
    """Creates the commands to change a user's account.

    @returns: two lists comprising the commands
    """
    add_user_command = create_add_user_command(user, new_vo_id, cluster)
    set_default_account_command = create_default_account_command(user, new_vo_id, cluster)
    remove_former_association_jobs_command = create_remove_user_jobs_command(
        user=user,
        cluster=cluster,
        account=current_vo_id,
    )
    remove_association_user_command = create_remove_user_account_command(user, current_vo_id, cluster)

    logging.debug(
        "Adding commands to change user %s on Cluster=%s from Account=%s to DefaultAccount=%s",
        user,
        cluster,
        current_vo_id,
        new_vo_id
        )

    return [
        add_user_command,
        set_default_account_command,
        remove_former_association_jobs_command,
        remove_association_user_command
    ]


@mksacctmgr('remove')
def create_remove_user_command(user, cluster):
    """Create the command to remove a user.

    @returns: list comprising the command
    """
    command = [
        "user",
        "Name={user}".format(user=user),
        "Cluster={cluster}".format(cluster=cluster)
    ]
    logging.debug(
        "Adding command to remove user %s from Cluster=%s",
        user,
        cluster,
        )

    return command


@mksacctmgr('remove')
def create_remove_account_command(account, cluster):
    """Create the command to remove an account.

    @returns: list comprising the command
    """
    command = [
        "account",
        "Name={account}".format(account=account),
        "Cluster={cluster}".format(cluster=cluster),
    ]

    logging.debug(
        "Adding command to remove account %s from cluster %s",
        account,
        cluster,
    )

    return command


@mksacctmgr('remove')
def create_remove_user_account_command(user, account, cluster):
    """Create the command to remove a user.

    @returns: list comprising the command
    """
    command = [
        "user",
        "Name={user}".format(user=user),
        "Account={account}".format(account=account),
        "Cluster={cluster}".format(cluster=cluster)
    ]

    logging.debug(
        "Adding command to remove user %s with account %s from Cluster=%s",
        user,
        account,
        cluster,
        )

    return command


@mksacctmgr('add')
def create_add_qos_command(name):
    """Create the command to add a QOS

    @returns: the list comprising the command
    """
    command = [
        "qos",
        "Name={0}".format(name)
    ]

    return command


@mksacctmgr('remove')
def create_remove_qos_command(name):
    """Create the command to remove a QOS.

    @param name: the name of the QOS to remove

    @returns: the list comprising the command
    """
    command = [
        "qos",
        "where",
        "Name={0}".format(name),
    ]

    return command


@mksacctmgr('modify')
def create_modify_qos_command(name, settings):
    """Create the command to modify a QOS

    @param name: the name of the QOS to modify
    @param settings: dict with the items that should be set (key/value pairs)

    @returns: the list comprising the command
    """
    command = [
        "qos",
        name,
        "set",
        "flags=NoDecay,DenyOnLimit",
    ]

    for k, v in settings.items():
        command.append("{0}={1}".format(k, v))

    return command


@mksacctmgr('add')
def create_add_resource_license_command(name, server, stype, clusters, count):
    """Create the command to add a license resource

    @returns: the list comprising the command
    """
    command = [
        "resource",
        "Type=license",
        "Name={0}".format(name),
        "Server={0}".format(server),
        "ServerType={0}".format(stype),
        "Cluster={0}".format(",".join(clusters)),
        "Count={0}".format(count),
        "PercentAllowed=100",
    ]

    return command


@mksacctmgr('remove')
def create_remove_resource_license_command(name, server, stype):
    """Create the command to remove a license resource.

    @returns: the list comprising the command
    """
    command = [
        "resource",
        "where",
        "Type=license",
        "Name={0}".format(name),
        "Server={0}".format(server),
        "ServerType={0}".format(stype),
    ]

    return command


@mksacctmgr('modify')
def create_modify_resource_license_command(name, server, stype, count):
    """Create the command to modify a license resource

    @returns: the list comprising the command
    """
    command = [
        "resource",
        "where",
        "Name={0}".format(name),
        "Server={0}".format(server),
        "ServerType={0}".format(stype),
        "set",
        "Count={0}".format(count),
    ]

    return command
