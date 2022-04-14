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


class SyncTypes(Enum):
    accounts = "accounts"
    users = "users"
    qos = "qos"


# Fields for Slurm 20.11.
# FIXME: at some point this should be versioned

SacctUserFields = [
    "User", "Def_Acct", "Admin", "Cluster", "Account", "Partition", "Share",
    "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
    "QOS", "Def_QOS"
]

SacctAccountFields = [
    "Account", "Descr", "Org", "Cluster", "Par_Name", "User", "Share",
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


IGNORE_USERS = ["root"]
IGNORE_ACCOUNTS = ["root"]
IGNORE_QOS = ["normal"]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)
SlurmQos = namedtuple_with_defaults('SlurmQos', SacctQosFields)


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


def parse_slurm_acct_line(header, line, info_type, user_field_number, account_field_number, exclude_accounts=None):
    """Parse the line into the correct data type."""
    fields = line.split("|")

    if info_type == SyncTypes.accounts:
        if exclude_accounts and fields[account_field_number] in exclude_accounts:
            return None
        if fields[user_field_number]:
            # association information for a user. Users are processed later.
            return None
        creator = mkSlurmAccount
    elif info_type == SyncTypes.users:
        creator = mkSlurmUser
    elif info_type == SyncTypes.qos:
        creator = mkSlurmQos
    else:
        return None

    return creator(dict(zip(header, fields)))


def parse_slurm_acct_dump(lines, info_type, exclude_accounts=None):
    """Parse the accounts from the listing."""
    acct_info = set()

    header = [w.replace(' ', '_') for w in lines[0].rstrip().split("|")]
    try:
        user_field_number = [h.lower() for h in header].index("user")
    except ValueError:
        user_field_number = None
    try:
        account_field_number = [h.lower() for h in header].index("account")
    except ValueError:
        account_field_number = None

    for line in lines[1:]:
        line = line.rstrip()
        try:
            info = parse_slurm_acct_line(
                header, line, info_type, user_field_number, account_field_number, exclude_accounts
            )
            # This fails when we get e.g., the users and look at the account lines.
            # We should them just skip that line instead of raising an exception
            if info:
                acct_info.add(info)
        except Exception as err:
            logging.exception("Slurm acct sync: could not process line %s [%s]", line, err)
            raise

    return acct_info


def get_slurm_acct_info(info_type, exclude_accounts=None):
    """Get slurm account info for the given clusterself.

    @param info_type: SyncTypes
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

    info = parse_slurm_acct_dump(contents.splitlines(), info_type, exclude_accounts)

    return info


def create_add_account_command(account, parent, organisation, cluster, fairshare=None, qos=None):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    create_account_command = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "add",
        "account",
        account,
        "Parent={0}".format(parent or "root"),
        "Organization={0}".format(SLURM_ORGANISATIONS[organisation]),
        "Cluster={0}".format(cluster)
    ]

    if fairshare is not None:
        create_account_command.append("Fairshare={0}".format(fairshare))
    if qos is not None:
        create_account_command.append("Qos={0}".format(qos))

    logging.debug(
        "Adding command to add account %s with Parent=%s Cluster=%s Organization=%s",
        account,
        parent,
        cluster,
        organisation,
        )

    return create_account_command


def create_default_account_command(user, account, cluster):
    """Creates the command the set a default account for a user.

    @param user: the user name in Slurm
    @param accont: the account name in Slurm
    @param cluster: cluster for which the user sets a default account
    """
    create_default_account_command = [
        SLURM_SACCT_MGR,
        "-i",
        "modify",
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

    return create_default_account_command


def create_change_account_fairshare_command(account, cluster, fairshare):
    change_account_fairshare_command = [
        SLURM_SACCT_MGR,
        "-i",
        "modify",
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

    return change_account_fairshare_command


def create_add_user_command(user, account, cluster, default_account=None):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    create_user_command = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "add",
        "user",
        user,
        "Account={0}".format(account),
        "Cluster={0}".format(cluster)
    ]
    if default_account is not None:
        create_user_command.append(
            "DefaultAccount={0}".format(account),
        )
    logging.debug(
        "Adding command to add user %s with Account=%s Cluster=%s",
        user,
        account,
        cluster,
        )

    return create_user_command


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
    remove_association_user_command = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "delete",
        "user",
        "name={0}".format(user),
        "Account={0}".format(current_vo_id),
        "Cluster={0}".format(cluster),
    ]
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


def create_remove_user_command(user, cluster):
    """Create the command to remove a user.

    @returns: list comprising the command
    """
    remove_user_command = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "delete",
        "user",
        "name={user}".format(user=user),
        "Cluster={cluster}".format(cluster=cluster)
    ]
    logging.debug(
        "Adding command to remove user %s from Cluster=%s",
        user,
        cluster,
        )

    return remove_user_command

def create_remove_account_command(account, cluster):
    """Create the command to remove an account.

    @returns: list comprising the command
    """
    remove_account_command = [
        SLURM_SACCT_MGR,
        "-i",
        "delete",
        "account",
        "Name={account}".format(account=account),
        "Cluster={cluster}".format(cluster=cluster),
    ]

    logging.debug(
        "Adding command to remove account %s from cluster %s",
        account,
        cluster,
    )

    return remove_account_command


def create_remove_user_account_command(user, account, cluster):
    """Create the command to remove a user.

    @returns: list comprising the command
    """
    remove_user_command = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "delete",
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

    return remove_user_command


def create_add_qos_command(name):
    """Create the command to add a QOS

    @returns: the list comprising the command
    """
    add_qos_command = [
        SLURM_SACCT_MGR,
        "-i",
        "add",
        "qos",
        "Name={0}".format(name)
    ]

    return add_qos_command

def create_remove_qos_command(name):
    """Create the command to remove a QOS.

    @param name: the name of the QOS to remove

    @returns: the list comprising the command
    """
    remove_qos_command = [
        SLURM_SACCT_MGR,
        "-i",
        "remove",
        "qos",
        "where",
        "Name={0}".format(name),
    ]

    return remove_qos_command


def create_modify_qos_command(name, settings):
    """Create the command to modify a QOS

    @param name: the name of the QOS to modify
    @param settings: dict with the items that should be set (key/value pairs)

    @returns: the list comprising the command
    """
    modify_qos_command = [
        SLURM_SACCT_MGR,
        "-i",
        "modify",
        "qos",
        name,
        "set",
        "flags=NoDecay,DenyOnLimit",
    ]

    for k, v in settings.items():
        modify_qos_command.append("{0}={1}".format(k, v))

    return modify_qos_command

