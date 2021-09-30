#
# Copyright 2013-2021 Ghent University
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
Functions to deploy users to slurm.
"""
import logging

from enum import Enum

from vsc.accountpage.wrappers import mkNamedTupleInstance

from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN, INSTITUTE_VOS_BY_INSTITUTE, INSTITUTE_FAIRSHARE
from vsc.utils.missing import namedtuple_with_defaults
from vsc.utils.run import asyncloop


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


IGNORE_USERS = ["root"]
IGNORE_ACCOUNTS = ["root"]

SacctUserFields = ["User", "Def_Acct", "Admin", "Cluster", "Account", "Partition", "Share",
                   "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                   "QOS", "Def_QOS"]
SacctAccountFields = ["Account", "Descr", "Org", "Cluster", "ParentName", "User", "Share",
                      "GrpJobs", "GrpNodes", "GrpCPUs", "GrpMem", "GrpSubmit", "GrpWall", "GrpCPUMins",
                      "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                      "QOS", "Def_QOS"]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)


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


def parse_slurm_acct_line(header, line, info_type, user_field_number):
    """Parse the line into the correct data type."""
    fields = line.split("|")

    if info_type == SyncTypes.accounts:
        if fields[user_field_number]:
            # association information for a user. Users are processed later.
            return None
        creator = mkSlurmAccount
    elif info_type == SyncTypes.users:
        creator = mkSlurmUser
    else:
        return None

    return creator(dict(zip(header, fields)))


def parse_slurm_acct_dump(lines, info_type):
    """Parse the accounts from the listing."""
    acct_info = set()

    header = [w.replace(' ', '_') for w in lines[0].rstrip().split("|")]
    user_field_number = [h.lower() for h in header].index("user")

    for line in lines[1:]:
        line = line.rstrip()
        try:
            info = parse_slurm_acct_line(header, line, info_type, user_field_number)
            # This fails when we get e.g., the users and look at the account lines.
            # We should them just skip that line instead of raising an exception
            if info:
                acct_info.add(info)
        except Exception as err:
            logging.exception("Slurm acct sync: could not process line %s [%s]", line, err)
            raise

    return acct_info


def get_slurm_acct_info(info_type):
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
    info = parse_slurm_acct_dump(contents.splitlines(), info_type)

    return info


def create_add_account_command(account, parent, organisation, cluster, fairshare):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    CREATE_ACCOUNT_COMMAND = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "add",
        "account",
        account,
        "Parent={0}".format(parent or "root"),
        "Organization={0}".format(SLURM_ORGANISATIONS[organisation]),
        "Cluster={0}".format(cluster),
        "Fairshare={0}".format(fairshare),
    ]
    logging.debug(
        "Adding command to add account %s with Parent=%s Cluster=%s Organization=%s",
        account,
        parent,
        cluster,
        organisation,
        )

    return CREATE_ACCOUNT_COMMAND

def create_change_account_fairshare_command(account, cluster, fairshare):
    CHANGE_ACCOUNT_FAIRSHARE_COMMAND = [
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

    return CHANGE_ACCOUNT_FAIRSHARE_COMMAND

def create_add_user_command(user, vo_id, cluster):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: list comprising the command
    """
    CREATE_USER_COMMAND = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "add",
        "user",
        user,
        "Account={0}".format(vo_id),
        "DefaultAccount={0}".format(vo_id),
        "Cluster={0}".format(cluster)
    ]
    logging.debug(
        "Adding command to add user %s with Account=%s Cluster=%s",
        user,
        vo_id,
        cluster,
        )

    return CREATE_USER_COMMAND


def create_change_user_command(user, current_vo_id, new_vo_id, cluster):
    """Creates the commands to change a user's account.

    @returns: two lists comprising the commands
    """
    add_user_command = create_add_user_command(user, new_vo_id, cluster)
    REMOVE_ASSOCIATION_USER_COMMAND = [
        SLURM_SACCT_MGR,
        "-i",   # commit immediately
        "delete",
        "user",
        "name={0}".format(user),
        "Account={0}".format(current_vo_id),
        "where",
        "Cluster={0}".format(cluster),
    ]
    logging.debug(
        "Adding commands to change user %s on Cluster=%s from Account=%s to DefaultAccount=%s",
        user,
        cluster,
        current_vo_id,
        new_vo_id
        )

    return [add_user_command, REMOVE_ASSOCIATION_USER_COMMAND]


def create_remove_user_command(user, cluster):
    """Create the command to remove a user.

    @returns: list comprising the command
    """
    REMOVE_USER_COMMAND = [
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

    return REMOVE_USER_COMMAND


def slurm_institute_accounts(slurm_account_info, clusters, host_institute, institute_vos):
    """Check for the presence of the institutes and their default VOs in the slurm account list.

    @returns: list of sacctmgr commands to add the accounts to the clusters if needed
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = [acct.Account for acct in slurm_account_info if acct and acct.Cluster == cluster]
        for (inst, vo) in sorted(list(INSTITUTE_VOS_BY_INSTITUTE[host_institute].items())):

            if inst not in cluster_accounts:
                commands.append(
                    create_add_account_command(
                        account=inst,
                        parent=None,
                        cluster=cluster,
                        organisation=inst,
                        fairshare=INSTITUTE_FAIRSHARE[host_institute][inst]
                    )
                )
            if vo not in cluster_accounts:
                commands.append(
                    create_add_account_command(
                        account=vo,
                        parent=inst,
                        cluster=cluster,
                        organisation=inst,
                        fairshare=institute_vos[vo].fairshare # needs to come from the AP
                    )
                )

    return commands


def slurm_vo_accounts(account_page_vos, slurm_account_info, clusters, host_institute):
    """Check for the presence of the new/changed VOs in the slurm account list.

    @returns: list of sacctmgr commands to add the accounts for VOs if needed
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = dict([
            (acct.Account, int(acct.Share))
            for acct in slurm_account_info
            if acct and acct.Cluster == cluster
        ])

        for vo in account_page_vos:

            # skip the "default" VOs for our own institute
            if vo.vsc_id in INSTITUTE_VOS_BY_INSTITUTE[host_institute].values():
                continue

            # create a new account for a VO that does not already have an account
            if vo.vsc_id not in cluster_accounts:
                commands.append(create_add_account_command(
                    account=vo.vsc_id,
                    parent=vo.institute['name'],
                    cluster=cluster,
                    organisation=vo.institute['name'],
                    fairshare=vo.fairshare,
                ))

            # create update commands for VOs with a changed fairshare
            elif int(vo.fairshare) != cluster_accounts[vo.vsc_id]:
                commands.append(create_change_account_fairshare_command(
                    account=vo.vsc_id,
                    cluster=cluster,
                    fairshare=vo.fairshare,
                ))

            # TODO: create removal commands when VOs go inactive

    return commands


def slurm_user_accounts(vo_members, active_accounts, slurm_user_info, clusters, dry_run=False):
    """Check for the presence of the user in his/her account.

    @returns: list of sacctmgr commands to add the users if needed.
    """
    commands = []

    active_vo_members = set()
    reverse_vo_mapping = dict()
    for (members, vo) in vo_members.values():
        # basic set arithmetic: take the intersection of the RHS sets and make the union with the LHS set
        active_vo_members |= members & active_accounts

        for m in members:
            reverse_vo_mapping[m] = (vo.vsc_id, vo.institute['name'])

    for cluster in clusters:
        cluster_users_acct = [
            (user.User, user.Def_Acct) for user in slurm_user_info if user and user.Cluster == cluster
        ]
        cluster_users = set([u[0] for u in cluster_users_acct])

        # these are the users that need to be removed as they are no longer an active user in any
        # (including the institute default) VO
        remove_users = cluster_users - active_vo_members

        new_users = set()
        changed_users = set()
        moved_users = set()

        for (vo_id, (members, vo)) in vo_members.items():

            # these are users not yet in the Slurm DB for this cluster
            new_users |= set([
                (user, vo.vsc_id, vo.institute['name'])
                for user in (members & active_accounts) - cluster_users
            ])

            # these are the current Slurm users per Account, i.e., the VO currently being processed
            slurm_acct_users = set([user for (user, acct) in cluster_users_acct if acct == vo_id])

            # these are the users that should no longer be in this account, but should not be removed
            # we need to look up their new VO
            # Again, basic set arithmetic. LHS is the intersection of the people we have left and the active users
            changed_users_vo = (slurm_acct_users - members) & active_accounts
            changed_users |= changed_users_vo

            try:
                moved_users |= set([(user, vo_id, reverse_vo_mapping[user]) for user in changed_users_vo])
            except KeyError as err:
                logging.warning("Found user not belonging to any VO in the reverse VO map: %s", err)
                if dry_run:
                    for user in changed_users:
                        try:
                            moved_users.add((user, reverse_vo_mapping[user]))
                        except KeyError:
                            logging.warning("Dry run, cannot find up user %s in reverse VO map",
                                            user)

        logging.debug("%d new users", len(new_users))
        logging.debug("%d removed users", len(remove_users))
        logging.debug("%d changed users", len(moved_users))

        commands.extend([create_add_user_command(
            user=user,
            vo_id=vo_id,
            cluster=cluster) for (user, vo_id, _) in new_users
        ])
        commands.extend([create_remove_user_command(user=user, cluster=cluster) for user in remove_users])

        def flatten(ls):
            """Turns a list of lists (ls) into a list, a.k.a. flatten a list."""
            return [item for l in ls for item in l]

        commands.extend(flatten([create_change_user_command(
            user=user,
            current_vo_id=current_vo_id,
            new_vo_id=new_vo_id,
            cluster=cluster) for (user, current_vo_id, (new_vo_id, _)) in moved_users])
        )

    return commands
