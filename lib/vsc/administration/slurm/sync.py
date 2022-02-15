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
Functions to deploy users to slurm.
"""
import logging

from enum import Enum

from vsc.accountpage.wrappers import mkNamedTupleInstance

from vsc.config.base import ANTWERPEN, BRUSSEL, GENT, LEUVEN, INSTITUTE_VOS_BY_INSTITUTE, INSTITUTE_FAIRSHARE
from vsc.utils.missing import namedtuple_with_defaults
from vsc.utils.run import asyncloop


SLURM_SACCT_MGR = "/usr/bin/sacctmgr"
SLURM_SCANCEL = "/usr/bin/scancel"

SLURM_ORGANISATIONS = {
    ANTWERPEN: 'uantwerpen',
    BRUSSEL: 'vub',
    GENT: 'ugent',
    LEUVEN: 'kuleuven',
}


class SacctMgrException(Exception):
    pass

class SlurmSyncException(Exception):
    pass

class SyncTypes(Enum):
    accounts = "accounts"
    users = "users"
    qos = "qos"


IGNORE_USERS = ["root"]
IGNORE_ACCOUNTS = ["root"]
IGNORE_QOS = ["normal"]

TIER1_GPU_TO_CPU_HOURS_RATE = 12 # 12 cpus per gpu
TIER1_SLURM_DEFAULT_PROJECT_ACCOUNT = "gt1_default"

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
SacctMgrQosFields = [
    "Name", "Priority", "GraceTime", "Preempt", "PreemptExemptTime", "PreemptMode",
    "Flags", "UsageThres", "UsageFactor", "GrpTRES", "GrpTRESMins", "GrpTRESRunMins",
    "GrpJobs", "GrpSubmit", "GrpWall", "MaxTRES", "MaxTRESPerNode", "MaxTRESMins",
    "MaxWall", "MaxTRESPU", "MaxJobsPU", "MaxSubmitPU", "MaxTRESPA", "MaxJobsPA",
    "MaxSubmitPA", "MinTRES"
]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)
SlurmQos = namedtuple_with_defaults('SlurmQos', SacctMgrQosFields)


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


def create_remove_user_jobs_command(user, cluster, state=None, account=None):
    """Create the command to remove a user's jobs in the given state.

    @returns: a list comprising the command
    """
    remove_user_jobs_command = [
        SLURM_SCANCEL,
        "--cluster={cluster}".format(cluster=cluster),
        "--user={user}".format(user=user),
    ]

    if state is not None:
        remove_user_jobs_command.append("--state={state}".format(state=state))

    if account is not None:
        remove_user_jobs_command.append("--account={account}".format(account=account))

    return remove_user_jobs_command


def create_remove_jobs_for_account_command(account, cluster):
    """Create the command to remove queued/suspended jobs from users that are
    in the account that needs to be removed.

    @returns: list comprising the account
    """
    remove_jobs_command_pending = [
        SLURM_SCANCEL,
        "--cluster={cluster}".format(cluster=cluster),
        "--account={account}".format(account=account),
        "--state=PENDING",
    ]
    remove_jobs_command_suspended = [
        SLURM_SCANCEL,
        "--cluster={cluster}".format(cluster=cluster),
        "--account={account}".format(account=account),
        "--state=SUSPENDED",
    ]

    return [remove_jobs_command_pending, remove_jobs_command_suspended]


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


def get_cluster_accounts(slurm_account_info, cluster):
    return dict([
            (acct.Account, int(acct.Share))
            for acct in slurm_account_info
            if acct and acct.Cluster == cluster
        ])


def get_cluster_qos(slurm_qos_info, cluster):
    """Returns a list of QOS names related to the given cluster"""

    return [qi.Name for qi in slurm_qos_info if qi.Name.startswith(cluster)]


def slurm_project_qos(projects, slurm_qos_info, clusters):
    """Check for new/changed projects and set their QOS accordingly"""
    commands = []
    for cluster in clusters:
        cluster_qos_names = set(get_cluster_qos(slurm_qos_info, cluster))
        project_qos_names = set([
            "{cluster}-{project_name}".format(cluster=cluster, project_name=p.name) for p in projects
        ])

        for project in projects:
            qos_name = "{0}-{1}".format(cluster, project.name)
            if qos_name not in cluster_qos_names:
                commands.append(create_add_qos_command(qos_name))
            commands.append(create_modify_qos_command(qos_name, {
                "GRPTRESMins": "cpu={cpuminutes},gres/gpu={gpuminutes}".format(
                    cpuminutes=60*int(project.cpu_hours)
                        + TIER1_GPU_TO_CPU_HOURS_RATE * 60 * int(project.gpu_hours),
                    gpuminutes=max(1, 60*int(project.gpu_hours)))
                }))

            # TODO: if we pass a cutoff date, we need to alter the hours if less was spent

        for qos_name in cluster_qos_names - project_qos_names:
            commands.append(create_remove_qos_command(qos_name))

    return commands


def slurm_modify_qos():
    pass


def slurm_project_accounts(resource_app_projects, slurm_account_info, clusters, protected_accounts):
    """Check for new/changed projects and create their accounts accordingly

    We assume that the QOS has already been created
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = set(get_cluster_accounts(slurm_account_info, cluster).keys())

        resource_app_project_names = set([p.name for p in resource_app_projects])

        for project_name in resource_app_project_names - cluster_accounts:
            if project_name not in cluster_accounts:
                commands.append(create_add_account_command(
                    account=project_name,
                    parent="projects",  # in case we want to deploy on Tier-2 as well
                    cluster=cluster,
                    organisation=GENT,   # tier-1 projects run here :p
                    qos="{0}-{1}".format(cluster, project_name),  # QOS is not attached to a cluster
                ))

        for project_name in cluster_accounts - resource_app_project_names:
            if project_name not in protected_accounts:
                commands.extend(create_remove_jobs_for_account_command(
                    account=project_name,
                    cluster=cluster))
                commands.append(create_remove_account_command(
                    account=project_name,
                    cluster=cluster))

    return commands


def slurm_vo_accounts(account_page_vos, slurm_account_info, clusters, host_institute):
    """Check for the presence of the new/changed VOs in the slurm account list.

    @returns: list of sacctmgr commands to add the accounts for VOs if needed
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = get_cluster_accounts(slurm_account_info, cluster)

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


def slurm_project_users_accounts(project_members, active_accounts, slurm_user_info, clusters):
    """Check if the users are in the project account.

    For users in the project:

    - If the user does not exist on the system:
        - a new association in the default account is created. The associated QoS does not allow jobs.
        - a new association in the project account is created.

    - For users who left the project:
        - The user's association in the project is removed.
        - If this was the last project for this user, we also remove the association in the default account.  (TODO)
    """

    commands = []

    for cluster in clusters:
        cluster_users_acct = [
            (user.User, user.Account) for user in slurm_user_info if user and user.Cluster == cluster
        ]

        new_users = set()
        remove_project_users = set()
        all_project_users = set()

        for (members, project_name) in project_members:

            # these are the current Slurm users for this project
            slurm_project_users = set([user for (user, acct) in cluster_users_acct if acct == project_name])
            all_project_users |= slurm_project_users

            # these users are not yet in the Slurm DBD for any project
            new_users |= set([(user, project_name) for user in (members & active_accounts) - slurm_project_users])

            # these are the Slurm users that should no longer be associated with the project
            remove_project_users |= set([(user, project_name) for user in slurm_project_users - members])

        logging.info("%d new users", len(new_users))
        logging.info("%d removed project users", len(remove_project_users))

        # these are the users not in any project, we should decide if we want any of those
        remove_slurm_users = set([u[0] for u in cluster_users_acct]) - all_project_users

        if remove_slurm_users:
            logging.warning(
                "Number of slurm users not in projecs: %d > 0: %s", len(remove_slurm_users), remove_slurm_users
            )

        commands.extend([create_add_user_command(
            user=user,
            account=project_name,
            cluster=cluster) for (user, project_name) in new_users
        ])
        commands.extend([
            create_remove_user_account_command(user=user, account=project_name, cluster=cluster)
            for (user, project_name) in remove_project_users
        ])

    return commands


def slurm_user_accounts(vo_members, active_accounts, slurm_user_info, clusters, dry_run=False):
    """Check for the presence of the user in his/her account.

    @returns: list of sacctmgr commands to add the users if needed.
    """
    commands = []
    job_cancel_commands = []

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
                        except KeyError as err:
                            logging.warning("Dry run, cannot find up user %s in reverse VO map: %s",
                                            user, err)


        logging.debug("%d new users", len(new_users))
        logging.debug("%d removed users", len(remove_users))
        logging.debug("%d changed users", len(moved_users))

        commands.extend([create_add_user_command(
            user=user,
            account=vo_id,
            cluster=cluster,
            default_account=vo_id) for (user, vo_id, _) in new_users
        ])
        job_cancel_commands.extend([
            create_remove_user_jobs_command(user=user, cluster=cluster) for user in remove_users
        ])
        commands.extend([create_remove_user_command(user=user, cluster=cluster) for user in remove_users])

        for (user, current_vo_id, (new_vo_id, _)) in moved_users:
            [add, default_account, remove_jobs, remove_association_user] = create_change_user_command(
                user=user,
                current_vo_id=current_vo_id,
                new_vo_id=new_vo_id,
                cluster=cluster
            )
            commands.extend([add, default_account, remove_association_user])
            job_cancel_commands.append(remove_jobs)

    return [job_cancel_commands, commands]
