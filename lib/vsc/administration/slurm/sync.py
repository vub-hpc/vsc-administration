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

from collections import defaultdict

from vsc.config.base import GENT, INSTITUTE_VOS_BY_INSTITUTE, INSTITUTE_FAIRSHARE
from vsc.utils.run import RunNoShell
from vsc.administration.slurm.sacctmgr import (
    create_add_account_command, create_remove_account_command,
    create_change_account_fairshare_command,
    create_add_user_command, create_change_user_command, create_remove_user_command, create_remove_user_account_command,
    create_add_qos_command, create_remove_qos_command, create_modify_qos_command
    )
from vsc.administration.slurm.scancel import (
    create_remove_user_jobs_command, create_remove_jobs_for_account_command,
    )


class SlurmSyncException(Exception):
    pass


class SCommandException(Exception):
    pass


def execute_commands(commands):
    """Run the specified commands"""

    for command in commands:
        logging.info("Running command: %s", command)

        # if one fails, we simply fail the script and should get notified
        (ec, _) = RunNoShell.run(command)
        if ec != 0:
            raise SCommandException("Command failed: {0}".format(command))


TIER1_GPU_TO_CPU_HOURS_RATE = 12 # 12 cpus per gpu


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


def slurm_project_qos(projects, slurm_qos_info, clusters, protected_qos, qos_cleanup=False):
    """Check for new/changed projects and set their QOS accordingly"""
    commands = []
    for cluster in clusters:
        cluster_qos_names = set(get_cluster_qos(slurm_qos_info, cluster)) - set(protected_qos)
        project_qos_names = set([
            "{cluster}-{project_name}".format(cluster=cluster, project_name=p.name) for p in projects
        ])

        for project in projects:
            qos_name = "{0}-{1}".format(cluster, project.name)
            if qos_name not in cluster_qos_names:
                commands.append(create_add_qos_command(qos_name))
            commands.append(create_modify_qos_command(qos_name, {
                "GRPTRESMins": "billing={cpuminutes},cpu={cpuminutes},gres/gpu={gpuminutes}".format(
                    cpuminutes=60*int(project.cpu_hours)
                        + TIER1_GPU_TO_CPU_HOURS_RATE * 60 * int(project.gpu_hours),
                    gpuminutes=max(1, 60*int(project.gpu_hours)))
                }))

            # TODO: if we pass a cutoff date, we need to alter the hours if less was spent

        # We should actually keep the QOS, so we keep the usage on the slurm system
        # in case a project returns from the dead by receiving an extension past the
        # former end date
        if qos_cleanup:
            for qos_name in cluster_qos_names - project_qos_names:
                commands.append(create_remove_qos_command(qos_name))

    return commands


def slurm_modify_qos():
    pass


def slurm_project_accounts(resource_app_projects, slurm_account_info, clusters, protected_accounts, general_qos):
    """Check for new/changed projects and create their accounts accordingly

    We assume that the QOS has already been created.

    The account gets access to each QOS in the general_qos list
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
                    qos=",".join(["{0}-{1}".format(cluster, project_name)] + general_qos),
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


def slurm_project_users_accounts(
    project_members,
    active_accounts,
    slurm_user_info,
    clusters,
    protected_accounts,
    default_account):
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

        protected_users = [u for (u, a) in cluster_users_acct if a in protected_accounts]

        new_users = set()
        remove_project_users = set()
        all_project_users = set()

        for (members, project_name) in project_members:

            # these are the current Slurm users for this project
            slurm_project_users = set([user for (user, acct) in cluster_users_acct if acct == project_name])
            all_project_users |= slurm_project_users

            # these users are not yet in the Slurm DBD for this project
            new_users |= set([(user, project_name) for user in (members & active_accounts) - slurm_project_users])

            # these are the Slurm users that should no longer be associated with the project
            remove_project_users |= set([(user, project_name) for user in slurm_project_users - members])

        logging.info("%d new users", len(new_users))
        logging.info("%d removed project users", len(remove_project_users))

        # these are the users not in any project, we should decide if we want any of those
        remove_slurm_users = set([u[0] for u in cluster_users_acct if u not in protected_users]) - all_project_users

        if remove_slurm_users:
            logging.warning(
                "Number of slurm users not in projects: %d > 0: %s", len(remove_slurm_users), remove_slurm_users
            )

        # create associations in the default account for users that do not already have one
        cluster_users_with_default_account = set([u for (u, a) in cluster_users_acct if a == default_account])
        commands.extend([create_add_user_command(
            user=user,
            account=default_account,
            default_account=default_account,
            cluster=cluster) for (user, _) in new_users if user not in cluster_users_with_default_account
        ])

        # create associations for the actual project's new users
        commands.extend([create_add_user_command(
            user=user,
            account=project_name,
            cluster=cluster) for (user, project_name) in new_users
        ])

        # kick out users no longer in the project
        commands.extend([
            create_remove_user_account_command(user=user, account=project_name, cluster=cluster)
            for (user, project_name) in remove_project_users
        ])

        # remove associations in the default account for users no longer in any project
        commands.extend([
            create_remove_user_account_command(user=user, account=default_account, cluster=cluster)
            for user in cluster_users_with_default_account - all_project_users if user not in protected_users
        ])

    return commands


def slurm_user_accounts(vo_members, active_accounts, slurm_user_info, clusters, dry_run=False):
    """Check for the presence of the user in his/her account.

    @returns: list of sacctmgr commands to add the users if needed.
    """
    commands = []
    job_cancel_commands = defaultdict(list)
    association_remove_commands = []

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

        for user in remove_users:
            job_cancel_commands[user].append(create_remove_user_jobs_command(user=user, cluster=cluster))

        # Remove users from the clusters (in all accounts)
        association_remove_commands.extend([
            create_remove_user_command(user=user, cluster=cluster) for user in remove_users
        ])

        for (user, current_vo_id, (new_vo_id, _)) in moved_users:
            [add, default_account, remove_jobs, remove_association_user] = create_change_user_command(
                user=user,
                current_vo_id=current_vo_id,
                new_vo_id=new_vo_id,
                cluster=cluster
            )
            commands.extend([add, default_account])
            association_remove_commands.append(remove_association_user)
            job_cancel_commands[user].append(remove_jobs)

    return (job_cancel_commands, commands, association_remove_commands)
