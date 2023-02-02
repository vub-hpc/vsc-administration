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
scancel commands
"""
SLURM_SCANCEL = "/usr/bin/scancel"


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

