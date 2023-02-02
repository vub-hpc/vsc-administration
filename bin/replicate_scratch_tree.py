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
This script replicates the users and VO's onto the scratch storage in
a temporary tree.

@author: Andy Georges
"""

import grp
import logging
import os
import pwd
import sys

from vsc.config.base import VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption


NAGIOS_HEADER = "replicate_scratch_tree"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes


SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)


def set_up_filesystem(
        gpfs,
        storage_settings,
        storage,
        filesystem_info,
        filesystem_name,
        vo_support=False,
        dry_run=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic gent fileset
    logging.info("Replicating up for storage %s", storage)
    fileset_name = storage_settings.path_templates[storage]['replica'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)

    # if the replicat fileset does not exist, we create it, like a BAWS
    if fileset_name not in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        if not dry_run:
            gpfs.make_fileset(fileset_path, fileset_name)
            gpfs.chmod(fileset_path, 0o755)
        logging.info("Fileset %s created and linked at %s", fileset_name, fileset_path)

    # create directories up to vsc42000
    for group in range(0, 21):

        group_path = os.path.join(fileset_path, "vsc4%02d" % group)
        if not os.path.exists(group_path):
            logging.info("Path %s does not exist. Creating directory.", group_path)
            try:
                if not dry_run:
                    os.mkdir(group_path)
                    os.chmod(group_path, 0o755)
            except (IOError, OSError) as err:
                logging.error("Problem creating dir %s [%s]", group_path, err)

        for user in range(0, 100):
            user_name = "vsc4%02d%02d" % (group, user)
            user_id = 2540000 + group * 100 + user
            user_path = os.path.join(group_path, user_name)
            if not os.path.exists(user_path):
                logging.info("Path %s does not exist. Creating directory.", user_path)
                try:
                    if not dry_run:
                        os.mkdir(user_path)
                        os.chown(user_path, user_id, user_id)
                        os.chmod(user_path, 0o700)
                except (IOError, OSError) as err:
                    logging.error("Problem creating dir %s: %s", user_path, err)

    if vo_support:

        vo_group_path = os.path.join(fileset_path, "gvo000")

        if not os.path.exists(vo_group_path):
            os.mkdir(vo_group_path)
            os.chmod(vo_group_path, 0o755)

        for vo in range(1, 100):

            vo_name = "gvo%05d" % (vo,)
            try:
                vo_group = grp.getgrnam(vo_name)
            except Exception:
                logging.warning("Cannot find a group for VO %s", vo_name)
                continue

            vo_path = os.path.join(fileset_path, vo_name[:-2], vo_name)
            vo_members = vo_group.gr_mem

            vo_moderator = None
            for member_name in vo_members:
                try:
                    vo_moderator = pwd.getpwnam(member_name)
                    logging.info("VO moderator is picked as %s", vo_moderator.pw_name)
                    break
                except KeyError as err:
                    continue

            if not vo_moderator:
                logging.error("Cannot find a moderator for VO %s", vo_name)
                vo_moderator = pwd.getpwnam('nobody')

            if not os.path.exists(vo_path):
                logging.info("Path %s does not exist. Creating directory.", vo_path)
                try:
                    os.mkdir(vo_path)
                    os.chown(vo_path, vo_moderator.pw_uid, vo_group.gr_gid)
                    os.chmod(vo_path, 0o770)
                except (IOError, OSError) as err:
                    logging.error("Problem creating dir %s", vo_path)

            for member_name in vo_members:

                member_path = os.path.join(vo_path, member_name)
                try:
                    member = pwd.getpwnam(member_name)
                except KeyError as err:
                    continue

                if not os.path.exists(member_path):
                    logging.info("Path %s does not exist. Creating directory", member_path)
                    try:
                        os.mkdir(member_path)
                        os.chown(member_path, member.pw_uid, member.pw_gid)
                        os.chmod(member_path, 0o700)
                    except Exception:
                        logging.error("Cannot create dir %s", member_path)


def main():
    """
    Main script.
    - process the users and VOs
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        storage_settings = VscStorage()
        gpfs = GpfsOperations()
        gpfs.list_filesystems()
        gpfs.list_filesets()

        for storage_name in opts.options.storage:

            filesystem_name = storage_settings[storage_name].filesystem
            filesystem_info = gpfs.get_filesystem_info(filesystem_name)

            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name, vo_support=True,
                              dry_run=opts.options.dry_run)

    except Exception as err:
        logging.exception("critical exception caught: %s", err)
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("UGent users and VOs synchronised", stats)


if __name__ == '__main__':
    main()
