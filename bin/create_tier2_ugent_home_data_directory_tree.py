#!/usr/bin/env python
#
# Copyright 2012-2023 Ghent University
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
Sets up the basic structure on the UGent home and data storage

@author: Andy Georges
"""

import logging
import os
from vsc.utils.py2vs3 import configparser

from vsc.filesystem.gpfs import GpfsOperations
from vsc.config.base import VscStorage

QUOTA_CONF_FILE = '/etc/quota_check.conf'


def set_up_apps(gpfs, storage_settings, storage, filesystem_info, filesystem_name):
    """Set up the apps fileset."""
    logging.info("Setting up the apps fileset on storage %s", storage)
    fileset_name = storage_settings.path_templates[storage]['apps'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
    if fileset_name not in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0o755, fileset_path)
        logging.info("Fileset %s created and linked at %s", fileset_name, fileset_path)


def set_up_filesystem(gpfs, storage_settings, storage, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic gent fileset
    logging.info("Setting up for storage %s", storage)
    fileset_name = storage_settings.path_templates[storage]['user'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
    if fileset_name not in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0o755, fileset_path)
        logging.info("Fileset %s created and linked at %s", fileset_name, fileset_path)

    if vo_support:
        # Create the basic vo fileset
        fileset_name = storage_settings.path_templates[storage]['vo'][0]
        vo_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
        if fileset_name not in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
            gpfs.make_fileset(vo_fileset_path, fileset_name)
            gpfs.chmod(0o755, vo_fileset_path)
            logging.info("Fileset %s created and linked at %s", fileset_name, vo_fileset_path)


def main():

    storage_settings = VscStorage()

    local_storage_conf = configparser.SafeConfigParser()
    local_storage_conf.read(QUOTA_CONF_FILE)

    gpfs = GpfsOperations()
    gpfs.list_filesystems()
    gpfs.list_filesets()

    for storage_name in local_storage_conf.get('MAIN', 'storage').split(','):

        filesystem_name = storage_settings[storage_name].filesystem
        filesystem_info = gpfs.get_filesystem_info(filesystem_name)

        if storage_name in ('VSC_HOME'):
            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name)
            set_up_apps(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name)
        else:
            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name, vo_support=True)

if __name__ == '__main__':
    main()
