# -*- coding: latin-1 -*-
#
# Copyright 2012-2022 Ghent University
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
This file contains common utilities for dealing with both users and VOs.

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
@author: Alex Domingo (Vrije Universiteit Brussel)
"""

import logging
import os

from vsc.config.base import VscStorage, GENT, VSC_DATA, VSC_DATA_SHARED, VSC_HOME
from vsc.filesystem.operator import StorageOperator

MOUNT_POINT_LOGIN = 'login'
MOUNT_POINT_DEFAULT = 'backend'

class VscTier2Accountpage(object):
    """Common methods to handle settings from the account page"""

    def __init__(self, storage=None, host_institute=GENT):
        """Initialise"""

        if storage is None:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.host_institute = host_institute
        self.institute_storage = self.storage[self.host_institute]

        # Initialze the corresponding operator for each storage backend
        for fs in self.storage[self.host_institute]:
            self.storage[self.host_institute][fs].operator = StorageOperator(self.storage[self.host_institute][fs])

    def _create_fileset(self, storage, path, fileset_name, parent_fileset=None, mod='755'):
        """Create a fileset in the storage backend"""

        try:
            filesystem_name = storage.filesystem
        except AttributeError:
            errmsg = "Failed to access attribute 'filesystem' in the storage configuration of fileset %s"
            logging.exception(errmsg, fileset_name)
            raise

        try:
            storage.operator().list_filesets()
        except AttributeError:
            logging.exception("Storage backend %s does not support listing filesets", storage.backend)
            raise

        logging.info("Trying to create fileset %s with link path %s", fileset_name, path)

        if not storage.operator().get_fileset_info(filesystem_name, fileset_name):
            logging.info("Creating new fileset on %s with name %s and path %s", filesystem_name, fileset_name, path)
            base_dir_hierarchy = os.path.dirname(path)
            storage.operator().make_dir(base_dir_hierarchy)
            # HACK to support versions older than 3.5 in our setup
            if parent_fileset is None:
                storage.operator().make_fileset(path, fileset_name)
            else:
                storage.operator().make_fileset(path, fileset_name, parent_fileset)
        else:
            logging.info("Fileset %s already exists ... not creating again.", fileset_name)

        mod_oct = int(mod, 8)
        storage.operator().chmod(mod_oct, path)

    def _get_storage(self, storage_name):
        """Seek and return storage settings from institute's storage"""
        try:
            storage = self.institute_storage[storage_name]
        except KeyError:
            err_msg = "Failed to access storage '%s' in the storage configuration of %s"
            logging.exception(err_msg, storage_name, self.host_institute)
            raise
        else:
            return storage

    def _get_mount_path(self, storage_name, mount_point):
        """Get the mount point for the location we're running"""
        if mount_point == MOUNT_POINT_DEFAULT:
            mount_path = self.institute_storage[storage_name].backend_mount_point
        elif mount_point == MOUNT_POINT_LOGIN:
            mount_path = self.institute_storage[storage_name].login_mount_point
        else:
            errmsg = "mount point type '%s' is not supported" % mount_point
            logging.error(errmsg)
            raise Exception(errmsg)

        return mount_path

    def _get_path(self, storage_name, mount_point=MOUNT_POINT_DEFAULT):
        """PLACEHOLDER: Get the path for user or VO directory on the given storage_name."""
        pass

    def _get_grouping_path(self, storage_name, mount_point=MOUNT_POINT_DEFAULT):
        """PLACEHOLDER: Get the path and the fileset for the user group directory."""
        pass

    def _home_path(self, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the home dir."""
        return self._get_path(VSC_HOME, mount_point)

    def _data_path(self, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the data dir."""
        return self._get_path(VSC_DATA, mount_point)

    def _data_shared_path(self, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path the VO shared data fileset on the storage"""
        return self._get_path(VSC_DATA_SHARED, mount_point)

    def _scratch_path(self, storage_name, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the scratch dir"""
        return self._get_path(storage_name, mount_point)

    def _grouping_home_path(self, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_HOME, mount_point)

    def _grouping_data_path(self, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_DATA, mount_point)

    def _grouping_scratch_path(self, storage_name, mount_point=MOUNT_POINT_DEFAULT):
        """Return the path to the grouping fileset for the users on the given scratch filesystem."""
        return self._get_grouping_path(storage_name, mount_point)

