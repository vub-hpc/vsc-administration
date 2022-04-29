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
This file contains the utilities for dealing with VOs on the VSC.
Original Perl code by Stijn De Weirdt

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import copy
import logging
import os
import pwd

from vsc.utils.py2vs3 import HTTPError

from vsc.accountpage.wrappers import mkVo, mkVscVoSizeQuota, mkVscAccount, mkVscAutogroup
from vsc.administration.user import VscTier2AccountpageUser, UserStatusUpdateError
from vsc.config.base import (
    VSC, VscStorage, VSC_HOME, VSC_DATA, VSC_DATA_SHARED, NEW, MODIFIED, MODIFY, ACTIVE,
    GENT, DATA_KEY, SCRATCH_KEY, DEFAULT_VOS_ALL, VSC_PRODUCTION_SCRATCH, INSTITUTE_VOS_BY_INSTITUTE,
    VO_SHARED_PREFIX_BY_INSTITUTE, VO_PREFIX_BY_INSTITUTE, STORAGE_SHARED_SUFFIX
)
from vsc.filesystem.gpfs import GpfsOperations, GpfsOperationError, PosixOperations, PosixOperationError
from vsc.filesystem.oceanstor import OceanStorOperations, OceanStorOperationError
from vsc.utils.missing import Monoid, MonoidDict


class VoStatusUpdateError(Exception):
    pass


def whenHTTPErrorRaise(f, msg, **kwargs):
    try:
        return f(**kwargs)
    except HTTPError as err:
        logging.error("%s: %s", msg, err)
        raise


class VscAccountPageVo(object):
    """
    A Vo that gets its own information from the accountpage through the REST API.
    """
    def __init__(self, vo_id, rest_client):
        """
        Initialise.
        """
        self.vo_id = vo_id
        self.rest_client = rest_client
        self._vo_cache = None

    @property
    def vo(self):
        if not self._vo_cache:
            self._vo_cache = mkVo(whenHTTPErrorRaise(self.rest_client.vo[self.vo_id].get,
                                                     "Could not get VO from accountpage for VO %s" % self.vo_id)[1])
        return self._vo_cache


class VscTier2AccountpageVo(VscAccountPageVo):
    """Class representing a VO in the VSC.

    A VO is a special kind of group, identified mainly by its name.
    """

    def __init__(self, vo_id, storage=None, rest_client=None, host_institute=GENT):
        """Initialise"""
        super(VscTier2AccountpageVo, self).__init__(vo_id, rest_client)

        self.vo_id = vo_id
        self.vsc = VSC()
        self.host_institute = host_institute

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        # initialize storage backend for each filesystem
        for fs_name, filesystem in self.storage[host_institute].items():
            if filesystem.backend == 'gpfs':
                filesystem.backend_operator = (GpfsOperations(), GpfsOperationError)
            elif filesystem.backend == 'oceanstor':
                oceanstor_api = (filesystem.api_url, filesystem.api_account, filesystem.api_user, filesystem.api_pwd)
                filesystem.backend_operator = (OceanStorOperations(*oceanstor_api), OceanStorOperationError)
            else:
                # fallback to POSIX
                filesystem.backend_operator = (PosixOperations(), PosixOperationError)

        self.dry_run = False

        self._vo_data_quota_cache = None
        self._vo_data_shared_quota_cache = None
        self._vo_scratch_quota_cache = None
        self._institute_quota_cache = None

        self._sharing_group_cache = None

    @property
    def _institute_quota(self):
        if not self._institute_quota_cache:
            all_quota = [mkVscVoSizeQuota(q) for q in
                         whenHTTPErrorRaise(self.rest_client.vo[self.vo.vsc_id].quota.get,
                                            "Could not get quotata from accountpage for VO %s" % self.vo.vsc_id)[1]]
            self._institute_quota_cache = [q for q in all_quota if q.storage['institute'] == self.host_institute]
        return self._institute_quota_cache

    def _get_institute_data_quota(self):
        return [q for q in self._institute_quota if q.storage['storage_type'] == DATA_KEY]

    def _get_institute_non_shared_data_quota(self):
        return [q.hard for q in self._get_institute_data_quota()
                if not q.storage['name'].endswith(STORAGE_SHARED_SUFFIX)]

    def _get_institute_shared_data_quota(self):
        return [q.hard for q in self._get_institute_data_quota()
                if q.storage['name'].endswith(STORAGE_SHARED_SUFFIX)]

    @property
    def vo_data_quota(self):
        if not self._vo_data_quota_cache:
            self._vo_data_quota_cache = self._get_institute_non_shared_data_quota()
            if not self._vo_data_quota_cache:
                self._vo_data_quota_cache = [self.storage[VSC_DATA].quota_vo]

        return self._vo_data_quota_cache[0]  # there can be only one

    @property
    def vo_data_shared_quota(self):
        if not self._vo_data_shared_quota_cache:
            try:
                self._vo_data_shared_quota_cache = self._get_institute_shared_data_quota()[0]
            except IndexError:
                return None
        return self._vo_data_shared_quota_cache

    @property
    def vo_scratch_quota(self):
        if not self._vo_scratch_quota_cache:
            self._vo_scratch_quota_cache = [q for q in self._institute_quota
                                            if q.storage['storage_type'] == SCRATCH_KEY]

        return self._vo_scratch_quota_cache

    @property
    def sharing_group(self):
        if not self.data_sharing:
            return None

        if not self._sharing_group_cache:
            group_name = self.vo.vsc_id.replace(VO_PREFIX_BY_INSTITUTE[self.vo.institute['name']],
                                                VO_SHARED_PREFIX_BY_INSTITUTE[self.vo.institute['name']])
            self._sharing_group_cache = mkVscAutogroup(
                whenHTTPErrorRaise(self.rest_client.autogroup[group_name].get,
                                   "Could not get autogroup %s details" % group_name)[1])

        return self._sharing_group_cache

    @property
    def data_sharing(self):
        return self.vo_data_shared_quota is not None

    def members(self):
        """Return a list with all the VO members in it."""
        return self.vo.members

    def _get_path(self, storage, mount_point="backend"):
        """Get the path for the (if any) user directory on the given storage."""

        (path, _) = self.storage.path_templates[self.host_institute][storage]['vo'](self.vo.vsc_id)
        if mount_point == "login":
            mount_path = self.storage[self.host_institute][storage].login_mount_point
        elif mount_point == "backend":
            mount_path = self.storage[self.host_institute][storage].backend_mount_point
        else:
            logging.error("mount_point (%s)is not login or backend", mount_point)
            raise Exception()

        return os.path.join(mount_path, path)

    def _data_path(self, mount_point="backend"):
        """Return the path to the VO data fileset on GPFS"""
        return self._get_path(VSC_DATA, mount_point)

    def _data_shared_path(self, mount_point="backend"):
        """Return the path the VO shared data fileset on GPFS"""
        return self._get_path(VSC_DATA_SHARED, mount_point)

    def _scratch_path(self, storage, mount_point="backend"):
        """Return the path to the VO scratch fileset on GPFS.

        @type storage: string
        @param storage: name of the storage we are looking at.
        """
        return self._get_path(storage, mount_point)

    def _create_fileset(self, storage, path, parent_fileset=None, fileset_name=None, group_owner_id=None):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets ownership to the first (active) VO moderator, or to nobody if there is no moderator
        - sets group ownership to the supplied value (group_owner_id) or if that is missing to the
          vsc_id of the VO owning the fileset

        The parent_fileset is used to support older (< 3.5.x) GPFS setups still present in our system
        """
        fs_backend, fs_backend_err = storage.backend_operator

        try:
            filesystem_name = storage.filesystem
        except AttributeError:
            logging.exception("Trying to access non-existent attribute 'filesystem' in the data storage instance")

        try:
            fs_backend.list_filesets()
        except AttributeError:
            logging.exception("Storage backend %s does not support listing filesets" % storage.backend)

        if not fileset_name:
            fileset_name = self.vo.vsc_id

        if group_owner_id:
            fileset_group_owner_id = group_owner_id
        else:
            fileset_group_owner_id = self.vo.vsc_id_number

        if not fs_backend.get_fileset_info(filesystem_name, fileset_name):
            logging.info("Creating new fileset on %s with name %s and path %s",
                         filesystem_name, fileset_name, path)
            base_dir_hierarchy = os.path.dirname(path)
            fs_backend.make_dir(base_dir_hierarchy)

            # HACK to support versions older than 3.5 in our setup
            if parent_fileset is None:
                fs_backend.make_fileset(path, fileset_name)
            else:
                fs_backend.make_fileset(path, fileset_name, parent_fileset)
        else:
            logging.info("Fileset %s already exists for VO %s ... not creating again.",
                         fileset_name, self.vo.vsc_id)

        fs_backend.chmod(0o770, path)

        try:
            moderator = mkVscAccount(self.rest_client.account[self.vo.moderators[0]].get()[1])
        except HTTPError:
            logging.exception("Cannot obtain moderator information from account page, setting ownership to nobody")
            fs_backend.chown(pwd.getpwnam('nobody').pw_uid, fileset_group_owner_id, path)
        except IndexError:
            logging.error("There is no moderator available for VO %s", self.vo.vsc_id)
            fs_backend.chown(pwd.getpwnam('nobody').pw_uid, fileset_group_owner_id, path)
        else:
            fs_backend.chown(moderator.vsc_id_number, fileset_group_owner_id, path)

    def create_data_fileset(self):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        path = self._data_path()
        try:
            storage = self.storage[self.host_institute][VSC_DATA]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the data storage dictionary"
            logging.exception(errmsg, VSC_DATA)
        else:
            self._create_fileset(storage, path)

    def create_data_shared_fileset(self):
        """Create a VO directory for sharing data on the HPC data filesystem. Always set the quota."""
        path = self._data_shared_path()
        try:
            storage = self.storage[self.host_institute][VSC_DATA_SHARED]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the shared data storage dictionary"
            logging.exception(errmsg, VSC_DATA_SHARED)
        else:
            self._create_fileset(
                storage,
                path,
                fileset_name=self.sharing_group.vsc_id,
                group_owner_id=self.sharing_group.vsc_id_number,
            )

    def create_scratch_fileset(self, storage_name):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        path = self._scratch_path(storage_name)
        try:
            storage = self.storage[self.host_institute][storage_name]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the scratch storage dictionary"
            logging.exception(errmsg, storage_name)

        if storage.backend == 'gpfs':
            if storage.version >= (3, 5, 0, 0):
                self._create_fileset(storage, path)
            else:
                self._create_fileset(storage, path, 'root')
        else:
            self._create_fileset(storage, path)

    def _create_vo_dir(self, path, storage_name=VSC_DATA):
        """Create a user owned directory on the GPFS."""
        try:
            storage = self.storage[self.host_institute][storage_name]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the storage dictionary"
            logging.exception(errmsg, storage_name)
        else:
            fs_backend, fs_backend_err = storage.backend_operator
            fs_backend.make_dir(path)

    def _set_quota(self, storage_name, path, quota, fileset_name=None):
        """Set FILESET quota on the FS for the VO fileset.
        @type quota: int
        @param quota: soft quota limit expressed in KiB
        """
        if not fileset_name:
            fileset_name = self.vo.vsc_id

        try:
            storage = self.storage[self.host_institute][storage_name]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the storage dictionary"
            logging.exception(errmsg, storage_name)
        else:
            fs_backend, fs_backend_err = storage.backend_operator

        # expressed in bytes, retrieved in KiB from the account backend
        hard = quota * 1024
        if storage.backend == 'gpfs':
            hard *= storage.data_replication_factor

        soft = int(hard * self.vsc.quota_soft_fraction)

        try:
            # LDAP information is expressed in KiB, GPFS wants bytes.
            fs_backend.set_fileset_quota(soft, path, fileset_name, hard)
            fs_backend.set_fileset_grace(path, self.vsc.vo_storage_grace_time)  # 7 days
        except fs_backend_err:
            logging.exception("Unable to set quota on path %s", path)
            raise

    def set_data_quota(self):
        """Set FILESET quota on the data FS for the VO fileset."""
        if self.vo_data_quota:
            self._set_quota(VSC_DATA, self._data_path(), int(self.vo_data_quota))
        else:
            self._set_quota(VSC_DATA, self._data_path(), 16 * 1024)

    def set_data_shared_quota(self):
        """Set FILESET quota on the data FS for the VO fileset."""
        if self.vo_data_shared_quota:
            self._set_quota(
                VSC_DATA_SHARED,
                self._data_shared_path(),
                int(self.vo_data_shared_quota),
                fileset_name=self.vo.vsc_id.replace(
                    VO_PREFIX_BY_INSTITUTE[self.vo.institute["name"]],
                    VO_SHARED_PREFIX_BY_INSTITUTE[self.vo.institute["name"]],
                ),
            )

    def set_scratch_quota(self, storage_name):
        """Set FILESET quota on the scratch FS for the VO fileset."""
        quota = [q for q in self.vo_scratch_quota if q.storage['name'] in (storage_name,)]

        if not quota:
            logging.error("No VO %s scratch quota information available for %s", self.vo.vsc_id, storage_name)
            logging.info("Setting default VO %s scratch quota on storage %s to %d",
                         self.vo.vsc_id, storage_name, self.storage[storage_name].quota_vo)
            self._set_quota(storage_name, self._scratch_path(storage_name), self.storage[storage_name].quota_vo)
            return
        elif len(quota) > 1:
            logging.exception("Cannot set scratch quota for %s with multiple quota instances %s",
                              storage_name, quota)
            raise

        logging.info("Setting VO %s quota on storage %s to %d", self.vo.vsc_id, storage_name, quota[0].hard)
        self._set_quota(storage_name, self._scratch_path(storage_name), quota[0].hard)

    def _set_member_quota(self, storage_name, path, member, quota):
        """Set USER quota on the FS for the VO fileset

        @type member: VscTier2AccountpageUser
        @type quota: integer (hard value)
        """
        try:
            storage = self.storage[self.host_institute][storage_name]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the storage dictionary"
            logging.exception(errmsg, storage_name)
        else:
            fs_backend, fs_backend_err = storage.backend_operator

        hard = quota * 1024
        if storage.backend == 'gpfs':
            hard *= storage.data_replication_factor
        soft = int(hard * self.vsc.quota_soft_fraction)

        try:
            fs_backend.set_user_quota(soft=soft, user=int(member.account.vsc_id_number), obj=path, hard=hard)
        except fs_backend_err:
            logging.exception("Unable to set USR quota for member %s on path %s", member.account.vsc_id, path)
            raise

    def set_member_data_quota(self, member):
        """Set the quota on the data FS for the member in the VO fileset.

        @type member: VscTier2AccountPageUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        if not self.vo_data_quota:
            logging.warning("Not setting VO %s member %s data quota: no VO data quota info available",
                            self.vo.vsc_id, member.account.vsc_id)
            return

        if self.vo.vsc_id in DEFAULT_VOS_ALL:
            logging.warning("Not setting VO %s member %s data quota: No VO member quota for this VO",
                            member.account.vsc_id, self.vo.vsc_id)
            return

        if member.vo_data_quota:
            # users having belonged to multiple VOs have multiple quota on VSC_DATA, so we
            # only need to deploy the quota for the VO the user currently belongs to.
            quota = [q for q in member.vo_data_quota
                     if q.fileset == self.vo.vsc_id and not q.storage['name'].endswith(STORAGE_SHARED_SUFFIX)]
            if len(quota) > 1:
                logging.exception("Cannot set data quota for member %s with multiple quota instances %s",
                                  member, quota)
                raise
            else:
                logging.info("Setting the data quota for VO %s member %s to %d KiB",
                             self.vo.vsc_id, member.account.vsc_id, quota[0].hard)
                self._set_member_quota(VSC_DATA, self._data_path(), member, quota[0].hard)
        else:
            logging.error("No VO %s data quota set for member %s", self.vo.vsc_id, member.account.vsc_id)

    def set_member_scratch_quota(self, storage_name, member):
        """Set the quota on the scratch FS for the member in the VO fileset.

        @type member: VscTier2AccountpageUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        if not self.vo_scratch_quota:
            logging.warning("Not setting VO %s member %s scratch quota: no VO quota info available",
                            self.vo.vsc_id, member.account.vsc_id)
            return

        if self.vo.vsc_id in DEFAULT_VOS_ALL:
            logging.warning("Not setting VO %s member %s scratch quota: No VO member quota for this VO",
                            member.account.vsc_id, self.vo.vsc_id)
            return

        if member.vo_scratch_quota:
            quota = [q for q in member.vo_scratch_quota
                     if q.storage['name'] in (storage_name,) and q.fileset in (self.vo_id,)]
            if quota:
                logging.info("Setting the scratch quota for VO %s member %s to %d GiB on %s",
                             self.vo.vsc_id, member.account.vsc_id, quota[0].hard / 1024 / 1024, storage_name)
                self._set_member_quota(storage_name, self._scratch_path(storage_name), member, quota[0].hard)
            else:
                logging.error("No VO %s scratch quota for member %s on %s after filter (all %s)",
                              self.vo.vsc_id, member.account.vsc_id, storage_name, member.vo_scratch_quota)
        else:
            logging.error("No VO %s scratch quota set for member %s on %s",
                          self.vo.vsc_id, member.account.vsc_id, storage_name)

    def _create_member_dir(self, member, target, storage_name):
        """Create a member-owned directory in the VO fileset."""
        try:
            storage = self.storage[self.host_institute][storage_name]
        except KeyError:
            errmsg = "Trying to access non-existent field %s in the storage dictionary"
            logging.exception(errmsg, storage_name)
        else:
            fs_backend, fs_backend_err = storage.backend_operator

        fs_backend.create_stat_directory(
            target,
            0o700,
            int(member.account.vsc_id_number),
            int(member.usergroup.vsc_id_number),
            # we should not override permissions on an existing dir where users may have changed them
            override_permissions=False
        )

    def create_member_data_dir(self, member):
        """Create a directory on data in the VO fileset that is owned
        by the member with name $VSC_DATA_VO/<vscid>."""
        target = os.path.join(self._data_path(), member.user_id)
        self._create_member_dir(member, target, VSC_DATA)

    def create_member_scratch_dir(self, storage_name, member):
        """Create a directory on scratch in the VO fileset that is owned
        by the member with name $VSC_SCRATCH_VO/<vscid>."""
        target = os.path.join(self._scratch_path(storage_name), member.user_id)
        self._create_member_dir(member, target, VSC_PRODUCTION_SCRATCH)

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the storage backend instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            for filesystem in self.storage[self.host_institute]:
                fs_backend, _ = self.storage[self.host_institute][filesystem].backend_operator
                fs_backend.dry_run = value

        super(VscTier2AccountpageVo, self).__setattr__(name, value)


def update_vo_status(vo):
    """Make sure the rest of the subsystems know that the VO status has changed.

    Currently, this is tailored to our LDAP-based setup.
    - if the LDAP state is new:
        change the state to notify
    - if the LDAP state is modify:
        change the state to active
    - otherwise, the VO already was active in the past, and we simply have an idempotent script.
    """
    if vo.dry_run:
        logging.info("VO %s has status %s. Dry-run so not changing anything", vo.vo_id, vo.vo.status)
        return

    if vo.vo.status not in (NEW, MODIFIED, MODIFY):
        logging.info("VO %s has status %s, not changing", vo.vo_id, vo.vo.status)
        return

    payload = {"status": ACTIVE}
    try:
        response = vo.rest_client.vo[vo.vo_id].patch(body=payload)
    except HTTPError as err:
        logging.error("VO %s status was not changed", vo.vo_id)
        raise VoStatusUpdateError("Vo %s status was not changed - received HTTP code %d" % err.code)
    else:
        virtual_organisation = mkVo(response)
        if virtual_organisation.status == ACTIVE:
            logging.info("VO %s status changed to %s", vo.vo_id, ACTIVE)
        else:
            logging.error("VO %s status was not changed", vo.vo_id)
            raise UserStatusUpdateError("VO %s status was not changed, still at %s" %
                                        (vo.vo_id, virtual_organisation.status))


def process_vos(options, vo_ids, storage_name, client, datestamp, host_institute=GENT):
    """Process the virtual organisations.

    - make the fileset per VO
    - set the quota for the complete fileset
    - set the quota on a per-user basis for all VO members
    """

    listm = Monoid([], lambda xs, ys: xs + ys)
    ok_vos = MonoidDict(copy.deepcopy(listm))
    error_vos = MonoidDict(copy.deepcopy(listm))

    for vo_id in sorted(vo_ids):

        vo = VscTier2AccountpageVo(vo_id, rest_client=client, host_institute=host_institute)
        vo.dry_run = options.dry_run

        try:
            if storage_name in [VSC_HOME]:
                continue

            if storage_name in [VSC_DATA] and vo_id not in DEFAULT_VOS_ALL:
                vo.create_data_fileset()
                vo.set_data_quota()
                update_vo_status(vo)

            if storage_name in [VSC_DATA_SHARED] and vo_id not in DEFAULT_VOS_ALL and vo.data_sharing:
                vo.create_data_shared_fileset()
                vo.set_data_shared_quota()

            if vo_id == INSTITUTE_VOS_BY_INSTITUTE[host_institute][host_institute]:
                logging.info("Not deploying default VO %s members", vo_id)
                continue

            if storage_name in VSC_PRODUCTION_SCRATCH[host_institute]:
                vo.create_scratch_fileset(storage_name)
                vo.set_scratch_quota(storage_name)

            if vo_id in DEFAULT_VOS_ALL and storage_name in (VSC_HOME, VSC_DATA):
                logging.info("Not deploying default VO %s members on %s", vo_id, storage_name)
                continue

            modified_member_list = client.vo[vo.vo_id].member.modified[datestamp].get()
            factory = lambda vid: VscTier2AccountpageUser(vid,
                                                          rest_client=client,
                                                          host_institute=host_institute,
                                                          use_user_cache=True)
            modified_members = [factory(a["vsc_id"]) for a in modified_member_list[1]]

            for member in modified_members:
                try:
                    member.dry_run = options.dry_run
                    if storage_name in [VSC_DATA]:
                        vo.set_member_data_quota(member)  # half of the VO quota
                        vo.create_member_data_dir(member)

                    if storage_name in VSC_PRODUCTION_SCRATCH[host_institute]:
                        vo.set_member_scratch_quota(storage_name, member)  # half of the VO quota
                        vo.create_member_scratch_dir(storage_name, member)

                    ok_vos[vo.vo_id] = [member.account.vsc_id]
                except Exception:
                    logging.exception("Failure at setting up the member %s of VO %s on %s",
                                      member.account.vsc_id, vo.vo_id, storage_name)
                    error_vos[vo.vo_id] = [member.account.vsc_id]
        except Exception:
            logging.exception("Something went wrong setting up the VO %s on the storage %s", vo.vo_id, storage_name)
            error_vos[vo.vo_id] = vo.members

    return (ok_vos, error_vos)
