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
This script synchronises the users and VO's from the HPC account page to the central
UGent storage for home and data.

For each (active) user, the following tasks are done:
    - create a directory in the home filesystem
    - chown this directory to the user
    - create the basic directories and scripts if they do not yet exist (.ssh, .bashrc, ...)
    - drop the user's public keys in the appropriate location
    - chmod the files to the correct value
    - chown the files (only changes things upon first invocation and new files)

The script should result in an idempotent execution, to ensure nothing breaks.
"""

import logging
import sys

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVscUserSizeQuota
from vsc.administration.user import process_users, process_users_quota
from vsc.administration.vo import process_vos
from vsc.config.base import GENT
from vsc.utils.missing import nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption
from vsc.utils.timestamp import convert_timestamp, write_timestamp, retrieve_timestamp_with_default

NAGIOS_HEADER = "sync_vsc_users"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_VSC_USERS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

STORAGE_USERS_LIMIT_WARNING = 1
STORAGE_USERS_LIMIT_CRITICAL = 10
STORAGE_QUOTA_LIMIT_WARNING = 1
STORAGE_QUOTA_LIMIT_CRITICAL = 5
STORAGE_VO_LIMIT_WARNING = 1
STORAGE_VO_LIMIT_CRITICAL = 2


class UserGroupStatusUpdateError(Exception):
    pass


def main():
    """
    Main script.
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
        'user': ('process users', None, 'store_true', False),
        'vo': ('process vos', None, 'store_true', False),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page where we can find the REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'start_timestamp': ('Timestamp to start the sync from', str, 'store', None),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    (last_timestamp, start_time) = retrieve_timestamp_with_default(
        SYNC_TIMESTAMP_FILENAME,
        start_timestamp=opts.options.start_timestamp)
    logging.info("Using timestamp %s", last_timestamp)
    logging.info("Using startime %s", start_time)

    try:
        client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + "/api/")

        institute = opts.options.host_institute

        (users_ok, users_fail) = ([], [])
        (quota_ok, quota_fail) = ([], [])
        if opts.options.user:
            changed_accounts = client.account.institute[institute].modified[last_timestamp].get()[1]

            logging.info("Found %d %s accounts that have changed in the accountpage since %s",
                        len(changed_accounts), institute, last_timestamp)

            accounts = nub([u['vsc_id'] for u in changed_accounts])

            for storage_name in opts.options.storage:
                (users_ok, users_fail) = process_users(
                    opts.options,
                    accounts,
                    storage_name,
                    client,
                    institute)
                stats["%s_users_sync" % (storage_name,)] = len(users_ok)
                stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
                stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
                stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL

            for storage_name in opts.options.storage:
                storage_changed_quota = [mkVscUserSizeQuota(q) for q in
                                         client.quota.user.storage[storage_name].modified[last_timestamp].get()[1]]
                storage_changed_quota = [q for q in storage_changed_quota if q.fileset.startswith('vsc')]
                logging.info("Found %d accounts that have changed quota on storage %s in the accountpage since %s",
                            len(storage_changed_quota), storage_name, last_timestamp)
                (quota_ok, quota_fail) = process_users_quota(
                    opts.options,
                    storage_changed_quota,
                    storage_name,
                    client,
                    institute)
                stats["%s_quota_sync" % (storage_name,)] = len(quota_ok)
                stats["%s_quota_sync_fail" % (storage_name,)] = len(quota_fail)
                stats["%s_quota_sync_fail_warning" % (storage_name,)] = STORAGE_QUOTA_LIMIT_WARNING
                stats["%s_quota_sync_fail_critical" % (storage_name,)] = STORAGE_QUOTA_LIMIT_CRITICAL

        (vos_ok, vos_fail) = ([], [])
        if opts.options.vo:
            changed_vos = client.vo.institute[institute].modified[last_timestamp].get()[1]
            changed_vo_quota = client.quota.vo.modified[last_timestamp].get()[1]

            vos = sorted(set([v['vsc_id'] for v in changed_vos] +
                             [v['virtual_organisation'] for v in changed_vo_quota]))

            logging.info("Found %d %s VOs that have changed in the accountpage since %s",
                        len(changed_vos), institute, last_timestamp)
            logging.info("Found %d %s VOs that have changed quota in the accountpage since %s",
                        len(changed_vo_quota), institute, last_timestamp)
            logging.debug("Found the following {institute} VOs: {vos}".format(institute=institute, vos=vos))

            for storage_name in opts.options.storage:
                (vos_ok, vos_fail) = process_vos(
                    opts.options,
                    vos,
                    storage_name,
                    client,
                    last_timestamp,
                    institute)
                stats["%s_vos_sync" % (storage_name,)] = len(vos_ok)
                stats["%s_vos_sync_fail" % (storage_name,)] = len(vos_fail)
                stats["%s_vos_sync_fail_warning" % (storage_name,)] = STORAGE_VO_LIMIT_WARNING
                stats["%s_vos_sync_fail_critical" % (storage_name,)] = STORAGE_VO_LIMIT_CRITICAL

        if not (users_fail or quota_fail or vos_fail) and not opts.options.dry_run:
            (_, ldap_timestamp) = convert_timestamp(start_time)
            write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
    except Exception as err:
        logging.exception("critical exception caught: %s", err)
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("%s users and VOs synchronised" % institute, stats)


if __name__ == '__main__':
    main()
