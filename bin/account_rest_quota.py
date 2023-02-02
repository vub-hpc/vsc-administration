#!/usr/bin/env python
#
# Copyright 2014-2023 Ghent University
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
This script will adjust the quota for the given entity, be it a user or a VO
on the given storage.
"""
from __future__ import print_function

import sys

from vsc.accountpage.client import AccountpageClient
from vsc.utils.generaloption import SimpleOption


def main():
    """
    Main script. Sets the hard limit for either user or VO.
    """
    options = {
        'storage': ("The storage system's name", None, 'store', None),
        'fileset': ("The fileset where you want to adjust the quota", None, 'store', None),
        'user': ('process users', None, 'store', None),
        'vo': ('process vos', None, 'store', None),
        'size': ('the target quota (in KiB)', int, 'store', None),
        'original': ('show the original quota values', None, 'store_true', False),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
    }

    opts = SimpleOption(options)
    client = AccountpageClient(token=opts.options.access_token)

    fileset = opts.options.fileset
    storage = opts.options.storage

    if not opts.options.size:
        print("size argument missing")
        sys.exit(1)

    # TODO: could use some love in allowing a unit to be appended and converting to KiB prior to uploading
    size = opts.options.size

    if opts.options.user:
        # quota/user/%(vsc_id)s/storage/%(storage)s/fileset/%(fileset)s/size/$
        vsc_id = opts.options.user
        original = client.account[vsc_id].quota
        upload = client.quota.user[vsc_id].storage[storage].fileset[fileset].size

    if opts.options.vo:

        vsc_id = opts.options.vo
        original = client.vo[vsc_id].quota
        upload = client.quota.vo[vsc_id].storage[storage].fileset[fileset].size

    current = original.get()
    if current[0] in (200,):
        if opts.options.original:
            print("Original values: %s" % (current[1],))
    else:
        print("Error, could not get original quota values for the given parameters")
        print("Issue: %s" % (current[1],))
        sys.exit(-1)

    result = upload.put(body={"hard": size})
    if result[0] in (200,):
        print("Request OK.")
        print("New values: %s" % (result[1],))
    else:
        print("Request failed")
        print("Issue: %s" % (result[1],))

if __name__ == '__main__':
    main()
