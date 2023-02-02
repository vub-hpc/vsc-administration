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
This script creates the list of canonical email adresses for VSC users.
"""
from __future__ import print_function

import logging

from vsc.accountpage.sync import Sync



class VscPostfixSync(Sync):
    CLI_OPTIONS = {
        'postfix_canonical_map': ('Location of the postfix canonical map', None, 'store', '/etc/postfix/vsc_canonical'),
    }

    def do(self, dry_run):
        """Actual work.

        - retrieve all the email addresss from changed users
        - read the current file
        - remove inactive users' email addresses
        - add new/changed email addresses
        - write file
        """
        active_accounts, inactive_accounts = self.get_accounts()

        if not active_accounts and not inactive_accounts:
            logging.info("No changed accounts. Not rewriting the canonical map file.")
            return

        active_emails = dict([("%s@vscentrum.be" % a.vsc_id, a.email) for a in active_accounts])
        inactive_emails = set(["%s@vscentrum.be" % a.vsc_id for a in inactive_accounts])

        logging.debug("active emails: %s", active_emails)
        logging.debug("inactive emails: %s", inactive_emails)

        address_map = dict()
        try:
            with open(self.options.postfix_canonical_map, 'r') as cm:
                address_map = dict(
                    [tuple(l) for l in [l.split() for l in cm.readlines()] if l and l[0] not in inactive_emails]
                )
        except IOError as err:
            logging.warning("No canonical map at %s: %s", self.options.postfix_canonical_map, err)

        address_map.update(active_emails)

        txt = "\n".join(["%s %s" % kv for kv in address_map.items()] + [''])

        if dry_run:
            logging.info("Dry run. File contents:\n%s", txt)
            print(txt)
        else:
            with open(self.options.postfix_canonical_map, 'w') as cm:
                cm.write(txt)
                logging.info("File %s written. %d entries.", self.options.postfix_canonical_map, len(address_map))


if __name__ == '__main__':
    VscPostfixSync().main()
