# -*- coding: latin-1 -*-
#
# Copyright 2012-2021 Ghent University
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
This file contains the tools for automated administration w.r.t. the VSC
Original Perl code by Stijn De Weirdt

@author: Andy Georges (Ghent University)
"""
import logging

TIER1_GRACE_GROUP_SUFFIX = "t1_mukgraceusers"
TIER1_HELPDESK_ADDRESS = "tier1@ugent.be"
UGENT_SMTP_ADDRESS = "smtp.ugent.be"

REINSTATEMENT_MESSAGE = """
Dear %(gecos)s,

You have been reinstated to regular status on the VSC Tier-1 cluster at Ghent. This means you can
again submit jobs to the scheduler.

Should you have any questions, please contact us at %(tier1_helpdesk)s or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
"""


def create_stat_directory(path, permissions, uid, gid, posix, override_permissions=True):
    """
    Function moved to vsc-filesystems
    """
    logging.warning("The create_stat_directory function has moved to vsc.filesystems")
    return posix.create_stat_directory(path, permissions, uid, gid, override_permissions=override_permissions)
