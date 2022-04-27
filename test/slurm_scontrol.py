#
# Copyright 2022-2022 Ghent University
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
Tests for vsc.administration.slurm.*
"""

from vsc.install.testing import TestCase

from vsc.administration.slurm.scontrol import (
    parse_scontrol_dump,
    ScontrolTypes, SlurmReservation, SlurmLicense,
    )


class SlurmScontrolTest(TestCase):
    def test_parse_scontrol_dump(self):
        """Test that the scontrol output is correctly processed."""

        # Or eg 'No licenses configured in Slurm.' (slurm consistency yay)
        scontrol_output = ["No reservations in the system"]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.reservation)

        self.assertEqual(info, set())

        # test reservation output
        scontrol_output = [
            "ReservationName=hpc123 StartTime=2022-03-28T16:05:00 EndTime=2028-05-28T07:59:59 Duration=2252-15:54:59 Nodes=node123,node456 NodeCnt=2 CoreCnt=512 Features=(null) PartitionName=(null) Flags=MAINT,IGNORE_JOBS,SPEC_NODES TRES=cpu=512 Users=vscabc,vscdef Groups=(null) Accounts=(null) Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)",
            "ReservationName=hellohello StartTime=2022-04-19T08:00:00 EndTime=2022-05-19T08:00:00 Duration=30-00:00:00 Nodes=nodeone,nodetwo,nodethree,nodefour NodeCnt=4 CoreCnt=8 Features=(null) PartitionName=party Flags= TRES=cpu=8 Users=(null) Groups=groupies Accounts=myaccount Licenses=(null) State=ACTIVE BurstBuffer=(null) Watts=n/a MaxStartDelay=(null)",
        ]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.reservation)

        self.assertEqual(info, set([
            SlurmReservation(ReservationName='hpc123', StartTime='2022-03-28T16:05:00', EndTime='2028-05-28T07:59:59', Duration='2252-15:54:59', Nodes='node123,node456', NodeCnt='2', CoreCnt='512', Features='(null)', PartitionName='(null)', Flags='MAINT,IGNORE_JOBS,SPEC_NODES', TRES='cpu=512', Users='vscabc,vscdef', Groups='(null)', Accounts='(null)', Licenses='(null)', State='ACTIVE', BurstBuffer='(null)', Watts='n/a', MaxStartDelay='(null)'),
            SlurmReservation(ReservationName='hellohello', StartTime='2022-04-19T08:00:00', EndTime='2022-05-19T08:00:00', Duration='30-00:00:00', Nodes='nodeone,nodetwo,nodethree,nodefour', NodeCnt='4', CoreCnt='8', Features='(null)', PartitionName='party', Flags='', TRES='cpu=8', Users='(null)', Groups='groupies', Accounts='myaccount', Licenses='(null)', State='ACTIVE', BurstBuffer='(null)', Watts='n/a', MaxStartDelay='(null)'),
        ]))

        # test license output
        scontrol_output = [
            'LicenseName=comsol3@bogus Total=2 Used=0 Free=2 Reserved=0 Remote=yes',
            'LicenseName=comsol3@bogus2 Total=4 Used=1 Free=3 Reserved=0 Remote=yes',
            ]

        info = parse_scontrol_dump(scontrol_output, ScontrolTypes.license)

        self.assertEqual(info, set([
            SlurmLicense(LicenseName='comsol3@bogus', Total=2, Used=0, Free=2, Reserved=0, Remote='yes'),
            SlurmLicense(LicenseName='comsol3@bogus2', Total=4, Used=1, Free=3, Reserved=0, Remote='yes'),
        ]))
